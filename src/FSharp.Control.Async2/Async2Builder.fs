namespace Microsoft.FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Microsoft.FSharp.Core.CompilerServices
open System.Runtime.ExceptionServices

module AwaitableHelpers =

    /// A structure that looks like an Awaiter
    type Awaiter<'Awaiter, 'TResult
        when 'Awaiter :> ICriticalNotifyCompletion
        and 'Awaiter: (member get_IsCompleted: unit -> bool)
        and 'Awaiter: (member GetResult: unit -> 'TResult)> = 'Awaiter

    type Awaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> = 'Awaitable

    type ColdAwaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> =
        unit -> 'Awaitable

    type CancellableAwaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> =
        CancellationToken -> 'Awaitable

    module Awaiter =
        let inline isCompleted (awaiter: Awaiter<_, _>) = awaiter.get_IsCompleted ()
        let inline getResult (awaiter: Awaiter<_, _>) = awaiter.GetResult()

        let inline onCompleted (awaiter: Awaiter<_, _>) continuation =
            awaiter.OnCompleted continuation

        let inline unsafeOnCompleted (awaiter: Awaiter<_, _>) continuation =
            awaiter.UnsafeOnCompleted continuation

        /// Waits for `awaiter` to complete and returns its result. Only runtime-async code can call it.
        let inline await (awaiter: Awaiter<_, _>) =
            if not (isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter

            getResult awaiter

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

open AwaitableHelpers

/// The state of one start from user code, shared by every start nested in it. Its frames can run on any thread, so
/// they carry it in their context.
[<NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Start =
    internal
        {
            CancellationToken: CancellationToken

            /// The failure that propagates through the frames of this start, with the trace of its throw. The frames
            /// of one start run one after another, and the completion of a frame's task orders its write before the
            /// read of the parent that awaits it, also when the parent resumes on another thread.
            mutable Failure: ExceptionDispatchInfo option

            /// The managed thread id of the thread whose drain loop runs for this start, or 0. Other threads read it,
            /// so it is one word.
            mutable Owner: int

            /// The continuations that nested starts bounced to the drain loop, created at the first bounce. Only the
            /// owner thread touches it.
            mutable Queue: Queue<Action> option
        }

/// A computation that a frame of a tail-call chain hands over, with the execution context it has to start in.
[<Struct; NoEquality; NoComparison>]
type internal Handover =
    {
        Invocation: obj
        Context: ExecutionContext voption
    }

/// The chain of computations that frames hand over with a final `return!`.
[<NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type TailChain = internal { mutable Next: Handover voption }

/// The context of a frame: the start it belongs to and, when the frame runs in a tail-call chain, the chain.
[<Struct; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Context =
    {
        Start: Async2Start
        Chain: TailChain option
    }

    /// The token of the start. Every start nested in it runs with the same token.
    member ctx.CancellationToken = ctx.Start.CancellationToken

type Async2Invocation<'R> = Async2Context -> Task<'R>

module internal Async2Context =
    let root (start: Async2Start) = { Start = start; Chain = None }

/// The drain loop of a start runs on the thread that made the start, further down the stack than the frames that run
/// inline. A nested start that bounces because the stack runs low queues its continuation there, and the loop runs it
/// when the synchronous part of the start returns.
module internal Drain =
    /// True when the drain loop of `start` runs further down the stack of this thread.
    let ownedBy (start: Async2Start) =
        Volatile.Read(&start.Owner) = Environment.CurrentManagedThreadId

    /// Queues a continuation on the drain loop of `start`. Only the owner thread calls it.
    let enqueue (start: Async2Start) (continuation: Action) =
        match start.Queue with
        | Some queue -> queue.Enqueue continuation
        | None ->
            let queue = Queue<Action>()
            queue.Enqueue continuation
            start.Queue <- Some queue

    /// Runs `action` with the drain loop of the new `start` on this thread, then runs every continuation queued
    /// meanwhile. Queued continuations never throw.
    let inline run (start: Async2Start) ([<InlineIfLambda>] action: unit -> 'T) =
        start.Owner <- Environment.CurrentManagedThreadId

        try
            action ()
        finally
            try
                match start.Queue with
                | Some queue ->
                    while queue.Count > 0 do
                        queue.Dequeue().Invoke()
                | None -> ()
            finally
                Volatile.Write(&start.Owner, 0)

    /// A nested start runs inline unless the stack runs low, so a bounce happens only where running inline would
    /// overflow. A bounced child is incomplete when its parent awaits it, so under a SynchronizationContext or a
    /// non-default TaskScheduler the parent's resumption would be posted there, and a blocked context never runs it.
    /// There nested starts always run inline.
    let canRunInline () =
        RuntimeHelpers.TryEnsureSufficientExecutionStack()
        || not (isNull SynchronizationContext.Current)
        || not (obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default))

/// Keeps the stack trace of a failure short while it propagates through many frames. Each frame that awaits a failed
/// task rethrows the failure. If it captured the trace each time, the trace would grow by one section at each frame,
/// and the cost of a deep failure would grow with the square of the depth.
module ExceptionCache =

    /// Rethrows `error` in a frame of `start`. When `error` is the failure that propagates through the start, it is
    /// rethrown with the trace of its throw. A user handler that rethrows the same object propagates the failure. Any
    /// other failure is a new throw: from user code, from a foreign awaiter, or a throw after a handler handled the
    /// failure. Its trace is captured here, and it becomes the failure that propagates.
    let rethrow (start: Async2Start) (error: exn) : 'T =
        let failure =
            match start.Failure with
            | Some failure when obj.ReferenceEquals(failure.SourceException, error) -> failure
            | _ ->
                let failure = ExceptionDispatchInfo.Capture error
                start.Failure <- Some failure
                failure

        failure.Throw()
        Unchecked.defaultof<_>

    /// Rethrows `error` outside builder code, where no start tracks the failure.
    let rethrowOutside (error: exn) : 'T =
        ExceptionDispatchInfo.Throw error
        Unchecked.defaultof<_>

    /// For a failure that a user handler handled. A later throw of the same object is then a new throw.
    let handled (start: Async2Start) = start.Failure <- None

    /// Awaits `task` and passes its failure to `rethrow`. The wait itself never throws: awaiting a faulted task
    /// directly would throw once in the awaiter, again where the suspended frame resumes, and a third time here, so a
    /// failure crossing one frame would cost three throws instead of one. `UnsafeAwaitAwaiter` only waits for
    /// completion and never calls `GetResult`; unlike awaiting a `ConfigureAwait(SuppressThrowing)` awaitable, it
    /// allocates nothing beyond what `Await` does.
    let inline awaitWith ([<InlineIfLambda>] rethrow: exn -> 'T) (task: Task<'T>) =
        if not task.IsCompleted then
            AsyncHelpers.UnsafeAwaitAwaiter(task.GetAwaiter())

        if task.IsCompletedSuccessfully then
            task.Result
        else
            match task.Exception with
            | null ->
                // Cancelled: only the awaiter can produce the OperationCanceledException the task holds.
                try
                    task.GetAwaiter().GetResult()
                with error ->
                    rethrow error
            | failure -> rethrow failure.InnerExceptions[0]

    /// Awaits `task` in a frame of `start`.
    let inline await (start: Async2Start) (task: Task<'T>) =
        awaitWith (fun error -> rethrow start error) task

    /// Awaits `task` outside builder code.
    let inline awaitOutside (task: Task<'T>) = awaitWith rethrowOutside task

module internal ExecutionContexts =
    /// Makes `context` the execution context of this thread. When it is already current, nothing is restored.
    let switchTo (context: ExecutionContext) =
        if not (obj.ReferenceEquals(context, ExecutionContext.Capture())) then
            ExecutionContext.Restore context

/// Runs a tail-call chain: the computation of the final `return!` that started the chain, then each computation that
/// a frame of the chain hands over with its own final `return!`. A handing frame finishes before its callee starts,
/// so the chain runs on a stack and in memory that do not grow with its length.
module TailChain =

    /// Builder plumbing: hands over the computation that runs after the calling frame finishes. Finishing reverts the
    /// execution context of the frame, so the callee keeps the one it was handed over in.
    let handOver (chain: TailChain) (invocation: Async2Invocation<'R>) =
        chain.Next <-
            ValueSome
                {
                    Invocation = invocation
                    Context = ValueOption.ofObj (ExecutionContext.Capture())
                }

    let private invoke (ctx: Async2Context) (invocation: Async2Invocation<'R>) =
        try
            invocation ctx
        with error ->
            Task.FromException<'R> error

    /// Builder plumbing: runs the chain from `first` and returns the outcome of its last frame. It waits for each
    /// frame that suspends. When it returns or suspends, runtime async restores the execution context of the caller.
    /// The frames of the chain hand over computations of the same result type `'R` as `first`.
    let run (start: Async2Start) (first: Async2Invocation<'R>) : Task<'R> =
        __runtimeAsyncReturn (
            let chain = { Next = ValueNone }
            let ctx = { Start = start; Chain = Some chain }
            let mutable task = invoke ctx first
            let mutable running = true

            while running do
                if not task.IsCompleted then
                    AsyncHelpers.UnsafeAwaitAwaiter(task.GetAwaiter())

                match chain.Next with
                | ValueSome handover when task.IsCompletedSuccessfully ->
                    chain.Next <- ValueNone

                    match handover.Context with
                    | ValueSome context -> ExecutionContexts.switchTo context
                    | ValueNone -> ()

                    task <- invoke ctx (handover.Invocation :?> Async2Invocation<'R>)
                | _ ->
                    // The frame returned a value or failed: the chain is complete.
                    running <- false

            ExceptionCache.await start task
        )

module internal Async2Starts =

    /// A synchronous failure of a custom factory surfaces as a faulted task, like a failure inside builder code.
    let inline private invoke (invocation: Async2Invocation<'T>) (start: Async2Start) =
        try
            invocation (Async2Context.root start)
        with error ->
            Task.FromException<'T> error

    /// Suspends the frame that awaits it and queues the continuation of that frame on the drain loop of `start`, so
    /// the loop resumes it on a shallow stack.
    [<Struct; NoEquality; NoComparison>]
    type private BounceAwaiter(start: Async2Start) =
        member _.IsCompleted = false
        member _.GetResult() = ()

        interface ICriticalNotifyCompletion with
            member _.OnCompleted continuation = Drain.enqueue start continuation
            member _.UnsafeOnCompleted continuation = Drain.enqueue start continuation

    /// Starts a nested computation that is too deep to run inline: this frame suspends at once and resumes from the
    /// drain loop, where it runs the computation. The parent awaits the task of this frame.
    let private bounced (start: Async2Start) (invocation: Async2Invocation<'T>) : Task<'T> =
        __runtimeAsyncReturn (
            AsyncHelpers.UnsafeAwaitAwaiter(BounceAwaiter start)
            invoke invocation start |> ExceptionCache.await start
        )

    /// Starts on a thread pool thread, which has an empty stack. `Task.Run` flows the execution context into the
    /// start, and the parent resumes when the start completes.
    let private startOnFreshStack (start: Async2Start) (invocation: Async2Invocation<'T>) =
        Task.Run<'T>(fun () -> invoke invocation start)

    /// Starts with a new start state and a drain loop for it on this thread, so the caller observes all synchronous
    /// progress.
    let startIsolated (invocation: Async2Invocation<'T>) cancellationToken =
        let start =
            {
                CancellationToken = cancellationToken
                Failure = None
                Owner = 0
                Queue = None
            }

        Drain.run start (fun () -> invoke invocation start)

    /// A start nested in another one, which awaits it. It runs inline when the stack has room. On a low stack it
    /// bounces to the drain loop of its start when that loop runs further down this stack. Otherwise nothing drains
    /// further down, so it continues on a fresh stack. That happens only away from the thread that made the start,
    /// for example after a frame resumed on the thread pool, or after the synchronous part of the start returned.
    let start (start: Async2Start) (invocation: Async2Invocation<'T>) =
        if Drain.canRunInline () then
            invoke invocation start
        elif Drain.ownedBy start then
            bounced start invocation
        else
            startOnFreshStack start invocation

/// Bound with `let!` in `async2` or `asyncSeq2` to read the token the computation was started with,
/// without starting another computation the way `Async2.CancellationToken` does.
[<Struct; NoEquality; NoComparison>]
type CurrentCancellationToken = CurrentCancellationToken

module Async2BuilderSources =

    type Source<'T> = delegate of CancellationToken -> 'T

    /// The awaiter is taken at once, so a hot awaitable keeps running while the sources merged with it start.
    let inline ofAwaitable awaitable =
        let awaiter = Awaitable.getAwaiter awaitable
        Source(fun _ -> Awaiter.await awaiter)

    let inline ofCancellableAwaitable cancellableAwaitable =
        Source(fun ct -> cancellableAwaitable ct |> Awaitable.getAwaiter |> Awaiter.await)

    /// Starts `right` as a task that runs until it first suspends, runs `left`, and then waits for `right`.
    let inline merge ([<InlineIfLambda>] left: Source<'A>) ([<InlineIfLambda>] right: Source<'B>) =
        Source(fun ct ->
            let rightTask = __runtimeAsyncReturn (right.Invoke ct)
            let leftResult = left.Invoke ct
            struct (leftResult, AsyncHelpers.Await rightTask))

[<Struct; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
type Async2<'T> (invocation: Async2Invocation<'T>) =

    /// Builder plumbing: starts a tail callee directly in the caller's frame, or hands it to a tail-call chain.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member _.Invocation = invocation

    /// Starts this computation with a trampoline of its own, for callers outside builder code that pass no parent.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    // Keep trampoline internals out of runtime-async inline expansions.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member _.StartTrampolined ct =
        Async2Starts.startIsolated invocation ct

    /// Builder plumbing: starts this computation nested in `start`.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member _.StartNested(start) =
        Async2Starts.start start invocation

/// Builder code: `'T` is the type of this piece.
type Async2Code<'T> = Async2Context -> 'T

open Async2BuilderSources

module Async2Builder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current
        && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

    let inline check (cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()

    let inline ofAsync2 (computation: Async2<'T>) =
        Source(fun ct -> computation.StartTrampolined ct |> ExceptionCache.awaitOutside)

    /// Starts `computation` nested in the start of the frame with context `ctx`, and waits for its result.
    ///
    /// The start goes through `StartNested`, which returns a task, so the call and the await are not fused into an
    /// async call of the runtime. Fusing them was measured: a fused chain of frames that suspends or bounces
    /// allocates more than a task for each frame.
    let inline awaitNested (ctx: Async2Context) (computation: Async2<'T>) =
        computation.StartNested ctx.Start |> ExceptionCache.await ctx.Start

open Async2Builder

type Async2Builder() =

    // The code type of the builder is `Async2Context -> 'T`: the context of the frame is passed along as state to
    // every delayed continuation.
    member inline _.Delay([<InlineIfLambda>] generator: unit -> Async2Code<'T>) : Async2Code<'T> =
        fun ctx ->
            check ctx.CancellationToken
            generator () ctx

    member inline _.Zero() : Async2Code<unit> =
        fun ctx ->
            check ctx.CancellationToken
            ()

    member inline _.Return(value: 'T) : Async2Code<'T> =
        fun ctx ->
            check ctx.CancellationToken
            value

    member inline _.Combine([<InlineIfLambda>] first: Async2Code<unit>, [<InlineIfLambda>] second: Async2Code<'T>)
        : Async2Code<'T> =
        fun ctx ->
            check ctx.CancellationToken
            first ctx
            second ctx

    member inline _.TryWith ([<InlineIfLambda>] body, [<InlineIfLambda>] handler) : Async2Code<'T> =
        fun ctx ->
            check ctx.CancellationToken
            try
                body ctx
            with error ->
                // The mark stays while the handler runs: a handler that rethrows the same object, such as a
                // `when` that does not match, propagates the failure, and the parent uses the cached trace.
                // If the parent captured the trace again at each such frame, the trace would grow by one
                // section per frame. A handler that returns has handled the failure.
                check ctx.CancellationToken
                let result = handler error ctx
                ExceptionCache.handled ctx.Start
                result

    member inline _.TryFinally ([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) : Async2Code<'T> =
        fun ctx ->
            try
                check ctx.CancellationToken
                body ctx
            finally
                compensation ()

    member inline _.Using(resource: 'T :> IDisposable | null, [<InlineIfLambda>] body) : Async2Code<'U> =
        fun ctx ->
            try
                check ctx.CancellationToken
                body resource ctx
            finally
                if not (isNull (box resource)) then resource.Dispose()

    member inline _.While(guard: unit -> bool, [<InlineIfLambda>] body) : Async2Code<unit> =
        fun ctx ->
            while guard () do
                check ctx.CancellationToken
                body ctx
            check ctx.CancellationToken

    member inline _.For(sequence: seq<'T>, [<InlineIfLambda>] body) : Async2Code<unit> =
        fun ctx ->
            for item in sequence do
                check ctx.CancellationToken
                body item ctx
            check ctx.CancellationToken

    member inline _.Bind([<InlineIfLambda>] source: Source<'T>, [<InlineIfLambda>] continuation) : Async2Code<'U> =
        fun ctx ->
            check ctx.CancellationToken
            continuation (source.Invoke ctx.CancellationToken) ctx

    member inline _.ReturnFrom([<InlineIfLambda>] source: Source<'T>) : Async2Code<'T> =
        fun ctx -> source.Invoke ctx.CancellationToken

    member inline _.ReturnFromFinal([<InlineIfLambda>] source: Source<'T>) : Async2Code<'T> =
        fun ctx -> source.Invoke ctx.CancellationToken

    member inline _.MergeSources([<InlineIfLambda>] left: Source<'A>, [<InlineIfLambda>] right: Source<'B>) =
        merge left right

    member inline _.Run([<InlineIfLambda>] code: Async2Code<'T>) : Async2<'T> =
        Async2(fun ctx -> __runtimeAsyncReturn(code ctx))

    member inline _.Source(computation: Async2<'T>) = computation

    member inline _.ReturnFrom(computation: Async2<'T>) : Async2Code<'T> =
        fun ctx -> awaitNested ctx computation

    member inline _.ReturnFromFinal(computation: Async2<'T>) : Async2Code<'T> =
        fun ctx ->
            match ctx.Chain with
            | Some chain ->
                // This frame runs in a tail-call chain: hand the callee over and let the frame finish.
                TailChain.handOver chain computation.Invocation
                Unchecked.defaultof<_>
            | None ->
                // Start a chain that runs the callee, then each computation it hands over in turn.
                TailChain.run ctx.Start computation.Invocation |> ExceptionCache.await ctx.Start

    member inline _.Bind(computation: Async2<'T>, [<InlineIfLambda>] continuation) : Async2Code<'U> =
        fun ctx ->
            check ctx.CancellationToken
            continuation (awaitNested ctx computation) ctx

    member inline _.MergeSources(left: Async2<'A>, [<InlineIfLambda>] right: Source<'B>) =
        merge (ofAsync2 left) right

    member inline _.MergeSources([<InlineIfLambda>] left: Source<'A>, right: Async2<'B>) =
        merge left (ofAsync2 right)

    member inline _.MergeSources(left: Async2<'A>, right: Async2<'B>) =
        merge (ofAsync2 left) (ofAsync2 right)

[<AutoOpen>]
module Async2BuilderAsyncDisposableExtensions =
    type Async2Builder with
        member inline _.Using(resource: 'T :> IAsyncDisposable | null, [<InlineIfLambda>] body) : Async2Code<'U> =
            fun ctx ->
                try
                    check ctx.CancellationToken
                    body resource ctx
                finally
                    if not (isNull (box resource)) then
                        resource.DisposeAsync() |> AsyncHelpers.Await

        member inline this.For(sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body) : Async2Code<unit> =
            fun ctx ->
                this.Using
                    (sequence.GetAsyncEnumerator ctx.CancellationToken,
                     fun enumerator ctx ->
                         while enumerator.MoveNextAsync()
                               |> AsyncHelpers.Await do
                             check ctx.CancellationToken
                             body enumerator.Current ctx
                         check ctx.CancellationToken)
                    ctx

[<AutoOpen>]
module Async2BuilderAwaitableExtensions =
    type Async2Builder with
        member inline _.Source(awaitable) = ofAwaitable awaitable

        member inline _.Source([<InlineIfLambda>] coldAwaitable) =
            ofAwaitable (coldAwaitable ())

        member inline _.Source([<InlineIfLambda>] cancellableAwaitable) =
            ofCancellableAwaitable cancellableAwaitable

[<AutoOpen>]
module Async2BuilderSourceExtensions =
    type Async2Builder with

        // Accepted sources for For
        member inline _.Source(sequence: 'T seq) = sequence
        member inline _.Source(sequence: IAsyncEnumerable<'T>) = sequence

        // Canonical runtime async Bind sources
        member inline _.Source(task: Task<'T>) = Source(fun _ -> task |> AsyncHelpers.Await)
        member inline _.Source(task: Task) = Source(fun _ -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask<'T>) = Source(fun _ -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask) = Source(fun _ -> task |> AsyncHelpers.Await)

        member inline _.Source(_: CurrentCancellationToken) = Source(fun ct -> ct)

        // Cold start sources
        member inline _.Source(computation: Async<'T>) =
            Source(fun ct -> Async.StartImmediateAsTask(computation, ct) |> AsyncHelpers.Await)

[<AutoOpen>]
module Async2BuilderImpl =
    let async2 = Async2Builder()
