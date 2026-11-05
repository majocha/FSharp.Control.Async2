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

            /// The failure that `ExceptionCache.throwCached` rethrew last in a frame of this start, or null. The
            /// frames of one start run one after another, and the completion of a frame's task orders its write
            /// before the read of the parent that awaits it, also when the parent resumes on another thread.
            mutable Propagating: exn | null
        }

/// The context of a frame: the start it belongs to and, when the frame runs in a tail-call chain, the box of the chain,
/// which a final `return!` hands its callee to.
[<Struct; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Context =
    {
        Start: Async2Start
        Chain: obj | null
    }

    /// The token of the start. Every start nested in it runs with the same token.
    member ctx.CancellationToken = ctx.Start.CancellationToken

module internal Async2Context =
    let root (start: Async2Start) = { Start = start; Chain = null }

/// The drain loop that runs on this thread, for one start. A nested start that bounces because the stack runs low
/// queues its continuation here, and the loop runs it when the synchronous part of the start returns, further down
/// the stack. Only that thread touches these values.
[<AbstractClass; Sealed>]
type internal DrainState =
    /// The start whose drain loop runs innermost on this thread, or null.
    [<ThreadStatic; DefaultValue>]
    static val mutable private owner: Async2Start | null

    [<ThreadStatic; DefaultValue>]
    static val mutable private queue: Queue<Action> | null

    static member Owner
        with get () = DrainState.owner
        and set value = DrainState.owner <- value

    static member Queue
        with get () = DrainState.queue
        and set value = DrainState.queue <- value

module internal Drain =
    /// True when the innermost drain loop on this thread belongs to `start`.
    let ownedBy (start: Async2Start) = obj.ReferenceEquals(DrainState.Owner, start)

    /// Queues a continuation on the innermost drain loop of this thread.
    let enqueue (continuation: Action) =
        match DrainState.Queue with
        | null ->
            let created = Queue<Action>()
            created.Enqueue continuation
            DrainState.Queue <- created
        | queue -> queue.Enqueue continuation

    /// Runs `action` with a drain loop for `start` on this thread, then runs every continuation queued meanwhile.
    /// The loop of an enclosing start on this thread continues afterwards. Queued continuations never throw.
    let inline run (start: Async2Start) ([<InlineIfLambda>] action: unit -> 'T) =
        let enclosingOwner = DrainState.Owner
        let enclosingQueue = DrainState.Queue
        DrainState.Owner <- start
        DrainState.Queue <- null

        try
            action ()
        finally
            try
                match DrainState.Queue with
                | null -> ()
                | queue ->
                    while queue.Count > 0 do
                        queue.Dequeue().Invoke()
            finally
                DrainState.Owner <- enclosingOwner
                DrainState.Queue <- enclosingQueue

    /// A nested start runs inline unless the stack runs low, so a bounce happens only where running inline would
    /// overflow. A bounced child is incomplete when its parent awaits it, so under a SynchronizationContext or a
    /// non-default TaskScheduler the parent's resumption would be posted there, and a blocked context never runs it.
    /// There nested starts always run inline.
    let canRunInline () =
        RuntimeHelpers.TryEnsureSufficientExecutionStack()
        || not (isNull SynchronizationContext.Current)
        || not (obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default))

module ExceptionCache =
    let private store = ConditionalWeakTable<exn, ExceptionDispatchInfo>()

    let private captureIfMissing =
        ConditionalWeakTable<exn, ExceptionDispatchInfo>.CreateValueCallback(fun error -> ExceptionDispatchInfo.Capture error)

    /// Captures the current trace of `error` and replaces the cached one.
    let private captureNew error =
        let edi = ExceptionDispatchInfo.Capture error
        store.AddOrUpdate(error, edi)
        edi

    /// Rethrows `error` with the dispatch info captured where it was thrown, so a failure propagating through
    /// many frames does not grow its stack trace at every frame. `owner` is the start of the awaiting frame, or null
    /// for an await outside builder code.
    ///
    /// The start holds the failure that this function rethrew last in one of its frames, so the awaiting parent tells
    /// propagation from a new throw of the same exception object on any thread. A user handler that rethrows the
    /// same object propagates the failure. Any other failure is a new throw: from user code, from a foreign awaiter,
    /// or a throw after a handler handled the failure. It replaces the entry, so a new throw of the same exception
    /// object does not get an old trace.
    let throwCached (owner: Async2Start | null) (error: exn) : 'T =
        let edi =
            match owner with
            | null -> captureNew error
            | owner when obj.ReferenceEquals(owner.Propagating, error) -> store.GetValue(error, captureIfMissing)
            | owner ->
                owner.Propagating <- error
                captureNew error

        edi.Throw()
        Unchecked.defaultof<_>

    /// For a failure that a user handler handled. A later throw of the same object is then a new throw.
    let handled (start: Async2Start) = start.Propagating <- null

    /// Awaits `task` and rethrows its failure through the cache. The wait itself never throws: awaiting a
    /// faulted task directly would throw once in the awaiter, again where the suspended frame resumes, and
    /// a third time here, so a failure crossing one frame would cost three throws instead of one.
    /// `UnsafeAwaitAwaiter` only waits for completion and never calls `GetResult`; unlike awaiting a
    /// `ConfigureAwait(SuppressThrowing)` awaitable, it allocates nothing beyond what `Await` does.
    let inline await (owner: Async2Start | null) (task: Task<'T>) =
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
                    throwCached owner error
            | failure -> throwCached owner failure.InnerExceptions[0]

module internal ExecutionContexts =
    /// Makes `context` the execution context of this thread. When it is already current, nothing is restored.
    let switchTo (context: ExecutionContext) =
        if not (obj.ReferenceEquals(context, ExecutionContext.Capture())) then
            ExecutionContext.Restore context

type Async2Invocation<'R> = Async2Context -> Task<'R>

/// Runs a tail-call chain: the computation of the final `return!` that started the chain, then each computation that
/// a frame of the chain hands over with its own final `return!`. A handing frame finishes before its callee starts,
/// so the chain runs on a stack and in memory that do not grow with its length.
[<Sealed; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type TailBox<'R>(start: Async2Start) =
    /// The computation that a frame handed over, with the execution context it has to start in.
    let mutable next = ValueNone

    /// Hands over the computation that runs after the calling frame finishes. Finishing reverts the execution context
    /// of the frame, so the callee keeps the one it was handed over in.
    member _.Set(invocation: Async2Invocation<'R>) =
        next <- ValueSome(struct (invocation, ExecutionContext.Capture()))

    member private this.Invoke(invocation: Async2Invocation<'R>) =
        try
            invocation { Start = start; Chain = this }
        with error ->
            Task.FromException<'R> error

    /// Runs the chain from `first` and returns the outcome of its last frame. It waits for each frame that suspends.
    /// When it returns or suspends, runtime async restores the execution context of the caller.
    member this.Run(first: Async2Invocation<'R>) : Task<'R> =
        __runtimeAsyncReturn (
            let mutable task = this.Invoke first
            let mutable running = true

            while running do
                if not task.IsCompleted then
                    AsyncHelpers.UnsafeAwaitAwaiter(task.GetAwaiter())

                match next with
                | ValueSome(struct (invocation, context)) when task.IsCompletedSuccessfully ->
                    next <- ValueNone

                    match context with
                    | null -> ()
                    | handed -> ExecutionContexts.switchTo handed

                    task <- this.Invoke invocation
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

    /// Suspends the frame that awaits it and queues the continuation of that frame on the drain loop of this thread,
    /// so the loop resumes it on a shallow stack.
    [<Struct; NoEquality; NoComparison>]
    type private BounceAwaiter =
        member _.IsCompleted = false
        member _.GetResult() = ()

        interface ICriticalNotifyCompletion with
            member _.OnCompleted continuation = Drain.enqueue continuation
            member _.UnsafeOnCompleted continuation = Drain.enqueue continuation

    /// Starts a nested computation that is too deep to run inline: this frame suspends at once and resumes from the
    /// drain loop, where it runs the computation. The parent awaits the task of this frame.
    let private bounced (start: Async2Start) (invocation: Async2Invocation<'T>) : Task<'T> =
        __runtimeAsyncReturn (
            AsyncHelpers.UnsafeAwaitAwaiter(BounceAwaiter())
            invoke invocation start |> ExceptionCache.await start
        )

    /// Starts on a thread pool thread, which has an empty stack. `Task.Run` flows the execution context into the
    /// start, and the parent resumes when the start completes.
    let private startOnFreshStack (start: Async2Start) (invocation: Async2Invocation<'T>) =
        Task.Run<'T>(fun () -> invoke invocation start)

    /// Starts with a new start state and a drain loop for it on this thread, so the caller observes all synchronous
    /// progress.
    let startIsolated (invocation: Async2Invocation<'T>) cancellationToken =
        let start = { CancellationToken = cancellationToken; Propagating = null }
        Drain.run start (fun () -> invoke invocation start)

    /// A start nested in another one, which awaits it. It runs inline when the stack has room. On a low stack it
    /// bounces to the drain loop of its start when that loop runs further down this stack. Otherwise nothing drains
    /// further down, so it continues on a fresh stack. That happens only away from the thread that made the start,
    /// for example after a frame resumed on the thread pool.
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
        Source(fun ct -> computation.StartTrampolined ct |> ExceptionCache.await null)

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
            | :? TailBox<'T> as tail ->
                // This frame runs in a tail-call chain: hand the callee over and let the frame finish.
                tail.Set computation.Invocation
                Unchecked.defaultof<_>
            | _ ->
                // Start a chain that runs the callee, then each computation it hands over in turn.
                TailBox(ctx.Start).Run computation.Invocation |> ExceptionCache.await ctx.Start

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
