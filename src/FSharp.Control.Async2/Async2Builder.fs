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

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

open AwaitableHelpers

/// The queue of deferred starts and completions of one start from user code, shared by every start nested
/// in it. The thread that owns it drains the queue before giving it up, so the queue is empty whenever nobody
/// owns it, and only the owner touches the queue. Queued actions handle their own failures and never throw.
///
/// It belongs to a start, not to a thread, as in FSharp.Core async. A frame that code outside Async2 resumes
/// inline, for example from `TaskCompletionSource.SetResult`, is not inside its own start's drain, so its next
/// nested start owns the trampoline there and drains it before control goes back to that code.
[<Sealed; AllowNullLiteral; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Trampoline() =
    [<ThreadStatic; DefaultValue>]
    static val mutable private cached: Async2Trampoline

    let mutable queue: Queue<unit -> unit> | null = null

    /// The failure that `ExceptionCache.throwCached` rethrew last in a frame of this start, or null.
    let mutable propagating: exn | null = null

    /// The trampoline that holds the failure for this one, or null when this one holds it.
    let mutable markHolder: Async2Trampoline | null = null

    /// The managed thread id of the owner, or 0.
    let mutable owner = 0

    let mutable cancellationToken = CancellationToken.None

    /// The token of the start. Every start nested in it runs with the same token, so frames read it here and the
    /// state that their suspensions keep stays small.
    member _.CancellationToken = cancellationToken

    member internal _.SetCancellationToken(ct: CancellationToken) = cancellationToken <- ct

    member private this.MarkHolder =
        match markHolder with
        | null -> this
        | holder -> holder

    member private _.Mark
        with get () = propagating
        and set (value: exn | null) = propagating <- value

    /// The failure that `ExceptionCache.throwCached` rethrew last in a frame of this start, or null. The frames
    /// of one start run one after another, and the completion of a frame's task orders its write before the read
    /// of the parent that awaits it, also when the parent resumes on another thread.
    member internal this.Propagating
        with get () = this.MarkHolder.Mark
        and set (value: exn | null) = this.MarkHolder.Mark <- value

    /// For a nested start that cannot share the trampoline of its parent: it shares the failure mark, so the
    /// parent sees that a failure of this start propagates.
    member internal this.ShareMarkOf(parent: Async2Trampoline) = markHolder <- parent.MarkHolder

    /// Forgets the failure mark before the trampoline is reused for another start.
    member internal _.ResetMark() =
        match markHolder with
        | null -> propagating <- null
        | _ -> markHolder <- null

    member internal _.IsOwnedHere = Volatile.Read(&owner) = Environment.CurrentManagedThreadId

    member internal _.TryOwn() =
        Interlocked.CompareExchange(&owner, Environment.CurrentManagedThreadId, 0) = 0

    /// Only the owner enqueues.
    member internal _.Enqueue(action: unit -> unit) =
        match queue with
        | null ->
            let created = Queue<unit -> unit>()
            created.Enqueue action
            queue <- created
        | existing -> existing.Enqueue action

    /// A trampoline owned by this thread for a new start: the one the last start on this thread gave back,
    /// or a new one. A start on another thread may still be releasing the cached one, so it is owned first.
    static member internal Rent() =
        let cached = Async2Trampoline.cached
        Async2Trampoline.cached <- null

        if not (isNull cached) && cached.TryOwn() then
            cached
        else
            let created = Async2Trampoline()
            created.TryOwn() |> ignore
            created

    /// Gives back the trampoline of a start that completed. Builder code reaches a trampoline only through the
    /// frames of its start, and those have all finished, so the next start on this thread can use it.
    static member internal Return(trampoline: Async2Trampoline) = Async2Trampoline.cached <- trampoline

    /// Runs `action` as the owner, after a successful `TryOwn`, then drains everything queued meanwhile.
    member inline internal this.RunAsOwner([<InlineIfLambda>] action: unit -> 'T) =
        try
            action ()
        finally
            this.DrainAndRelease()

    member internal _.DrainAndRelease() =
        try
            match queue with
            | null -> ()
            | queue ->
                while queue.Count > 0 do
                    queue.Dequeue()()
        finally
            Volatile.Write(&owner, 0)

    /// A nested start runs inline unless the stack runs low, so a bounce happens only where running inline
    /// would overflow. A bounced child is incomplete when its parent awaits it, so under a SynchronizationContext
    /// or a non-default TaskScheduler the parent's resumption would be posted there, and a blocked context never
    /// runs it. There nested starts always run inline, as they would without a trampoline.
    static member internal CanRunInline =
        RuntimeHelpers.TryEnsureSufficientExecutionStack()
        || not (isNull SynchronizationContext.Current)
        || not (obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default))

module ExceptionCache =
    let private store = ConditionalWeakTable<exn, ExceptionDispatchInfo>()

    let private capture =
        ConditionalWeakTable<exn, ExceptionDispatchInfo>.CreateValueCallback(fun error -> ExceptionDispatchInfo.Capture error)

    /// Rethrows `error` with the dispatch info captured where it was thrown, so a failure propagating through
    /// many frames does not grow its stack trace at every frame. `owner` is the trampoline of the awaiting frame,
    /// or null for an await outside builder code.
    ///
    /// The trampoline holds the failure that this function rethrew last in a frame of the start, so the awaiting
    /// parent tells propagation from a new throw of the same exception object on any thread. A user handler that
    /// rethrows the same object propagates the failure. Any other failure is a new throw: from user code, from a
    /// foreign awaiter, or a throw after a handler handled the failure. It replaces the entry, so a new throw of
    /// the same exception object does not get an old trace.
    let throwCached (owner: Async2Trampoline | null) (error: exn) : 'T =
        let edi =
            match owner with
            | null ->
                let edi = ExceptionDispatchInfo.Capture error
                store.AddOrUpdate(error, edi)
                edi
            | owner when obj.ReferenceEquals(owner.Propagating, error) -> store.GetValue(error, capture)
            | owner ->
                let edi = ExceptionDispatchInfo.Capture error
                store.AddOrUpdate(error, edi)
                owner.Propagating <- error
                edi

        edi.Throw()
        Unchecked.defaultof<_>

    /// For a failure that a user handler handled. A later throw of the same object is then a new throw.
    let handled (trampoline: Async2Trampoline) = trampoline.Propagating <- null

    /// Awaits `task` and rethrows its failure through the cache. The wait itself never throws: awaiting a
    /// faulted task directly would throw once in the awaiter, again where the suspended frame resumes, and
    /// a third time here, so a failure crossing one frame would cost three throws instead of one.
    /// `UnsafeAwaitAwaiter` only waits for completion and never calls `GetResult`; unlike awaiting a
    /// `ConfigureAwait(SuppressThrowing)` awaitable, it allocates nothing beyond what `Await` does.
    let inline await (owner: Async2Trampoline | null) (task: Task<'T>) =
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

/// The state threaded through builder code: the trampoline of the start, which holds its cancellation token, and, when
/// the computation was started in a tail-call chain, the box a final `return!` hands its callee to. `'R` is the
/// result type of the whole computation expression, so a final `return!` is known to produce exactly that type.
[<Struct; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Context<'R> = {
        Trampoline: Async2Trampoline
        Tail: TailBox<'R> | null
    }
    with
        member ctx.CancellationToken = ctx.Trampoline.CancellationToken

and Async2Invocation<'R> = Async2Context<'R> -> Task<'R>

/// Runs a tail-call chain: the computation of the final `return!` that started the chain, then each computation that
/// a frame of the chain hands over with its own final `return!`. A handing frame finishes, so the chain runs on a
/// stack and in memory that do not grow with its length.
///
/// The driver runs the frames one after another. When a frame suspends, the driver does not wait for it: the
/// completion of the frame's task continues the chain on the thread that completes it. Only the outcome of the last
/// frame matters, and it completes the task of the chain. So a suspension costs a continuation on the frame's task
/// instead of a suspension and a resumption of a waiting frame.
and [<Sealed; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>] TailBox<'R>() =
    let mutable next = ValueNone
    let mutable context: ExecutionContext | null = null
    let mutable chainContext = Unchecked.defaultof<Async2Context<'R>>

    /// The task of the frame that suspended, while the chain waits for it.
    let mutable pending: Task<'R> | null = null

    /// The task of a chain that suspended. Created at the first suspension, so a chain that completes synchronously
    /// returns the task of its last frame.
    let mutable completion: TaskCompletionSource<'R> | null = null
    let mutable onPendingCompleted: Action | null = null

    /// Hands over the computation that runs after the calling frame finishes.
    member _.Set(invocation: Async2Invocation<'R>) =
        next <- ValueSome invocation
        // The handing frame finishes before the driver starts the callee, and finishing reverts the execution
        // context, so keep the one the callee has to start in.
        context <- ExecutionContext.Capture()

    /// Runs frames until the chain completes, and returns the task of the last frame, or null when a frame suspends.
    /// After a suspension another thread can continue the chain at once, so nothing here touches the box after it.
    member private this.Run() : Task<'R> | null =
        let mutable result = null
        let mutable running = true

        while running do
            let invocation = next.Value
            next <- ValueNone

            match context with
            | null -> ()
            | handed ->
                context <- null

                if not (obj.ReferenceEquals(handed, ExecutionContext.Capture())) then
                    ExecutionContext.Restore handed

            let task =
                try
                    invocation chainContext
                with error ->
                    Task.FromException<'R> error

            if not task.IsCompleted then
                running <- false
                pending <- task

                if isNull completion then
                    completion <- TaskCompletionSource<'R>()
                    onPendingCompleted <- Action(this.PendingCompleted)

                task.GetAwaiter().UnsafeOnCompleted(onPendingCompleted)
            elif next.IsNone || not task.IsCompletedSuccessfully then
                // The frame returned a value or failed: the chain is complete.
                next <- ValueNone
                context <- null
                running <- false
                result <- task

        result

    /// Runs `this.Run` and then reverts the execution context of the calling thread, which `Run` switches to the one
    /// each frame was handed over in.
    member private this.RunInCallerContext() =
        let callerContext = ExecutionContext.Capture()

        try
            this.Run()
        finally
            match callerContext with
            | null -> ()
            | caller ->
                if not (obj.ReferenceEquals(caller, ExecutionContext.Capture())) then
                    ExecutionContext.Restore caller

    member private this.PendingCompleted() =
        let task = nonNull pending
        pending <- null

        let last =
            if next.IsSome && task.IsCompletedSuccessfully then
                this.RunInCallerContext()
            else
                next <- ValueNone
                context <- null
                task

        match last with
        | null -> ()
        | last -> (nonNull completion).TrySetFromTask last |> ignore

    /// Starts the chain with `first` and returns its task.
    member this.Start(ctx: Async2Context<'R>, first: Async2Invocation<'R>) : Task<'R> =
        chainContext <- { ctx with Tail = this }
        next <- ValueSome first

        match this.RunInCallerContext() with
        | null -> (nonNull completion).Task
        | last -> last

module Async2Context =
    let create trampoline = { Trampoline = trampoline; Tail = null }

module internal Async2StartTrampoline =

    /// A synchronous failure of a custom factory surfaces as a faulted task, like a failure inside builder code.
    let inline private invoke (invocation: Async2Invocation<'T>) ctx =
        try
            invocation ctx
        with error ->
            Task.FromException<'T> error

    /// Runs a completion on the owner's queue if this thread owns the trampoline. Otherwise it runs here: as the
    /// owner when nobody owns it, so completions that follow are queued flat, or directly when another thread
    /// owns it.
    let private enqueueCompletion (trampoline: Async2Trampoline) action =
        if trampoline.IsOwnedHere then
            trampoline.Enqueue action
        elif trampoline.TryOwn() then
            trampoline.RunAsOwner action
        else
            action ()

    let private runInContext = ContextCallback(fun state -> (state :?> unit -> unit) ())

    /// Defers a start that is too deep to run inline. The returned task is completed from a drain loop and runs
    /// its continuations synchronously there on purpose, so the awaiting parent resumes on a shallow stack.
    let private enqueueStart (trampoline: Async2Trampoline) (invocation: Async2Invocation<'T>) ctx =
        let completion = TaskCompletionSource<'T>()

        let run () =
            try
                let pending = invoke invocation ctx

                if pending.IsCompleted then
                    completion.TrySetFromTask pending |> ignore
                else
                    // Do not capture a scheduling context for this internal completion bridge.
                    pending.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(fun () ->
                        enqueueCompletion trampoline (fun () -> completion.TrySetFromTask pending |> ignore))
            with error ->
                completion.TrySetException error |> ignore

        // The queue is drained after the parent has returned or suspended, so the child must run
        // in the execution context the parent had when it asked for the start.
        match ExecutionContext.Capture() with
        | null -> trampoline.Enqueue run
        | context -> trampoline.Enqueue(fun () -> ExecutionContext.Run(context, runInContext, run))

        completion.Task

    let private startOwned (trampoline: Async2Trampoline) (invocation: Async2Invocation<'T>) ct =
        trampoline.SetCancellationToken ct
        let task = trampoline.RunAsOwner(fun () -> invoke invocation (Async2Context.create trampoline))

        // A start that suspended still has frames that hold its trampoline.
        if task.IsCompleted then
            // The next start can reuse the trampoline, so its frames must not match a failure that this start left.
            trampoline.ResetMark()
            Async2Trampoline.Return trampoline

        task

    /// Starts as the owner of a new trampoline, so the caller observes all synchronous progress.
    let startIsolated (invocation: Async2Invocation<'T>) ct =
        startOwned (Async2Trampoline.Rent()) invocation ct

    /// A start nested in another one, which awaits it: it shares the parent's trampoline. When another thread owns
    /// that one, the parent runs concurrently with the owner, and the start gets a trampoline of its own. That
    /// trampoline shares the failure mark of the parent, so a failure that propagates keeps its cached trace.
    let start (trampoline: Async2Trampoline) (invocation: Async2Invocation<'T>) =
        let ctx = Async2Context.create trampoline

        if trampoline.IsOwnedHere then
            if Async2Trampoline.CanRunInline then
                invoke invocation ctx
            else
                enqueueStart trampoline invocation ctx
        elif trampoline.TryOwn() then
            trampoline.RunAsOwner(fun () -> invoke invocation ctx)
        else
            let own = Async2Trampoline.Rent()
            own.ShareMarkOf trampoline
            startOwned own invocation trampoline.CancellationToken

/// Bound with `let!` in `async2` or `asyncSeq2` to read the token the computation was started with,
/// without starting another computation the way `Async2.CancellationToken` does.
[<Struct; NoEquality; NoComparison>]
type CurrentCancellationToken = CurrentCancellationToken

module Async2BuilderSources =

    type Source<'T> = delegate of CancellationToken -> 'T

    let inline ofAwaitable awaitable =
        let awaiter = Awaitable.getAwaiter awaitable

        Source(fun _ ->
            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

    let inline ofCancellableAwaitable cancellableAwaitable =
        Source(fun ct ->
            let awaiter =
                cancellableAwaitable ct
                |> Awaitable.getAwaiter

            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

    let inline merge ([<InlineIfLambda>] left: Source<'A>) ([<InlineIfLambda>] right: Source<'B>) =
        Source(fun ct ->
            let right = __runtimeAsyncReturn (right.Invoke ct)
            let left = left.Invoke ct
            struct (left, right |> AsyncHelpers.Await))

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
        Async2StartTrampoline.startIsolated invocation ct

    /// Builder plumbing: starts this computation nested in the start that owns `trampoline`.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member _.StartNested(trampoline) =
        Async2StartTrampoline.start trampoline invocation

    member internal _.StartInIsolatedTrampoline ct =
        Async2StartTrampoline.startIsolated invocation ct

/// Builder code: `'R` is the result type of the whole computation, `'T` the type of this piece.
type Async2Code<'R, 'T> = Async2Context<'R> -> 'T

open Async2BuilderSources

module Async2Builder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current
        && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

    let inline check (cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()

    let inline ofAsync2 (computation: Async2<'T>) =
        Source(fun ct -> computation.StartTrampolined ct |> ExceptionCache.await null)

open Async2Builder

type Async2Builder() =

    // The code type of the builder is `Async2Context -> 'T`, i.e. the cancellation token and the tail box
    // are passed along as state to every delayed continuation.
    member inline _.Delay([<InlineIfLambda>] generator: unit -> Async2Code<'R, 'T>) : Async2Code<'R, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            let code = generator ()
            code ctx

    member inline _.Zero() : Async2Code<_, unit> =
        fun ctx ->
            check ctx.CancellationToken
            ()

    member inline _.Return(value: 'T) : Async2Code<_, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            value

    member inline _.Combine([<InlineIfLambda>] first, [<InlineIfLambda>] second) : Async2Code<_,'T> =
        fun ctx ->
            check ctx.CancellationToken
            first (ctx) |> ignore
            second ctx

    member inline _.TryWith ([<InlineIfLambda>] body, [<InlineIfLambda>] handler) : Async2Code<_, 'T> =
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
                ExceptionCache.handled ctx.Trampoline
                result

    member inline _.TryFinally ([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) : Async2Code<_, 'T> =
        fun ctx ->
            try
                check ctx.CancellationToken
                body ctx
            finally
                compensation ()

    member inline _.Using(resource: 'T :> IDisposable | null, [<InlineIfLambda>] body) : Async2Code<_, 'U> =
        fun ctx ->
            try
                check ctx.CancellationToken
                body resource ctx
            finally
                if not (isNull (box resource)) then resource.Dispose()

    member inline _.While(guard: unit -> bool, [<InlineIfLambda>] body) : Async2Code<_, unit> =
        fun ctx ->
            while guard () do
                check ctx.CancellationToken
                body ctx
            check ctx.CancellationToken

    member inline _.For(sequence: seq<'T>, [<InlineIfLambda>] body) : Async2Code<_, unit> =
        fun ctx ->
            for item in sequence do
                check ctx.CancellationToken
                body item ctx
            check ctx.CancellationToken

    member inline _.Bind([<InlineIfLambda>] source: Source<'T>, [<InlineIfLambda>] continuation) : Async2Code<_, 'U> =
        fun ctx ->
            check ctx.CancellationToken
            continuation (source.Invoke ctx.CancellationToken) ctx

    member inline _.ReturnFrom([<InlineIfLambda>] source: Source<'T>) : Async2Code<_, 'T> =
        fun ctx -> source.Invoke ctx.CancellationToken

    member inline _.ReturnFromFinal([<InlineIfLambda>] source: Source<'T>) : Async2Code<_, 'T> =
        fun ctx -> source.Invoke ctx.CancellationToken

    member inline _.MergeSources([<InlineIfLambda>] left: Source<'A>, [<InlineIfLambda>] right: Source<'B>) =
        merge left right

    member inline _.Run([<InlineIfLambda>] code: Async2Code<'T, 'T>) : Async2<'T> =
        Async2(fun ctx -> __runtimeAsyncReturn(code ctx))

    member inline _.Source(computation: Async2<'T>) = computation

    member inline _.ReturnFrom(computation: Async2<'T>) : Async2Code<_, 'T> =
        fun ctx -> computation.StartNested ctx.Trampoline |> ExceptionCache.await ctx.Trampoline

    member inline _.ReturnFromFinal(computation: Async2<'T>) : Async2Code<_, 'T> =
        fun ctx ->
            match ctx.Tail with
            | null ->
                // Start a chain that runs the callee, then each computation it hands over in turn.
                TailBox().Start(ctx, computation.Invocation) |> ExceptionCache.await ctx.Trampoline
            | tail ->
                // This frame runs in a tail-call chain: hand the callee over and let the frame finish.
                tail.Set computation.Invocation
                Unchecked.defaultof<_>

    member inline _.Bind(computation: Async2<'T>, [<InlineIfLambda>] continuation) : Async2Code<'R, 'U> =
        fun ctx ->
            check ctx.CancellationToken
            let result = computation.StartNested ctx.Trampoline |> ExceptionCache.await ctx.Trampoline
            continuation result ctx

    member inline _.MergeSources(left: Async2<'A>, [<InlineIfLambda>] right: Source<'B>) =
        merge (ofAsync2 left) right

    member inline _.MergeSources([<InlineIfLambda>] left: Source<'A>, right: Async2<'B>) =
        merge left (ofAsync2 right)

    member inline _.MergeSources(left: Async2<'A>, right: Async2<'B>) =
        merge (ofAsync2 left) (ofAsync2 right)

[<AutoOpen>]
module Async2BuilderAsyncDisposableExtensions =
    type Async2Builder with
        member inline _.Using(resource: 'T :> IAsyncDisposable | null, [<InlineIfLambda>] body) : Async2Code<_, 'U> =
            fun ctx ->
                try
                    check ctx.CancellationToken
                    body resource ctx
                finally
                    if not (isNull (box resource)) then
                        resource.DisposeAsync() |> AsyncHelpers.Await

        member inline this.For(sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body) : Async2Code<_, unit> =
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
