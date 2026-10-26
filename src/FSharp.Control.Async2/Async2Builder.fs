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

module ExceptionCache =
    let private store = ConditionalWeakTable<exn, ExceptionDispatchInfo>()

    let private capture =
        ConditionalWeakTable<exn, ExceptionDispatchInfo>.CreateValueCallback(fun error -> ExceptionDispatchInfo.Capture error)

    /// Rethrows `error` with the dispatch info captured the first time it was seen, so a failure
    /// propagating through many frames does not grow its stack trace at every frame.
    let throwCached (error: exn) : 'T =
        store.GetValue(error, capture).Throw()
        Unchecked.defaultof<_>

    let inline await (task: Task<'T>) =
        try
            AsyncHelpers.Await task
        with error ->
            throwCached error

/// The state threaded through builder code: the cancellation token and, when the computation was
/// started by a tail-call loop, the box a final `return!` hands its callee to. `'R` is the result type
/// of the whole computation expression, so a final `return!` is known to produce exactly that type.
[<Struct; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Context<'R> = {
        CancellationToken: CancellationToken
        Tail: TailBox<'R> voption
    }

and Async2Invocation<'R> = Async2Context<'R> -> Task<'R>

/// Carries the next computation of a tail-call chain from a finished frame back to the loop that owns the chain.
and [<Sealed; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>] TailBox<'R>() =
    let mutable next = ValueNone

    member _.Set(invocation: Async2Invocation<'R>) = next <- ValueSome invocation

    member _.TryTake() =
        let taken = next
        next <- ValueNone
        taken

module Async2Context =
    let create ct = { CancellationToken = ct; Tail = ValueNone }

module internal Async2StartTrampoline =

    /// Nested starts run inline up to this depth. Besides keeping the starting stack shallow, the cap bounds the
    /// chain of continuations that runs when those frames resume later, on a stack whose depth was never checked.
    [<Literal>]
    let private InlineDepthLimit = 128

    /// Per-thread queue of deferred starts and completions. The thread that sets `IsRunning` owns the trampoline
    /// and drains the queue before clearing it, so the queue is empty whenever nobody owns it.
    /// Queued actions handle their own failures and never throw.
    [<Sealed; AllowNullLiteral>]
    type private Trampoline() =
        [<ThreadStatic; DefaultValue>]
        static val mutable private current: Trampoline

        static member Current =
            match Trampoline.current with
            | null ->
                let created = Trampoline()
                Trampoline.current <- created
                created
            | existing -> existing

        static member Exchange(next: Trampoline) =
            let previous = Trampoline.current
            Trampoline.current <- next
            previous

        member val Queue = Queue<unit -> unit>()
        member val IsRunning = false with get, set
        member val Depth = 0 with get, set

        member this.CanRunInline =
            this.Depth < InlineDepthLimit && RuntimeHelpers.TryEnsureSufficientExecutionStack()

        /// Runs `action` as the owner of this trampoline, then drains everything queued meanwhile.
        member inline this.RunAsOwner([<InlineIfLambda>] action: unit -> 'T) =
            this.IsRunning <- true

            try
                action ()
            finally
                try
                    while this.Queue.Count > 0 do
                        this.Queue.Dequeue()()
                finally
                    this.IsRunning <- false

        member inline this.RunNested([<InlineIfLambda>] action: unit -> 'T) =
            this.Depth <- this.Depth + 1

            try
                action ()
            finally
                this.Depth <- this.Depth - 1

    /// A synchronous failure of a custom factory surfaces as a faulted task, like a failure inside builder code.
    let inline private invoke (invocation: Async2Invocation<'T>) ctx =
        try
            invocation ctx
        with error ->
            Task.FromException<'T> error

    let private enqueueCompletion action =
        let trampoline = Trampoline.Current

        if trampoline.IsRunning then
            trampoline.Queue.Enqueue action
        else
            trampoline.RunAsOwner action

    let private runInContext = ContextCallback(fun state -> (state :?> unit -> unit) ())

    /// Defers a start that is too deep to run inline. The returned task is completed from a drain loop and runs
    /// its continuations synchronously there on purpose, so the awaiting parent resumes on a shallow stack.
    let private enqueueStart (trampoline: Trampoline) (invocation: Async2Invocation<'T>) ctx =
        let completion = TaskCompletionSource<'T>()

        let run () =
            try
                let pending = trampoline.RunNested(fun () -> invoke invocation ctx)

                if pending.IsCompleted then
                    completion.TrySetFromTask pending |> ignore
                else
                    // Do not capture a scheduling context for this internal completion bridge.
                    pending.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(fun () ->
                        enqueueCompletion (fun () -> completion.TrySetFromTask pending |> ignore))
            with error ->
                completion.TrySetException error |> ignore

        // The queue is drained after the parent has returned or suspended, so the child must run
        // in the execution context the parent had when it asked for the start.
        match ExecutionContext.Capture() with
        | null -> trampoline.Queue.Enqueue run
        | context -> trampoline.Queue.Enqueue(fun () -> ExecutionContext.Run(context, runInContext, run))

        completion.Task

    let start (invocation: Async2Invocation<'T>) ct =
        let ctx = Async2Context.create ct
        let trampoline = Trampoline.Current

        if not trampoline.IsRunning then
            trampoline.RunAsOwner(fun () -> invoke invocation ctx)
        elif trampoline.CanRunInline then
            trampoline.RunNested(fun () -> invoke invocation ctx)
        else
            enqueueStart trampoline invocation ctx

    /// Starts as the owner of a trampoline, so the caller observes all synchronous progress
    /// instead of having nested work deferred to an enclosing drain loop.
    let startIsolated (invocation: Async2Invocation<'T>) ct =
        let ctx = Async2Context.create ct
        let trampoline = Trampoline.Current

        if not trampoline.IsRunning then
            trampoline.RunAsOwner(fun () -> invoke invocation ctx)
        else
            let previous = Trampoline.Exchange(Trampoline())

            try
                Trampoline.Current.RunAsOwner(fun () -> invoke invocation ctx)
            finally
                Trampoline.Exchange previous |> ignore

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

    let inline merge (left: Source<'A>) (right: Source<'B>) =
        Source(fun ct ->
            let right = __runtimeAsyncReturn (right.Invoke ct)
            let left = left.Invoke ct
            struct (left, right |> AsyncHelpers.Await))

[<Struct; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
type Async2<'T> (invocation: Async2Invocation<'T>) =

    /// Builder plumbing: starts a tail callee directly in the caller's frame, or hands it to a tail-call loop.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member _.Invocation = invocation

    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    // Keep trampoline internals out of runtime-async inline expansions.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member _.StartTrampolined ct =
        Async2StartTrampoline.start invocation ct

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
        Source(fun ct -> computation.StartTrampolined ct |> ExceptionCache.await)

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
                check ctx.CancellationToken
                handler error ctx

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
        fun ctx -> computation.StartTrampolined ctx.CancellationToken |> ExceptionCache.await

    member inline _.ReturnFromFinal(computation: Async2<'T>) : Async2Code<_, 'T> =
        fun ctx ->
            match ctx.Tail with
            | ValueSome tail ->
                // This frame was started by a tail-call loop: hand the callee over and let the frame finish.
                tail.Set computation.Invocation
                Unchecked.defaultof<_>
            | ValueNone ->
                // Own the loop: run the callee in this frame, then each computation it hands over in turn.
                let tail = TailBox()
                let ctx = { ctx with Tail = ValueSome tail }
                let mutable result = Unchecked.defaultof<_>
                let mutable next = ValueSome computation.Invocation

                while next.IsSome do
                    result <- next.Value ctx |> ExceptionCache.await
                    next <- tail.TryTake()

                result

    member inline _.Bind(computation: Async2<'T>, [<InlineIfLambda>] continuation) : Async2Code<'R, 'U> =
        fun ctx ->
            check ctx.CancellationToken
            let result = computation.StartTrampolined ctx.CancellationToken |> ExceptionCache.await
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
