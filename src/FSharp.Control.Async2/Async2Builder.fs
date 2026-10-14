namespace Microsoft.FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources
open System.Collections.Generic
open Microsoft.FSharp.Core.CompilerServices

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

module Async2BuilderSources =

    // A delegate to unify dissimilar builder source types, this allows us to have no additional Bind or MergeSources overloads.
    // The delegate takes the cancellation token threaded through the builder; its invocation is inlined, so this is zero cost.
    type Started<'T> = delegate of unit -> 'T
    // We need to distinguish between hot and cold awaitables, we can pass the cancellation token only to the cold ones.
    // Ideally the signature should be CancellationToken -> Started<'T>, but the Started<_> delegates execute AsyncHelpers.Await
    // and must be inlined unconditionally into async method body.
    type Cold<'T> = delegate of CancellationToken -> 'T

    let inline startAwaitable awaitable =
        let awaiter = Awaitable.getAwaiter awaitable

        Started(fun () ->
            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

    let inline startCancellableAwaitable cancellableAwaitable =
        Cold(fun ct ->
            let awaiter =
                cancellableAwaitable ct
                |> Awaitable.getAwaiter

            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

open Async2BuilderSources

/// Carries the next computation of a tail-call chain from a finished frame back to the loop that owns the chain.
[<AllowNullLiteral; Sealed; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type TailBox<'T>() =
    let mutable next = Unchecked.defaultof<CancellationToken -> TailBox<'T> -> ValueTask<'T>>
    member _.HasNext = not (isNull (box next))

    member _.Set(start: CancellationToken -> TailBox<'T> -> ValueTask<'T>) = next <- start

    member _.Take() =
        let value = next
        next <- Unchecked.defaultof<_>
        value

module internal Async2StartTrampoline =
    [<AllowNullLiteral>]
    type private State() =
        let queue = Queue<unit -> unit>()
        member _.Queue = queue
        member val IsRunning = false with get, set
        member val Depth = 0 with get, set

    [<AbstractClass; Sealed>]
    type private StateHolder =
        [<ThreadStatic; DefaultValue>]
        static val mutable private state: State

        static member Get() =
            if isNull StateHolder.state then
                StateHolder.state <- State()
            StateHolder.state

        static member Set(value) =
            StateHolder.state <- value

    let private inlineDepthLimit = 128
    let private stackCheckInterval = 16

    let private drain (current: State) =
        while current.Queue.Count > 0 do
            current.Queue.Dequeue()()

    let private enqueueCompletion action =
        let current = StateHolder.Get()

        if current.IsRunning then
            current.Queue.Enqueue(action)
        else
            current.IsRunning <- true

            try
                current.Queue.Enqueue(action)
                drain current
            finally
                current.IsRunning <- false

    type private QueuedStart<'T>(start: CancellationToken -> TailBox<'T> -> ValueTask<'T>, cancellationToken: CancellationToken) =
        static let runInContext = ContextCallback(fun state -> (state :?> QueuedStart<'T>).Start())
        let mutable source = ManualResetValueTaskSourceCore<'T>()
        let mutable awaiter = Unchecked.defaultof<ConfiguredValueTaskAwaitable<'T>.ConfiguredValueTaskAwaiter>

        member private _.Complete() =
            let mutable result = Unchecked.defaultof<'T>
            let mutable error = null

            try
                result <- awaiter.GetResult()
            with caught ->
                error <- caught

            if isNull error then
                source.SetResult(result)
            else
                source.SetException(error)

        member private this.Start() =
            let mutable error = null
            let mutable completed = false

            try
                // Do not capture a scheduling context for this internal completion bridge.
                let current = StateHolder.Get()
                current.Depth <- current.Depth + 1
                let pending =
                    try
                        start cancellationToken null
                    finally
                        current.Depth <- current.Depth - 1
                awaiter <- pending.ConfigureAwait(false).GetAwaiter()
                completed <- awaiter.IsCompleted
            with caught ->
                error <- caught

            if not (isNull error) then
                source.SetException(error)
            elif completed then
                this.Complete()
            else
                try
                    awaiter.UnsafeOnCompleted(Action(fun () ->
                        enqueueCompletion (fun () -> this.Complete())))
                with error ->
                    source.SetException(error)

        member this.Run(context: ExecutionContext) =
            if isNull context then
                this.Start()
            else
                ExecutionContext.Run(context, runInContext, this)

        member this.ValueTask = ValueTask<'T>(this, source.Version)

        interface IValueTaskSource<'T> with
            member _.GetResult token = source.GetResult token
            member _.GetStatus token = source.GetStatus token
            member _.OnCompleted(continuation, state, token, flags) =
                source.OnCompleted(continuation, state, token, flags)

    let start<'T> (start: CancellationToken -> TailBox<'T> -> ValueTask<'T>) (cancellationToken: CancellationToken) =
        let current = StateHolder.Get()

        if current.IsRunning then
            // Runtime-async continuation frames can outgrow the trampoline's nested-start depth.
            if
                current.Depth < inlineDepthLimit
                && (current.Depth % stackCheckInterval <> 0 || RuntimeHelpers.TryEnsureSufficientExecutionStack())
            then
                current.Depth <- current.Depth + 1
                try
                    try
                        start cancellationToken null
                    with error ->
                        ValueTask.FromException<'T>(error)
                finally
                    current.Depth <- current.Depth - 1
            else
                let completion = QueuedStart<'T>(start, cancellationToken)
                // The queue is drained after the parent has returned or suspended, so the child must
                // run in the execution context the parent had when it asked for the start.
                let context = ExecutionContext.Capture()
                current.Queue.Enqueue(fun () -> completion.Run(context))
                completion.ValueTask
        else
            current.IsRunning <- true

            try
                start cancellationToken null
            finally
                try
                    drain current
                finally
                    current.IsRunning <- false

    let startIsolated<'T> (startComputation: CancellationToken -> TailBox<'T> -> ValueTask<'T>, cancellationToken: CancellationToken) =
        let previous = StateHolder.Get()
        StateHolder.Set(State())

        try
            startComputation cancellationToken null
        finally
            StateHolder.Set(previous)

/// The state threaded through builder code: the cancellation token and, when the computation was
/// started by a tail-call loop, the box a final `return!` hands its callee to. `'R` is the result type
/// of the whole computation expression, so a final `return!` is known to produce exactly that type.
[<Struct; NoEquality; NoComparison; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Context<'R> =
    val CancellationToken: CancellationToken
    val Tail: TailBox<'R>
    new(cancellationToken, tail) = { CancellationToken = cancellationToken; Tail = tail }

[<Struct; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
type Async2<'T> private (start: CancellationToken -> TailBox<'T> -> ValueTask<'T>) =
    new (start: CancellationToken -> ValueTask<'T>) = Async2<'T>(fun ct _ -> start ct)

    member _.Start ct = start ct null |> _.AsTask()

    /// Builder-produced computations receive the tail box; custom ones ignore it.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    static member FromTailStart(start: CancellationToken -> TailBox<'T> -> ValueTask<'T>) = Async2<'T>(start)

    // Starts directly: the tail box must reach the callee's own frame, so this bypasses the trampoline queue.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member _.StartTail(tail: TailBox<'T>, ct: CancellationToken) = start ct tail

    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member _.HandOff(tail: TailBox<'T>) = tail.Set start

    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    // Keep trampoline internals out of runtime-async inline expansions.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    member _.StartTrampolined ct =
        Async2StartTrampoline.start start ct

    member internal _.StartInIsolatedTrampoline ct =
        Async2StartTrampoline.startIsolated (start, ct)

[<AbstractClass; Sealed; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2TailCall =
    /// Runs `first` and every computation it hands over through a final `return!`, in constant stack.
    static member Loop<'T>(first: Async2<'T>, ct: CancellationToken) : ValueTask<'T> =
        __runtimeAsyncReturnValueTask (
            let tail = TailBox<'T>()
            let mutable result = first.StartTail(tail, ct) |> AsyncHelpers.Await

            while tail.HasNext do
                let next = tail.Take()
                result <- next ct tail |> AsyncHelpers.Await

            result
        )

/// Builder code: `'R` is the result type of the whole computation, `'T` the type of this piece.
type Async2Code<'R, 'T> = Async2Context<'R> -> 'T

module Async2Builder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current
        && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

    let inline check (cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()

    let inline coldOf (computation: Async2<'T>) =
        Cold(fun ct -> computation.StartTrampolined ct |> AsyncHelpers.Await)

    /// The same token without a tail box: used where a final `return!` must not escape (try regions, non-tail code).
    let inline noTail (ctx: Async2Context<'R>) = Async2Context<'R>(ctx.CancellationToken, null)

open Async2Builder

type Async2Builder() =

    // The code type of the builder is `Async2Context -> 'T`, i.e. the cancellation token and the tail box
    // are passed along as state to every delayed continuation.
    member inline _.Delay([<InlineIfLambda>] generator: unit -> Async2Code<'R, 'T>) : Async2Code<'R, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            let code = generator ()
            code ctx

    member inline _.Zero() : Async2Code<'R, unit> =
        fun ctx ->
            check ctx.CancellationToken
            ()

    member inline _.Return(value: 'T) : Async2Code<'R, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            value

    member inline _.Combine([<InlineIfLambda>] first, [<InlineIfLambda>] second) : Async2Code<'R, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            first (noTail ctx) |> ignore
            second ctx

    member inline _.TryWith ([<InlineIfLambda>] body, [<InlineIfLambda>] handler) : Async2Code<'R, 'T> =
        fun ctx ->
            check ctx.CancellationToken
            let inner = noTail ctx
            try
                body inner
            with error ->
                check ctx.CancellationToken
                handler error ctx

    member inline _.TryFinally ([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) : Async2Code<'R, 'T> =
        fun ctx ->
            let inner = noTail ctx
            try
                check ctx.CancellationToken
                body inner
            finally
                compensation ()

    member inline _.Using(resource: 'T :> IDisposable | null, [<InlineIfLambda>] body) : Async2Code<'R, 'U> =
        fun ctx ->
            let inner = noTail ctx
            try
                check ctx.CancellationToken
                body resource inner
            finally
                if not (isNull (box resource)) then resource.Dispose()

    member inline _.While(guard: unit -> bool, [<InlineIfLambda>] body) : Async2Code<'R, unit> =
        fun ctx ->
            let ctx = noTail ctx
            while guard () do
                check ctx.CancellationToken
                body ctx
            check ctx.CancellationToken

    member inline _.For(sequence: seq<'T>, [<InlineIfLambda>] body) : Async2Code<'R, unit> =
        fun ctx ->
            let ctx = noTail ctx
            for item in sequence do
                check ctx.CancellationToken
                body item ctx
            check ctx.CancellationToken

    member inline _.Bind([<InlineIfLambda>] awaited: Started<'T>,[<InlineIfLambda>] continuation) : Async2Code<'R, 'U> =
        fun ctx ->
            check ctx.CancellationToken
            continuation (awaited.Invoke()) ctx

    member inline this.Bind([<InlineIfLambda>] cancellable: Cold<'T>,[<InlineIfLambda>] continuation) : Async2Code<'R, 'U> =
        fun ctx ->
            check ctx.CancellationToken
            continuation (cancellable.Invoke ctx.CancellationToken) ctx

    member inline _.ReturnFrom([<InlineIfLambda>] awaited: Started<'T>) : Async2Code<'R, 'T> =
        fun _ -> awaited.Invoke()

    member inline _.ReturnFromFinal([<InlineIfLambda>] awaited: Started<'T>) : Async2Code<'R, 'T> =
        fun _ -> awaited.Invoke()

    member inline _.ReturnFrom([<InlineIfLambda>] cancellable: Cold<'T>) : Async2Code<'R, 'T> =
        fun ctx -> cancellable.Invoke ctx.CancellationToken

    member inline _.ReturnFromFinal([<InlineIfLambda>] cancellable: Cold<'T>) : Async2Code<'R, 'T> =
        fun ctx -> cancellable.Invoke ctx.CancellationToken

    member inline this.Bind(computation: Async2<'T>, [<InlineIfLambda>] continuation) : Async2Code<'R, 'U> =
        this.Bind(coldOf computation, continuation)

    member inline this.ReturnFrom(computation: Async2<'T>) : Async2Code<'R, 'T> =
        this.ReturnFrom(coldOf computation)

    // Nothing follows a final `return!`, so the frame can finish before the callee starts:
    // the callee is handed to the loop that owns the chain instead of being awaited. Without an owner
    // (this frame was not started by a loop) this frame becomes the loop.
    member inline _.ReturnFromFinal(computation: Async2<'R>) : Async2Code<'R, 'R> =
        fun ctx ->
            if isNull ctx.Tail then
                Async2TailCall.Loop(computation, ctx.CancellationToken) |> AsyncHelpers.Await
            else
                computation.HandOff ctx.Tail
                Unchecked.defaultof<'R>

    member inline _.MergeSources([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: Started<'B>) =
        let left = left.Invoke()
        let right = right.Invoke()
        Started(fun () -> struct (left, right))

    member inline this.MergeSources([<InlineIfLambda>] left: Cold<'A>, [<InlineIfLambda>] right: Cold<'B>) =
        Cold(fun ct ->
            let right = __runtimeAsyncReturnValueTask (right.Invoke ct)
            let left = left.Invoke ct

            struct (left,
                    right
                    |> AsyncHelpers.Await)
        )

    member inline this.MergeSources ([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: Cold<'B>) =
        Cold(fun ct ->
            let right = right.Invoke ct
            let left = left.Invoke()
            struct (left, right)
        )

    member inline this.MergeSources ([<InlineIfLambda>] left: Cold<'A>, [<InlineIfLambda>] right: Started<'B>) =
        Cold(fun ct ->
            let left = left.Invoke ct
            let right = right.Invoke()
            struct (left, right)
        )

    member inline this.MergeSources(left: Async2<'A>, right: Async2<'B>) =
        this.MergeSources(coldOf left, coldOf right)

    member inline this.MergeSources(left: Async2<'A>, [<InlineIfLambda>] right: Started<'B>) =
        this.MergeSources(coldOf left, right)

    member inline this.MergeSources(left: Async2<'A>, [<InlineIfLambda>] right: Cold<'B>) =
        this.MergeSources(coldOf left, right)

    member inline this.MergeSources([<InlineIfLambda>] left: Started<'A>, right: Async2<'B>) =
        this.MergeSources(left, coldOf right)

    member inline this.MergeSources([<InlineIfLambda>] left: Cold<'A>, right: Async2<'B>) =
        this.MergeSources(left, coldOf right)

    member inline _.Source(computation: Async2<'T>) = computation

    member inline _.Run([<InlineIfLambda>] code: Async2Code<'T, 'T>) : Async2<'T> =
        Async2<'T>.FromTailStart(fun ct tail ->
            __runtimeAsyncReturnValueTask (code (Async2Context<'T>(ct, tail))))

[<AutoOpen>]
module Async2BuilderAsyncDisposableExtensions =
    type Async2Builder with
        member inline _.Using(resource: 'T :> IAsyncDisposable | null, [<InlineIfLambda>] body) : Async2Code<'R, 'U> =
            fun ctx ->
                let inner = noTail ctx
                try
                    check ctx.CancellationToken
                    body resource inner
                finally
                    if not (isNull (box resource)) then
                        resource.DisposeAsync() |> AsyncHelpers.Await

        member inline this.For(sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body) : Async2Code<'R, unit> =
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
        member inline _.Source(awaitable) = startAwaitable awaitable

        member inline this.Source([<InlineIfLambda>] coldAwaitable) =
            startAwaitable (coldAwaitable ())

        member inline this.Source([<InlineIfLambda>] cancellableAwaitable) =
            startCancellableAwaitable cancellableAwaitable

[<AutoOpen>]
module Async2BuilderSourceExtensions =
    type Async2Builder with

        // Accepted sources for For
        member inline _.Source(sequence: 'T seq) = sequence
        member inline _.Source(sequence: IAsyncEnumerable<'T>) = sequence

        // Cannonical runtime async Bind sources
        member inline _.Source(task: Task<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: Task) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask) = Started(fun () -> task |> AsyncHelpers.Await)

         //Cold start sources
        member inline this.Source(computation: Async<'T>) =
            Cold(fun ct -> Async.StartImmediateAsTask(computation, ct) |> AsyncHelpers.Await)

[<AutoOpen>]
module Async2BuilderImpl =
    let async2 = Async2Builder()
