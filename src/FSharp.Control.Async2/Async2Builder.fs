namespace Microsoft.FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
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

/// Compiler support for native completion and cancellation propagation.
[<Struct; RequireQualifiedAccess; NoEquality; NoComparison;
  System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type Async2Result<'T> =
    | Completed of value: 'T
    | Cancelled of cancellation: OperationCanceledException

/// Compiler support for rethrowing awaited exceptions with their original dispatch information.
[<RequireQualifiedAccess; System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
module ExceptionCache =
    let private store = ConditionalWeakTable<exn, ExceptionDispatchInfo>()
    let private capture = ConditionalWeakTable<exn, ExceptionDispatchInfo>.CreateValueCallback(ExceptionDispatchInfo.Capture)
    let private cancellations = ConditionalWeakTable<Task, OperationCanceledException>()

    let captureOrRetrieve (error: exn) =
        store.GetValue(error, capture)

    let internal registerCancellation task error =
        captureOrRetrieve error |> ignore
        cancellations.Add(task, error)

    let tryGetCancellation task =
        cancellations.TryGetValue task

    let getCancellation (task: Task<'T>) =
        match tryGetCancellation task with
        | true, error -> error
        | false, _ ->
            try
                task.GetAwaiter().GetResult() |> ignore
                invalidOp "A canceled task did not throw a cancellation exception."
            with :? OperationCanceledException as error -> error

    let inline throwException (error: exn) : 'T =
        (captureOrRetrieve error).Throw()
        Unchecked.defaultof<'T>

    let inline getResultOrThrow awaiter =
        try
            Awaiter.getResult awaiter
        with error ->
            throwException error

    let inline awaitAwaiter awaiter =
        if not (Awaiter.isCompleted awaiter) then
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
        getResultOrThrow awaiter

    let inline awaitTask (task: Task<'T>) =
        let awaiter: TaskAwaiter<'T> = task.GetAwaiter()
        if not awaiter.IsCompleted then
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
        if task.IsCompletedSuccessfully then task.Result
        elif task.IsCanceled then
            match tryGetCancellation task with
            | true, error -> throwException error
            | false, _ -> getResultOrThrow awaiter
        else getResultOrThrow awaiter

    let inline awaitResultTask (ct: CancellationToken) (task: Task<Async2Result<'T>>) =
        let awaiter = task.GetAwaiter()
        if not awaiter.IsCompleted then
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
        if task.IsCompletedSuccessfully then task.Result
        elif task.IsCanceled && ct.IsCancellationRequested then
            Async2Result.Cancelled(getCancellation task)
        else
            awaitTask task

module Async2BuilderSources =

    // Delegate invocations containing runtime-async awaits must be inlined into the method body.
    type Started<'T> = delegate of unit -> 'T
    // We need to distinguish between hot and cold awaitables, we can pass the cancellation token only to the cold ones.
    // Ideally the signature should be CancellationToken -> Started<'T>, but the Started<_> delegates execute AsyncHelpers.Await
    // and must be inlined unconditionally into async method body.
    type Cold<'T> = delegate of CancellationToken -> 'T
    type CancellableResult<'T> = delegate of CancellationToken -> Async2Result<'T>

    let inline startAwaitable awaitable =
        let awaiter = Awaitable.getAwaiter awaitable

        Started(fun () ->
            ExceptionCache.awaitAwaiter awaiter
        )

    let inline startCancellableAwaitable cancellableAwaitable =
        Cold(fun ct ->
            let awaiter =
                cancellableAwaitable ct
                |> Awaitable.getAwaiter

            ExceptionCache.awaitAwaiter awaiter
        )

open Async2BuilderSources

module internal Async2StartTrampoline =
    type private State() =
        let queue = Queue<unit -> unit>()
        member _.Queue = queue
        member val IsRunning = false with get, set

    let private state = new ThreadLocal<State>(fun () -> State())

    let private drain (current: State) =
        while current.Queue.Count > 0 do
            current.Queue.Dequeue()()

    let private enqueueCompletion action =
        let current = state.Value

        if current.IsRunning then
            current.Queue.Enqueue(action)
        else
            current.IsRunning <- true

            try
                current.Queue.Enqueue(action)
                drain current
            finally
                current.IsRunning <- false

    let private complete<'T> (task: Task<'T>) (completion: TaskCompletionSource<'T>) =
        if task.IsCanceled then
            let error = ExceptionCache.getCancellation task
            // Keep the original exception available to native awaits without another task wrapper.
            ExceptionCache.registerCancellation completion.Task error
            completion.TrySetCanceled(error.CancellationToken) |> ignore
        elif task.IsFaulted then
            completion.TrySetException(task.Exception.InnerExceptions) |> ignore
        else
            completion.TrySetResult(task.Result) |> ignore

    let private startQueued<'T> (start: unit -> Task<'T>) (completion: TaskCompletionSource<'T>) =
        try
            let task = start ()

            if task.IsCompleted then
                enqueueCompletion (fun () -> complete task completion)
            else
                task.ContinueWith(
                    Action<Task<'T>>(fun _ ->
                        enqueueCompletion (fun () -> complete task completion)),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default
                )
                |> ignore
        with error ->
            enqueueCompletion (fun () -> completion.TrySetException(error) |> ignore)

    let start<'T> (start: unit -> Task<'T>) =
        let current = state.Value

        if current.IsRunning then
            let completion = TaskCompletionSource<'T>()
            // The queue is drained after the parent has returned or suspended, so the child must
            // run in the execution context the parent had when it asked for the start.
            let context = ExecutionContext.Capture()

            current.Queue.Enqueue(fun () ->
                if isNull context then
                    startQueued start completion
                else
                    ExecutionContext.Run(context, (fun _ -> startQueued start completion), null))

            completion.Task
        else
            current.IsRunning <- true

            try
                start ()
            finally
                try
                    drain current
                finally
                    current.IsRunning <- false

    let startIsolated<'T> (startComputation: unit -> Task<'T>) =
        let previous = state.Value
        state.Value <- State()

        try
            startComputation ()
        finally
            state.Value <- previous

module internal Async2Results =
    let fromAwaitable (start: CancellationToken -> ValueTask<'T>) ct =
        // Keep raw factory invocation outside the async method: start failures are synchronous.
        let pending = start ct
        __runtimeAsyncReturnValueTask (
            try
                Async2Result.Completed(ExceptionCache.awaitAwaiter (pending.GetAwaiter()))
            with :? OperationCanceledException as error when ct.IsCancellationRequested ->
                Async2Result.Cancelled error)

    let toTask (pending: ValueTask<Async2Result<'T>>) =
        __runtimeAsyncReturnValueTask (
            let result =
                if pending.IsCompletedSuccessfully then pending.Result
                else ExceptionCache.awaitTask (pending.AsTask())
            match result with
            | Async2Result.Completed value -> value
            | Async2Result.Cancelled error -> ExceptionCache.throwException error)
        |> _.AsTask()

[<Sealed; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
type Async2<'T> private
    (start: CancellationToken -> ValueTask<Async2Result<'T>>,
     originalStart: (CancellationToken -> ValueTask<'T>) option) =

    new(start: CancellationToken -> ValueTask<'T>) =
        Async2(Async2Results.fromAwaitable start, Some start)

    member _.Start ct =
        match originalStart with
        | Some original -> original ct |> _.AsTask()
        | None -> start ct |> Async2Results.toTask

    /// Creates a native carrier for compiler-generated completion results.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    static member FromResult(start: CancellationToken -> ValueTask<Async2Result<'T>>) =
        Async2<'T>(start, None)

    /// Starts a native computation without translating cancellation into a canceled task.
    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member _.StartResultTrampolined ct =
        Async2StartTrampoline.start (fun () -> start ct |> _.AsTask())

    [<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
    member this.StartTrampolined ct =
        match originalStart with
        | Some original ->
            Async2StartTrampoline.start (fun () -> original ct |> _.AsTask())
        | None ->
            this.StartResultTrampolined ct |> ValueTask<_> |> Async2Results.toTask

    member internal this.StartInIsolatedTrampoline ct =
        Async2StartTrampoline.startIsolated (fun () -> this.Start ct)

    member internal _.StartResultInIsolatedTrampoline ct =
        Async2StartTrampoline.startIsolated (fun () -> start ct |> _.AsTask())

type Async2Code<'T> = CancellationToken -> Async2Result<'T>

module Async2Builder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current
        && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

    let inline check (cancellationToken: CancellationToken) =
        cancellationToken.ThrowIfCancellationRequested()

    let inline cancelled (ct: CancellationToken) =
        Async2Result.Cancelled(OperationCanceledException(ct))

open Async2Builder

type Async2Builder() =

    member inline _.Delay([<InlineIfLambda>] generator) : Async2Code<'T> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else generator () ct

    member inline _.Zero() : Async2Code<unit> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else Async2Result.Completed ()

    member inline _.Return(value: 'T) : Async2Code<'T> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else Async2Result.Completed value

    member inline _.Combine([<InlineIfLambda>] first, [<InlineIfLambda>] second) : Async2Code<'T> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else
                match first ct with
                | Async2Result.Completed _ -> second ct
                | Async2Result.Cancelled error -> Async2Result.Cancelled error

    member inline _.TryWith ([<InlineIfLambda>] body, [<InlineIfLambda>] handler) : Async2Code<'T> =
        fun ct ->
            let result =
                if ct.IsCancellationRequested then cancelled ct
                else
                    try
                        body ct
                    with error ->
                        if ct.IsCancellationRequested then
                            match error with
                            | :? OperationCanceledException as cancellation
                                when cancellation.CancellationToken = ct
                                     && cancellation.GetType() = typeof<OperationCanceledException> ->
                                Async2Result.Cancelled cancellation
                            | _ -> cancelled ct
                        else handler error ct
            match result with
            | Async2Result.Cancelled error
                when ct.IsCancellationRequested
                     && (error.CancellationToken <> ct
                         || error.GetType() <> typeof<OperationCanceledException>) ->
                cancelled ct
            | _ -> result

    member inline _.TryFinally ([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) : Async2Code<'T> =
        fun ct ->
            try
                if ct.IsCancellationRequested then cancelled ct
                else body ct
            finally
                compensation ()

    member inline _.Using(resource: 'T :> IDisposable | null, [<InlineIfLambda>] body) : Async2Code<'U> =
        fun ct ->
            try
                if ct.IsCancellationRequested then cancelled ct
                else body resource ct
            finally
                if not (isNull (box resource)) then resource.Dispose()

    member inline _.While(guard: unit -> bool, [<InlineIfLambda>] body) : Async2Code<unit> =
        fun ct ->
            let mutable result = Async2Result.Completed ()
            let mutable running = true
            while running && guard () do
                result <- if ct.IsCancellationRequested then cancelled ct else body ct
                match result with
                | Async2Result.Completed () -> ()
                | Async2Result.Cancelled _ -> running <- false
            if running && ct.IsCancellationRequested then cancelled ct else result

    member inline _.For(sequence: seq<'T>, [<InlineIfLambda>] body) : Async2Code<unit> =
        fun ct ->
            use enumerator = sequence.GetEnumerator()
            let mutable result = Async2Result.Completed ()
            let mutable running = true
            while running && enumerator.MoveNext() do
                result <- if ct.IsCancellationRequested then cancelled ct else body enumerator.Current ct
                match result with
                | Async2Result.Completed () -> ()
                | Async2Result.Cancelled _ -> running <- false
            if running && ct.IsCancellationRequested then cancelled ct else result

    member inline _.Bind([<InlineIfLambda>] awaited: Started<'T>,[<InlineIfLambda>] continuation) : Async2Code<'U> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else
                let result =
                    try Async2Result.Completed(awaited.Invoke())
                    with :? OperationCanceledException as error when ct.IsCancellationRequested ->
                        Async2Result.Cancelled error
                match result with
                | Async2Result.Completed value -> continuation value ct
                | Async2Result.Cancelled error -> Async2Result.Cancelled error

    member inline this.Bind([<InlineIfLambda>] cancellable: Cold<'T>,[<InlineIfLambda>] continuation) : Async2Code<'U> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else
                let result =
                    try Async2Result.Completed(cancellable.Invoke ct)
                    with :? OperationCanceledException as error when ct.IsCancellationRequested ->
                        Async2Result.Cancelled error
                match result with
                | Async2Result.Completed value -> continuation value ct
                | Async2Result.Cancelled error -> Async2Result.Cancelled error

    member inline _.Bind([<InlineIfLambda>] cancellable: CancellableResult<'T>, [<InlineIfLambda>] continuation) : Async2Code<'U> =
        fun ct ->
            if ct.IsCancellationRequested then cancelled ct
            else
                match cancellable.Invoke ct with
                | Async2Result.Completed value -> continuation value ct
                | Async2Result.Cancelled error -> Async2Result.Cancelled error

    member inline _.ReturnFrom([<InlineIfLambda>] awaited: Started<'T>) : Async2Code<'T> =
        fun ct ->
            try Async2Result.Completed(awaited.Invoke())
            with :? OperationCanceledException as error when ct.IsCancellationRequested ->
                Async2Result.Cancelled error

    member inline _.ReturnFrom([<InlineIfLambda>] cancellable: Cold<'T>) : Async2Code<'T> =
        fun ct ->
            try Async2Result.Completed(cancellable.Invoke ct)
            with :? OperationCanceledException as error when ct.IsCancellationRequested ->
                Async2Result.Cancelled error

    member inline _.ReturnFrom([<InlineIfLambda>] cancellable: CancellableResult<'T>) : Async2Code<'T> =
        fun ct -> cancellable.Invoke ct

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
                    |> _.GetAwaiter()
                    |> ExceptionCache.awaitAwaiter)
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

    member inline _.MergeSources([<InlineIfLambda>] left: CancellableResult<'A>, [<InlineIfLambda>] right: CancellableResult<'B>) =
        CancellableResult(fun ct ->
            let right = __runtimeAsyncReturnValueTask (right.Invoke ct)
            match left.Invoke ct with
            | Async2Result.Cancelled error -> Async2Result.Cancelled error
            | Async2Result.Completed left ->
                match ExceptionCache.awaitAwaiter (right.GetAwaiter()) with
                | Async2Result.Completed right -> Async2Result.Completed(struct (left, right))
                | Async2Result.Cancelled error -> Async2Result.Cancelled error)

    member inline _.MergeSources([<InlineIfLambda>] left: CancellableResult<'A>, [<InlineIfLambda>] right: Cold<'B>) =
        CancellableResult(fun ct ->
            let right = __runtimeAsyncReturnValueTask (right.Invoke ct)
            match left.Invoke ct with
            | Async2Result.Cancelled error -> Async2Result.Cancelled error
            | Async2Result.Completed left ->
                let right = ExceptionCache.awaitAwaiter (right.GetAwaiter())
                Async2Result.Completed(struct (left, right)))

    member inline _.MergeSources([<InlineIfLambda>] left: Cold<'A>, [<InlineIfLambda>] right: CancellableResult<'B>) =
        CancellableResult(fun ct ->
            let right = __runtimeAsyncReturnValueTask (right.Invoke ct)
            let left = left.Invoke ct
            match ExceptionCache.awaitAwaiter (right.GetAwaiter()) with
            | Async2Result.Completed right -> Async2Result.Completed(struct (left, right))
            | Async2Result.Cancelled error -> Async2Result.Cancelled error)

    member inline _.MergeSources([<InlineIfLambda>] left: CancellableResult<'A>, [<InlineIfLambda>] right: Started<'B>) =
        CancellableResult(fun ct ->
            match left.Invoke ct with
            | Async2Result.Completed left ->
                Async2Result.Completed(struct (left, right.Invoke()))
            | Async2Result.Cancelled error -> Async2Result.Cancelled error)

    member inline _.MergeSources([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: CancellableResult<'B>) =
        CancellableResult(fun ct ->
            match right.Invoke ct with
            | Async2Result.Cancelled error -> Async2Result.Cancelled error
            | Async2Result.Completed right ->
                Async2Result.Completed(struct (left.Invoke(), right)))

    member inline this.Source(computation: Async2<'T>) =
        CancellableResult(fun ct ->
            computation.StartResultTrampolined ct |> ExceptionCache.awaitResultTask ct)

    member inline _.Run([<InlineIfLambda>] code: Async2Code<'T>) : Async2<'T> =
        Async2.FromResult(fun ct ->
            __runtimeAsyncReturnValueTask (code ct))

[<AutoOpen>]
module Async2BuilderAsyncDisposableExtensions =
    type Async2Builder with
        member inline _.Using(resource: 'T :> IAsyncDisposable | null, [<InlineIfLambda>] body) : Async2Code<'U> =
            fun ct ->
                try
                    if ct.IsCancellationRequested then cancelled ct
                    else body resource ct
                finally
                    if not (isNull (box resource)) then
                        resource.DisposeAsync() |> AsyncHelpers.Await

        member inline this.For(sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body) : Async2Code<unit> =
            fun ct ->
                this.Using
                    (sequence.GetAsyncEnumerator ct,
                     fun enumerator ct ->
                         let mutable result = Async2Result.Completed ()
                         let mutable running = true
                         while running && (enumerator.MoveNextAsync() |> AsyncHelpers.Await) do
                             result <- if ct.IsCancellationRequested then cancelled ct else body enumerator.Current ct
                             match result with
                             | Async2Result.Completed () -> ()
                             | Async2Result.Cancelled _ -> running <- false
                         if running && ct.IsCancellationRequested then cancelled ct else result)
                    ct

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

        // Task and value-task Bind sources
        member inline _.Source(task: Task<'T>) =
            Started(fun () ->
                task |> ExceptionCache.awaitTask)
        member inline _.Source(task: Task) =
            Started(fun () ->
                task.GetAwaiter() |> ExceptionCache.awaitAwaiter)
        member inline _.Source(task: ValueTask<'T>) =
            Started(fun () ->
                task.GetAwaiter() |> ExceptionCache.awaitAwaiter)
        member inline _.Source(task: ValueTask) =
            Started(fun () ->
                task.GetAwaiter() |> ExceptionCache.awaitAwaiter)

         //Cold start sources
        member inline this.Source(computation: Async<'T>) =
            Cold(fun ct ->
                Async.StartImmediateAsTask(computation, ct) |> ExceptionCache.awaitTask)

[<AutoOpen>]
module Async2BuilderImpl =
    let async2 = Async2Builder()
