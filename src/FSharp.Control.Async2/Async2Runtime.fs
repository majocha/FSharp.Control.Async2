// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Runtime-async adaptation for Async2.

namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Core
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Core.CompilerServices.StateMachineHelpers
open Microsoft.FSharp.Collections

module internal Async2RuntimeHelpers =
    let defaultCancellationTokenSource = ref (new CancellationTokenSource())

    let getDefaultCancellationToken () =
        (!defaultCancellationTokenSource).Token

    let replaceDefaultCancellationToken () =
        let oldSource = !defaultCancellationTokenSource
        defaultCancellationTokenSource := new CancellationTokenSource()
        oldSource.Cancel()
        oldSource.Dispose()

    let getToken token =
        defaultArg token (getDefaultCancellationToken ())

    let startOnThreadPool (start: CancellationToken -> Task<'T>) cancellationToken taskCreationOptions =
        let task =
            (Task.Factory.StartNew(
                Func<Task<'T>>(fun () -> start cancellationToken),
                CancellationToken.None,
                taskCreationOptions,
                TaskScheduler.Default
            ))
                .Unwrap()

        let completion =
            TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)

        task.ContinueWith(
            Action<Task<'T>>(fun completed ->
                if completed.IsCanceled then
                    completion.TrySetCanceled() |> ignore
                elif completed.IsFaulted then
                    completion.TrySetException(completed.Exception) |> ignore
                else
                    completion.TrySetResult(completed.Result) |> ignore),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        )
        |> ignore

        completion.Task

    let awaitTaskWithCancellation (cancellationToken: CancellationToken) (task: Task<'T>) =
        __runtimeAsyncReturn (
            let cancellationTask =
                Task.Delay(Timeout.Infinite, cancellationToken)

            let winner =
                AsyncHelpers.Await(
                    Task.WhenAny(
                        [| task :> Task
                           cancellationTask :> Task |]
                    )
                )

            if obj.ReferenceEquals(winner, task :> Task) then
                AsyncHelpers.Await task
            else
                cancellationToken.ThrowIfCancellationRequested()
                Unchecked.defaultof<'T>
        )

    let awaitTaskUnitWithCancellation (cancellationToken: CancellationToken) (task: Task) =
        __runtimeAsyncReturn (
            let cancellationTask =
                Task.Delay(Timeout.Infinite, cancellationToken)

            let winner =
                AsyncHelpers.Await(
                    Task.WhenAny(
                        [| task
                           cancellationTask :> Task |]
                    )
                )

            if obj.ReferenceEquals(winner, task) then
                AsyncHelpers.Await task
            else
                cancellationToken.ThrowIfCancellationRequested()
        )

    let awaitValueTaskWithCancellation (cancellationToken: CancellationToken) (task: ValueTask<'T>) =
        __runtimeAsyncReturn (
            let cancellationTask =
                Task.Delay(Timeout.Infinite, cancellationToken)

            let winner =
                AsyncHelpers.Await(
                    Task.WhenAny(
                        [| task.AsTask() :> Task
                           cancellationTask :> Task |]
                    )
                )

            if obj.ReferenceEquals(winner, task.AsTask() :> Task) then
                AsyncHelpers.Await task
            else
                cancellationToken.ThrowIfCancellationRequested()
                Unchecked.defaultof<'T>
        )

    let awaitValueTaskUnitWithCancellation (cancellationToken: CancellationToken) (task: ValueTask) =
        __runtimeAsyncReturn (
            let task = task.AsTask()
            let cancellationTask =
                Task.Delay(Timeout.Infinite, cancellationToken)

            let winner =
                AsyncHelpers.Await(
                    Task.WhenAny(
                        [| task
                           cancellationTask :> Task |]
                    )
                )

            if obj.ReferenceEquals(winner, task) then
                AsyncHelpers.Await task
            else
                cancellationToken.ThrowIfCancellationRequested()
        )

    let continueWithResult (task: Task<'T>) (continuation: Task<'T> -> 'U) : Task<'U> =
        task.ContinueWith(
            Func<Task<'T>, 'U>(fun (completed: Task<'T>) ->
                continuation completed),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        )

    let cancelOnFault (cancellationToken: CancellationTokenSource) (task: Task<'T>) =
        task.ContinueWith(
            Action<Task<'T>>(fun completed ->
                if completed.IsFaulted then
                    try
                        cancellationToken.Cancel()
                    with _ ->
                        ()),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        )
        |> ignore

module TasklikeHelpers =
    type Awaiter<'Awaiter, 'TResult
        when 'Awaiter :> ICriticalNotifyCompletion
        and 'Awaiter: (member get_IsCompleted: unit -> bool)
        and 'Awaiter: (member GetResult: unit -> 'TResult)> = 'Awaiter

    type Awaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> = 'Awaitable

    module Awaiter =
        let inline getResult (awaiter: Awaiter<_, _>) = awaiter.GetResult()

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

type [<Sealed; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>] Async2<'T>
    (start: CancellationToken -> Task<'T>) =

    /// Starts this computation with the supplied cancellation token.
    member this.Start(cancellationToken: CancellationToken) =
        start cancellationToken

type RuntimeAsyncStarted<'T> = delegate of unit -> 'T
type RuntimeAsyncCancellable<'T> = delegate of CancellationToken -> 'T

module RuntimeAsyncBuilderHelpers =
    [<NoEagerConstraintApplication>]
    let inline startAwaitable (awaitable: TasklikeHelpers.Awaitable<_, _, _>) =
        let awaiter = TasklikeHelpers.Awaitable.getAwaiter awaitable

        RuntimeAsyncStarted(fun () ->
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
            TasklikeHelpers.Awaiter.getResult awaiter)

    [<NoEagerConstraintApplication>]
    let inline startCancellableAwaitable
        (cancellableAwaitable: CancellationToken -> TasklikeHelpers.Awaitable<_, _, _>)
        =
        RuntimeAsyncCancellable(fun ct ->
            let awaiter =
                cancellableAwaitable ct
                |> TasklikeHelpers.Awaitable.getAwaiter

            AsyncHelpers.UnsafeAwaitAwaiter awaiter
            TasklikeHelpers.Awaiter.getResult awaiter)

open RuntimeAsyncBuilderHelpers

type CancellableRuntimeAsyncBuilder() =
    member inline _.Delay
        ([<InlineIfLambda>] generator: unit -> CancellationToken -> 'T)
        : CancellationToken -> 'T =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            generator () ct

    member inline _.Zero() : CancellationToken -> unit =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            ()

    member inline _.Return(value: 'T) : CancellationToken -> 'T =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            value

    member inline _.Combine
        (
            [<InlineIfLambda>] first: CancellationToken -> 'A,
            [<InlineIfLambda>] second: CancellationToken -> 'T
        ) : CancellationToken -> 'T =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            first ct |> ignore
            second ct

    member inline _.TryWith
        (
            [<InlineIfLambda>] body: CancellationToken -> 'T,
            [<InlineIfLambda>] handler: exn -> CancellationToken -> 'T
        ) : CancellationToken -> 'T =
        fun ct ->
            ct.ThrowIfCancellationRequested()

            try
                body ct
            with
            | :? OperationCanceledException -> reraise ()
            | error -> handler error ct

    member inline _.TryFinally
        (
            [<InlineIfLambda>] body: CancellationToken -> 'T,
            [<InlineIfLambda>] compensation: unit -> unit
        ) : CancellationToken -> 'T =
        fun ct ->
            try
                ct.ThrowIfCancellationRequested()
                body ct
            finally
                compensation ()

    member inline _.Using
        (resource: 'T, [<InlineIfLambda>] body: 'T -> CancellationToken -> 'U)
        : CancellationToken -> 'U =
        fun ct ->
            try
                ct.ThrowIfCancellationRequested()
                body resource ct
            finally
                match box resource with
                | null -> ()
                | :? IAsyncDisposable as asyncDisposable ->
                    AsyncHelpers.Await(asyncDisposable.DisposeAsync())
                | :? IDisposable as disposable -> disposable.Dispose()
                | _ -> ()

    member inline _.While
        (guard: unit -> bool, [<InlineIfLambda>] body: CancellationToken -> unit)
        : CancellationToken -> unit =
        fun ct ->
            while guard () do
                body ct

            ct.ThrowIfCancellationRequested()

    member inline _.For
        (sequence: seq<'T>, [<InlineIfLambda>] body: 'T -> CancellationToken -> unit)
        : CancellationToken -> unit =
        fun ct ->
            use enumerator = sequence.GetEnumerator()

            while enumerator.MoveNext() do
                body enumerator.Current ct

            ct.ThrowIfCancellationRequested()

    member inline _.Bind
        (
            [<InlineIfLambda>] awaited: RuntimeAsyncStarted<'T>,
            [<InlineIfLambda>] continuation: 'T -> CancellationToken -> 'U
        ) : CancellationToken -> 'U =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            continuation (awaited.Invoke()) ct

    member inline _.Bind
        (
            [<InlineIfLambda>] cancellable: RuntimeAsyncCancellable<'T>,
            [<InlineIfLambda>] continuation: 'T -> CancellationToken -> 'U
        ) : CancellationToken -> 'U =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            continuation (cancellable.Invoke ct) ct

    member inline _.Bind
        (
            computation: Async2<'T>,
            [<InlineIfLambda>] continuation: 'T -> CancellationToken -> 'U
        ) : CancellationToken -> 'U =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            continuation (AsyncHelpers.Await(computation.Start ct)) ct

    member inline _.ReturnFrom(computation: Async2<'T>) : CancellationToken -> 'T =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            AsyncHelpers.Await(computation.Start ct)

    member inline _.MergeSources
        ([<InlineIfLambda>] left: RuntimeAsyncStarted<'A>, [<InlineIfLambda>] right: RuntimeAsyncStarted<'B>)
        =
        let left = left.Invoke()
        let right = right.Invoke()
        RuntimeAsyncStarted(fun () -> struct (left, right))

    member inline _.MergeSources
        (
            [<InlineIfLambda>] left: RuntimeAsyncCancellable<'A>,
            [<InlineIfLambda>] right: RuntimeAsyncCancellable<'B>
        ) =
        RuntimeAsyncCancellable(fun ct ->
            let rightTask = __runtimeAsyncReturnValueTask (right.Invoke ct)
            let leftValue = left.Invoke ct
            struct (leftValue, AsyncHelpers.Await rightTask))

    member inline _.MergeSources
        (
            [<InlineIfLambda>] left: RuntimeAsyncStarted<'A>,
            [<InlineIfLambda>] right: RuntimeAsyncCancellable<'B>
        ) =
        RuntimeAsyncCancellable(fun ct ->
            let rightValue = right.Invoke ct
            let leftValue = left.Invoke()
            struct (leftValue, rightValue))

    member inline _.MergeSources
        (
            [<InlineIfLambda>] left: RuntimeAsyncCancellable<'A>,
            [<InlineIfLambda>] right: RuntimeAsyncStarted<'B>
        ) =
        RuntimeAsyncCancellable(fun ct ->
            let leftValue = left.Invoke ct
            let rightValue = right.Invoke()
            struct (leftValue, rightValue))

[<AutoOpen>]
module CancellableRuntimeAsyncBuilderExtensions =
    type CancellableRuntimeAsyncBuilder with
        member inline this.For
            (sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body: 'T -> CancellationToken -> unit)
            : CancellationToken -> unit =
            fun ct ->
                this.Using(
                    sequence.GetAsyncEnumerator ct,
                    fun enumerator ct ->
                        while enumerator.MoveNextAsync() |> AsyncHelpers.Await do
                            body enumerator.Current ct
                ) ct

        member inline _.Source(value: 'T) = value

[<Sealed>]
type Async2Builder() =
    inherit CancellableRuntimeAsyncBuilder()

    member inline _.Run([<InlineIfLambda>] code) : Async2<'T> =
        Async2(fun ct -> __runtimeAsyncReturn (code ct))

[<AutoOpen>]
module Async2BuilderImpl =
    let async2 = Async2Builder()

[<Sealed; CompiledName("FSharpAsync2")>]
type Async2 =
    static member DefaultCancellationToken =
        Async2RuntimeHelpers.getDefaultCancellationToken ()

    static member CancelDefaultToken() =
        Async2RuntimeHelpers.replaceDefaultCancellationToken ()

    static member CancellationToken : Async2<CancellationToken> =
        Async2(fun ct -> Task.FromResult ct)

    static member RunSynchronously(computation: Async2<'T>, ?timeout: int, ?cancellationToken: CancellationToken) =
        let timeout = defaultArg timeout Timeout.Infinite
        let task = computation.Start(Async2RuntimeHelpers.getToken cancellationToken)

        if timeout <> Timeout.Infinite then
            try
                if not (task.Wait timeout) then
                    raise (TimeoutException())
            with :? AggregateException ->
                ()

        task.GetAwaiter().GetResult()

    static member RunSynchronouslyImmediate
        (computation: Async2<'T>, ?cancellationToken: CancellationToken)
        =
        computation.Start(Async2RuntimeHelpers.getToken cancellationToken)
            .GetAwaiter()
            .GetResult()

    static member Start(computation: Async2<unit>, ?cancellationToken: CancellationToken) =
        let token = Async2RuntimeHelpers.getToken cancellationToken
        Async2RuntimeHelpers.startOnThreadPool (fun token -> computation.Start token) token TaskCreationOptions.DenyChildAttach
        |> ignore

    static member StartAsTask
        (
            computation: Async2<'T>,
            ?taskCreationOptions: TaskCreationOptions,
            ?cancellationToken: CancellationToken
        )
        =
        let token = Async2RuntimeHelpers.getToken cancellationToken
        let options = defaultArg taskCreationOptions TaskCreationOptions.DenyChildAttach
        Async2RuntimeHelpers.startOnThreadPool (fun token -> computation.Start token) token options

    static member StartChildAsTask
        (computation: Async2<'T>, ?taskCreationOptions: TaskCreationOptions)
        : Async2<Task<'T>> =
        Async2(fun ct ->
            let child =
                Async2RuntimeHelpers.startOnThreadPool
                    (fun token -> computation.Start token)
                    ct
                    (defaultArg taskCreationOptions TaskCreationOptions.DenyChildAttach)

            Task.FromResult child)

    static member Catch(computation: Async2<'T>) : Async2<Choice<'T, exn>> =
        async2 {
            try
                let! result = computation
                return Choice1Of2 result
            with error ->
                return Choice2Of2 error
        }

    static member TryCancelled
        (computation: Async2<'T>, compensation: OperationCanceledException -> unit)
        : Async2<'T> =
        Async2(fun ct ->
            let task = computation.Start ct

            Async2RuntimeHelpers.continueWithResult task (fun completed ->
                if completed.IsCanceled then
                    compensation (OperationCanceledException ct)

                completed.GetAwaiter().GetResult()))

    static member OnCancel(interruption: unit -> unit) : Async2<IDisposable> =
        async2 {
            let! ct = Async2.CancellationToken
            return ct.Register(Action interruption) :> IDisposable
        }

    static member StartChild
        (computation: Async2<'T>, ?millisecondsTimeout: int)
        : Async2<Async2<'T>> =
        Async2(fun ct ->
            ct.ThrowIfCancellationRequested()

            let childCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(ct)

            let childTask =
                (Task.Factory.StartNew(
                    Func<Task<'T>>(fun () -> computation.Start childCancellation.Token),
                    CancellationToken.None,
                    TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default
                ))
                    .Unwrap()
            let child = Async2(fun _ -> childTask)

            match millisecondsTimeout with
            | None ->
                childCancellation.Dispose()
                Task.FromResult child
            | Some timeout ->
                __runtimeAsyncReturn (
                    use timeoutTask = Task.Delay(timeout, ct)

                    let winner =
                        AsyncHelpers.Await(
                            Task.WhenAny(
                                [| childTask :> Task
                                   timeoutTask :> Task |]
                            )
                        )

                    if obj.ReferenceEquals(winner, timeoutTask :> Task) then
                        childCancellation.Cancel()
                        raise (TimeoutException())

                    child))

    static member Parallel(computations: seq<Async2<'T>>) : Async2<'T array> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                ct.ThrowIfCancellationRequested()
                let linked = CancellationTokenSource.CreateLinkedTokenSource ct

                try
                    let tasks =
                        computations
                        |> Seq.map (fun computation -> computation.Start linked.Token)
                        |> Seq.toArray

                    tasks |> Array.iter (Async2RuntimeHelpers.cancelOnFault linked)
                    AsyncHelpers.Await<'T array>(Task.WhenAll tasks)
                finally
                    linked.Dispose()))

    static member Parallel
        (computations: seq<Async2<'T>>, ?maxDegreeOfParallelism: int)
        : Async2<'T array> =
        match maxDegreeOfParallelism with
        | None -> Async2.Parallel computations
        | Some maxDegreeOfParallelism ->
            if maxDegreeOfParallelism <= 0 then
                invalidArg "maxDegreeOfParallelism" "maxDegreeOfParallelism must be positive"

            Async2(fun ct ->
                __runtimeAsyncReturn (
                    use gate = new SemaphoreSlim(maxDegreeOfParallelism)

                    let run (computation: Async2<'T>) : Task<'T> =
                        Task.Run(Func<'T>(fun () ->
                            gate.Wait ct

                            try
                                (computation.Start ct).GetAwaiter().GetResult()
                            finally
                                gate.Release() |> ignore), CancellationToken.None)

                    computations
                    |> Seq.map run
                    |> Seq.toArray
                    |> fun tasks -> Task.WhenAll(tasks)
                    |> AsyncHelpers.Await))

    static member Sequential(computations: seq<Async2<'T>>) : Async2<'T array> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                let results = ResizeArray<'T>()

                for computation in computations do
                    results.Add(AsyncHelpers.Await(computation.Start ct))

                results.ToArray()))

    static member Choice(computations: seq<Async2<'T option>>) : Async2<'T option> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                let tasks = computations |> Seq.map (fun computation -> computation.Start ct) |> Seq.toArray
                let remaining = ResizeArray<Task<'T option>>(tasks)
                let mutable result = None
                let mutable found = false

                while not found && remaining.Count > 0 do
                    let completed = AsyncHelpers.Await(Task.WhenAny(remaining))
                    remaining.Remove completed |> ignore

                    match AsyncHelpers.Await completed with
                    | Some value ->
                        result <- Some value
                        found <- true
                    | None -> ()

                result))

    static member SwitchToNewThread() : Async2<unit> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                Task.Factory.StartNew(
                    Action(fun () -> ()),
                    ct,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                |> AsyncHelpers.Await))

    static member SwitchToThreadPool() : Async2<unit> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                Task.Run(Action(fun () -> ()), ct)
                |> AsyncHelpers.Await))

    static member SwitchToContext(syncContext: SynchronizationContext | null) : Async2<unit> =
        Async2(fun ct ->
            let completion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            ct.ThrowIfCancellationRequested()

            if isNull syncContext then
                Task.Run(Action(fun () -> completion.SetResult()), ct)
                |> ignore
            else
                syncContext.Post(
                    SendOrPostCallback(fun _ ->
                        try
                            completion.SetResult()
                        with _ ->
                            ()),
                    null
                )

            completion.Task)

    static member FromContinuations
        (callback: ('T -> unit) * (exn -> unit) * (OperationCanceledException -> unit) -> unit)
        : Async2<'T> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                let completion = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
                let mutable completed = 0

                let once action value =
                    if Interlocked.CompareExchange(&completed, 1, 0) = 0 then
                        action value |> ignore

                use registration =
                    ct.Register(
                        Action(fun () ->
                            once
                                (fun (error: OperationCanceledException) ->
                                    completion.TrySetCanceled(error.CancellationToken) |> ignore)
                                (OperationCanceledException ct))
                    )

                callback(
                    (fun value ->
                        once
                            (fun (value: 'T) -> completion.TrySetResult(value) |> ignore)
                            value),
                    (fun error ->
                        once
                            (fun (error: exn) -> completion.TrySetException(error) |> ignore)
                            error),
                    (fun error ->
                        once
                            (fun (error: OperationCanceledException) ->
                                completion.TrySetCanceled(error.CancellationToken) |> ignore)
                            error)
                )

                AsyncHelpers.Await completion.Task))

    static member AwaitTask(task: Task<'T>) : Async2<'T> =
        Async2(fun ct -> Async2RuntimeHelpers.awaitTaskWithCancellation ct task)

    static member AwaitTask(task: Task) : Async2<unit> =
        Async2(fun ct -> Async2RuntimeHelpers.awaitTaskUnitWithCancellation ct task)

    static member Await(task: Task<'T>) : Async2<'T> =
        Async2.AwaitTask task

    static member Await(task: Task) : Async2<unit> =
        Async2.AwaitTask task

    static member Await(task: ValueTask<'T>) : Async2<'T> =
        Async2(fun ct -> Async2RuntimeHelpers.awaitValueTaskWithCancellation ct task)

    static member Await(task: ValueTask) : Async2<unit> =
        Async2(fun ct -> Async2RuntimeHelpers.awaitValueTaskUnitWithCancellation ct task)

    static member StartTaskImmediate(createTask: CancellationToken -> Task<'T>) : Async2<'T> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                createTask ct
                |> AsyncHelpers.Await))

    static member StartTaskImmediate(createTask: CancellationToken -> Task) : Async2<unit> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                createTask ct
                |> AsyncHelpers.Await))

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask<'T>) : Async2<'T> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                createTask ct
                |> AsyncHelpers.Await))

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask) : Async2<unit> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                createTask ct
                |> AsyncHelpers.Await))

    static member Sleep(millisecondsDueTime: int) : Async2<unit> =
        Async2(fun ct ->
            Async2RuntimeHelpers.awaitTaskUnitWithCancellation ct (Task.Delay(millisecondsDueTime, ct)))

    static member Sleep(dueTime: TimeSpan) : Async2<unit> =
        Async2(fun ct ->
            Async2RuntimeHelpers.awaitTaskUnitWithCancellation ct (Task.Delay(dueTime, ct)))

    static member AwaitWaitHandle(waitHandle: WaitHandle, ?millisecondsTimeout: int) : Async2<bool> =
        Async2(fun ct ->
            let timeout = defaultArg millisecondsTimeout Timeout.Infinite
            let completion = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
            let mutable registration = Unchecked.defaultof<RegisteredWaitHandle>

            registration <-
                ThreadPool.RegisterWaitForSingleObject(
                    waitHandle,
                    WaitOrTimerCallback(fun _ timedOut -> completion.TrySetResult(not timedOut) |> ignore),
                    null,
                    timeout,
                    true
                )

            use cancellationRegistration =
                ct.Register(
                    Action(fun () ->
                        completion.TrySetCanceled(ct) |> ignore
                        registration.Unregister(null) |> ignore)
                )

            Async2RuntimeHelpers.awaitTaskWithCancellation ct completion.Task)

    static member AwaitIAsyncResult(iar: IAsyncResult, ?millisecondsTimeout: int) : Async2<bool> =
        Async2.AwaitWaitHandle(iar.AsyncWaitHandle, ?millisecondsTimeout = millisecondsTimeout)

    static member FromBeginEnd
        (
            beginAction: AsyncCallback * obj -> IAsyncResult,
            endAction: IAsyncResult -> 'T,
            ?cancelAction: unit -> unit
        )
        : Async2<'T> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                let completion = TaskCompletionSource<IAsyncResult>(TaskCreationOptions.RunContinuationsAsynchronously)

                let callback =
                    AsyncCallback(fun result -> completion.TrySetResult(result) |> ignore)

                let result = beginAction(callback, null)

                if result.CompletedSynchronously then
                    endAction result
                else
                    use registration =
                        ct.Register(
                            Action(fun () ->
                                match cancelAction with
                                | Some cancel -> cancel ()
                                | None -> completion.TrySetCanceled(ct) |> ignore)
                        )

                    endAction (AsyncHelpers.Await completion.Task)))

    static member AsBeginEnd(computation: 'Arg -> Async2<'T>) =
        let beginAction (arg: 'Arg, callback: AsyncCallback, state: obj) =
            let task = Async2.StartAsTask(computation arg)
            task.ContinueWith(
                Action<Task<'T>>(fun _ -> callback.Invoke task),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            )
            |> ignore
            task :> IAsyncResult

        let endAction (result: IAsyncResult) =
            (result :?> Task<'T>).GetAwaiter().GetResult()

        let cancelAction (result: IAsyncResult) =
            (result :?> Task<'T>).Dispose()

        beginAction, endAction, cancelAction

    static member Ignore(computation: Async2<'T>) : Async2<unit> =
        async2 {
            let! _ = computation
            return ()
        }

    static member StartWithContinuations
        (
            computation: Async2<'T>,
            continuation: 'T -> unit,
            exceptionContinuation: exn -> unit,
            cancellationContinuation: OperationCanceledException -> unit,
            ?cancellationToken: CancellationToken
        ) =
        let task = computation.Start(Async2RuntimeHelpers.getToken cancellationToken)

        task.ContinueWith(
            Action<Task<'T>>(fun completed ->
                if completed.IsCanceled then
                    cancellationContinuation (OperationCanceledException(Async2RuntimeHelpers.getToken cancellationToken))
                elif completed.IsFaulted then
                    exceptionContinuation completed.Exception.InnerException
                else
                    continuation completed.Result),
            TaskScheduler.Default
        )
        |> ignore

    static member StartImmediate(computation: Async2<unit>, ?cancellationToken: CancellationToken) =
        Async2.StartWithContinuations(
            computation,
            ignore,
            (fun error -> raise error),
            ignore,
            ?cancellationToken = cancellationToken
        )

    static member StartImmediateAsTask(computation: Async2<'T>, ?cancellationToken: CancellationToken) =
        computation.Start(Async2RuntimeHelpers.getToken cancellationToken)

[<AutoOpen>]
module Async2TaskLikeExtensions =
    type Async2 with
        [<NoEagerConstraintApplication>]
        static member inline Await
            < ^TaskLike, ^Awaiter, 'T
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (task: ^TaskLike)
            : Async2<'T> =
            Async2(fun _ ->
                __runtimeAsyncReturn (
                    let awaiter = task.GetAwaiter()
                    AsyncHelpers.UnsafeAwaitAwaiter awaiter
                    awaiter.GetResult()))

        [<NoEagerConstraintApplication>]
        static member inline StartTaskImmediate
            < ^TaskLike, ^Awaiter, 'T
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (createTask: CancellationToken -> ^TaskLike)
            : Async2<'T> =
            Async2(fun ct ->
                __runtimeAsyncReturn (
                    let awaiter = (createTask ct).GetAwaiter()
                    AsyncHelpers.UnsafeAwaitAwaiter awaiter
                    awaiter.GetResult()))

[<AutoOpen>]
module CommonExtensions =
    type Stream with
        member stream.AsyncRead(buffer: byte array, ?offset: int, ?count: int) =
            let offset = defaultArg offset 0
            let count = defaultArg count (buffer.Length - offset)
            Async2.StartTaskImmediate(fun ct -> stream.ReadAsync(buffer, offset, count, ct))

        member stream.AsyncRead(count: int) =
            let taskFactory (ct: CancellationToken) : Task<byte array> =
                let buffer = Array.zeroCreate<byte> count
                stream.ReadAsync(buffer, 0, count, ct)
                    .ContinueWith(Func<Task<int>, byte array>(fun _ -> buffer))

            Async2.StartTaskImmediate(taskFactory)

        member stream.AsyncWrite(buffer: byte array, ?offset: int, ?count: int) =
            let offset = defaultArg offset 0
            let count = defaultArg count (buffer.Length - offset)
            Async2.StartTaskImmediate(fun ct -> stream.WriteAsync(buffer, offset, count, ct))

    type IObservable<'T> with
        member observable.Add(callback: 'T -> unit) =
            observable.Subscribe(
                { new IObserver<'T> with
                    member _.OnNext(value) = callback value
                    member _.OnError(_) = ()
                    member _.OnCompleted() = () }
            )
            |> ignore

        member observable.Subscribe(callback: 'T -> unit) =
            observable.Subscribe(
                { new IObserver<'T> with
                    member _.OnNext(value) = callback value
                    member _.OnError(_) = ()
                    member _.OnCompleted() = () }
            )
            :> IDisposable

    type WebRequest with
        member request.AsyncGetResponse() =
            Async2.StartTaskImmediate(fun _ -> request.GetResponseAsync())

    type WebClient with
        member client.AsyncDownloadString(address: Uri) =
            Async2.StartTaskImmediate(fun _ -> client.DownloadStringTaskAsync(address))

        member client.AsyncDownloadData(address: Uri) =
            Async2.StartTaskImmediate(fun _ -> client.DownloadDataTaskAsync(address))

        member client.AsyncDownloadFile(address: Uri, fileName: string) =
            Async2.StartTaskImmediate(fun _ -> client.DownloadFileTaskAsync(address, fileName))

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Async2 =
    let inline result (value: 'T) : Async2<'T> =
        Async2(fun ct ->
            ct.ThrowIfCancellationRequested()
            Task.FromResult value)

    let inline map (mapping: 'T -> 'U) (computation: Async2<'T>) : Async2<'U> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                mapping (AsyncHelpers.Await(computation.Start ct))))

    let inline bind (binder: 'T -> Async2<'U>) (computation: Async2<'T>) : Async2<'U> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                AsyncHelpers.Await((binder (AsyncHelpers.Await(computation.Start ct))).Start ct)))

    let inline ignore<'T> (computation: Async2<'T>) : Async2<unit> =
        Async2.Ignore computation

    let catchWith (handler: exn -> 'T) (computation: Async2<'T>) : Async2<'T> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                try
                    AsyncHelpers.Await(computation.Start ct)
                with
                | :? OperationCanceledException -> reraise ()
                | error ->
                    handler error))

    let catch (computation: Async2<'T>) : Async2<Result<'T, exn>> =
        Async2(fun ct ->
            __runtimeAsyncReturn (
                try
                    Ok (AsyncHelpers.Await(computation.Start ct))
                with
                | :? OperationCanceledException -> reraise ()
                | error ->
                    Error error))

    let empty : Async2<unit> =
        async2 { return () }

    let sequentialDo (computations: seq<Async2<unit>>) : Async2<unit> =
        async2 {
            for computation in computations do
                do! computation
        }

    let parallelLimit
        (maxDegreeOfParallelism: int)
        (computations: seq<Async2<'T>>)
        : Async2<'T array> =
        Async2.Parallel(computations, maxDegreeOfParallelism = maxDegreeOfParallelism)

    let parallelDoLimit maxDegreeOfParallelism computations =
        sequentialDo (computations |> Seq.map (Async2.Ignore))
