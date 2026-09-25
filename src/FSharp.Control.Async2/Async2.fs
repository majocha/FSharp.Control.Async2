
// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Runtime-async adaptation for Async2.

namespace Microsoft.FSharp.Control

open System
open System.IO
open System.Net
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources

open Microsoft.FSharp.Core
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Core.CompilerServices.StateMachineHelpers

module internal Async2RuntimeHelpers =
    type ValueTaskCompletionSource<'T>() as this =
        let source = ManualResetValueTaskSourceCore<'T>(RunContinuationsAsynchronously = true)
        let mutable completed = 0
        let mutable canceled = 0
        let mutable cancellationRegistration = Unchecked.defaultof<CancellationTokenRegistration>

        interface IValueTaskSource<'T> with
            member _.GetResult token = source.GetResult token
            member _.GetStatus token =
                let status = source.GetStatus token
                if status = ValueTaskSourceStatus.Faulted && Volatile.Read(&canceled) <> 0 then
                    ValueTaskSourceStatus.Canceled
                else
                    status
            member _.OnCompleted(continuation, state, token, flags) = source.OnCompleted(continuation, state, token, flags)

        member _.TrySetResult(value: 'T) =
            if Interlocked.CompareExchange(&completed, 1, 0) = 0 then
                source.SetResult value

        member _.TrySetException(ex: exn) =
            if Interlocked.CompareExchange(&completed, 1, 0) = 0 then
                if ex :? OperationCanceledException then
                    Volatile.Write(&canceled, 1)
                source.SetException ex

        member _.Await(ct: CancellationToken) =
            cancellationRegistration <-
                ct.Register(
                    Action(fun () ->
                        this.TrySetException(TaskCanceledException("The task was canceled.", null, ct)))
                )
            ValueTask<'T>(this, source.Version)

        member _.Dispose() = cancellationRegistration.Dispose()

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

    let startOnThreadPool cancellationToken (computation: Async2<_>) =
        Task.Run<'T>(fun () -> computation.Start cancellationToken)

    let startImmediate cancellationToken (computation: Async2<_>) =
        computation.StartInIsolatedTrampoline cancellationToken

    let cancellationTokenAsync = Async2(fun ct -> ValueTask<CancellationToken>(ct))

open Async2RuntimeHelpers
open System.Runtime.ExceptionServices

[<Sealed; CompiledName("FSharpAsync2")>]
type Async2 =

    static member DefaultCancellationToken =
        getDefaultCancellationToken ()

    static member CancelDefaultToken() =
        replaceDefaultCancellationToken ()

    static member CancellationToken = cancellationTokenAsync

    static member RunSynchronously(computation: Async2<'T>, ?timeout: int, ?cancellationToken: CancellationToken) =
        let timeout =
            match cancellationToken with
            | Some token when token.CanBeCanceled -> Timeout.Infinite
            | _ -> defaultArg timeout Timeout.Infinite
        let cancellationToken = getToken cancellationToken

        let start ct =
            if
                timeout = Timeout.Infinite
                && Thread.CurrentThread.IsThreadPoolThread
                && Async2Builder.isAlreadyBackground ()
            then
                computation.StartInIsolatedTrampoline ct
            else
                computation |> startOnThreadPool ct

        match timeout with
        | Timeout.Infinite ->
            cancellationToken |> start |> _.GetAwaiter().GetResult()
        | timeout ->
            use cts = CancellationTokenSource.CreateLinkedTokenSource cancellationToken
            let task = start cts.Token
            let completed =
                try
                    task.Wait timeout
                with :? AggregateException ->
                    true

            if completed then
                task.GetAwaiter().GetResult()
            else
                cts.Cancel()
                // A timed-out computation must finish unwinding before its linked token is disposed.
                try
                    task.Wait()
                with :? AggregateException -> ()
                raise (TimeoutException())


    static member RunSynchronouslyImmediate
        (computation: Async2<'T>, ?cancellationToken: CancellationToken)
        =
        computation.StartInIsolatedTrampoline(getToken cancellationToken)
            .GetAwaiter()
            .GetResult()

    static member Start(computation: Async2<unit>, ?cancellationToken: CancellationToken) =
        let ct = getToken cancellationToken
        computation |> startOnThreadPool ct
        |> ignore

    static member StartAsTask
        (
            computation: Async2<'T>,
            ?taskCreationOptions: TaskCreationOptions,
            ?cancellationToken: CancellationToken
        )
        =
        ignore taskCreationOptions
        let ct = getToken cancellationToken
        //computation |> startImmediate ct
        computation |> startOnThreadPool ct

    static member StartChildAsTask(computation, ?taskCreationOptions) =
        async2 {
            let! ct = cancellationTokenAsync
            return computation |> startImmediate ct
        }

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
        async2 {
            let! ct = Async2.CancellationToken
            try
                return! computation
            with
            | :? OperationCanceledException as ex when ex.CancellationToken = ct ->
                compensation ex
                return Unchecked.defaultof<'T>
        }

    static member OnCancel(interruption: unit -> unit) : Async2<IDisposable> =
        async2 {
            let! ct = Async2.CancellationToken
            return ct.Register(Action interruption) :> IDisposable
        }

    static member StartChild
        (computation: Async2<'T>, ?millisecondsTimeout: int)
        : Async2<Async2<'T>> =
            async2 {
                let! ct = Async2.CancellationToken
                return async2 {
                    return! computation |> startOnThreadPool ct
                }
            }

    static member Parallel
        (computations: seq<Async2<'T>>, ?maxDegreeOfParallelism: int)
        : Async2<'T array> =
        let computations = computations |> Seq.toArray
        let maxDegreeOfParallelism = defaultArg maxDegreeOfParallelism (max 1 computations.Length)

        if maxDegreeOfParallelism < 1 then
            invalidArg "maxDegreeOfParallelism" (sprintf "maxDegreeOfParallelism must be positive, was %d" maxDegreeOfParallelism)

        async2 {
            let! ct = Async2.CancellationToken
            use cts = CancellationTokenSource.CreateLinkedTokenSource ct
            let innerToken = cts.Token
            let mutable firstFailure : ExceptionDispatchInfo option = None
            let mutable index = -1
            let workerCount = min computations.Length maxDegreeOfParallelism
            let results = Array.zeroCreate<'T> computations.Length
            let workers =
                Array.init workerCount (fun _ ->
                    let worker = async2 {
                        try 
                            while index < computations.Length && not innerToken.IsCancellationRequested do
                                let currentIndex = Interlocked.Increment(&index)
                                if currentIndex < computations.Length then
                                    let computation = computations[currentIndex]
                                    let! result = computation
                                    results[currentIndex] <- result
                        with
                        | exn when not innerToken.IsCancellationRequested ->
                            let failure = ExceptionDispatchInfo.Capture exn
                            if Interlocked.CompareExchange(&firstFailure, Some failure, None).IsNone then
                                cts.Cancel()
                        | _ -> ()
                    }
                    Task.Run<unit>(fun () -> worker.Start innerToken))

            try
                let! completed = Task.WhenAll workers
                ignore completed
            with _ -> ()
            firstFailure |> Option.iter _.Throw()
            return results
        }

    static member Sequential(computations: seq<Async2<'T>>) : Async2<'T array> =
        async2 {
            let results = ResizeArray()

            for c in computations do
                let! r = c
                results.Add r

            return results.ToArray()
        }

    static member Choice(computations: seq<Async2<'T option>>) : Async2<'T option> =
        let computations = computations |> Seq.toArray
        async2 {
            let! ct = Async2.CancellationToken
            use cts = CancellationTokenSource.CreateLinkedTokenSource ct
            let mutable result = None
            let mutable found = false

            let worker computation =
                async2 {
                    if not found then
                        try
                            match! computation with
                            | Some value ->
                                result <- Some value
                                found <- true
                                cts.Cancel()
                            | None -> ()
                        with _ -> ()
                }
                |> startOnThreadPool cts.Token

            let workers = computations |> Array.map worker

            try
                let! rejoin = Task.WhenAll(workers)
                ignore rejoin
            with _ -> ()
            
            return result
        }

    static member SwitchToNewThread() : Async2<unit> =
        async2 {
            let! ct = Async2.CancellationToken
            do!
                Task.Factory.StartNew(
                    Action(fun () -> ()),
                    ct,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
        }

    static member SwitchToThreadPool() : Async2<unit> =
        async2 {
            let! ct = Async2.CancellationToken
            do! Task.Run(Action(fun () -> ()), ct)
        }

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

            completion.Task |> ValueTask.ofTask)

    static member FromContinuations
        (callback: ('T -> unit) * (exn -> unit) * (OperationCanceledException -> unit) -> unit)
        : Async2<'T> =
        async2 {
            let! ct = Async2.CancellationToken
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

            return! completion.Task
        }

    static member AwaitTask(task: Task<'T>) : Async2<'T> =
        Async2(fun _ -> ValueTask<'T>(task))

    static member AwaitTask(task: Task) : Async2<unit> =
        async2 { return! task }

    static member Await(task: Task<'T>) : Async2<'T> =
        Async2(fun _ -> ValueTask<'T>(task))

    static member Await(task: Task) : Async2<unit> =
        async2 { return! task }

    static member Await(task: ValueTask<'T>) : Async2<'T> =
        Async2(fun _ -> task)

    static member Await(task: ValueTask) : Async2<unit> =
        async2 { return! task }

    static member StartTaskImmediate(createTask: CancellationToken -> Task<'T>) : Async2<'T> =
        async2 { let! ct = Async2.CancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> Task) : Async2<unit> =
        async2 { let! ct = Async2.CancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask<'T>) : Async2<'T> =
        async2 { let! ct = Async2.CancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask) : Async2<unit> =
        async2 { let! ct = Async2.CancellationToken in return! createTask ct }

    static member Sleep(millisecondsDueTime: int) : Async2<unit> =
        async2 {
            let! ct = Async2.CancellationToken
            return! Task.Delay(millisecondsDueTime, ct)
        }

    static member Sleep(dueTime: TimeSpan) : Async2<unit> =
        async2 {
            let! ct = Async2.CancellationToken
            return! Task.Delay(dueTime, ct)
        }

    static member AwaitWaitHandle(waitHandle: WaitHandle, ?millisecondsTimeout: int) : Async2<bool> =
        async2 {
            let! ct = Async2.CancellationToken

            let tcs = ValueTaskCompletionSource<bool>()

            let callback =
                WaitOrTimerCallback(fun _ timedOut -> tcs.TrySetResult(not timedOut))

            let handle =
                ThreadPool.RegisterWaitForSingleObject(waitHandle, callback, null, defaultArg millisecondsTimeout Timeout.Infinite, true)
            try
                return! tcs.Await ct
            finally
                handle.Unregister null |> ignore
                tcs.Dispose()
        }


    static member AwaitIAsyncResult(iar: IAsyncResult, ?millisecondsTimeout: int) : Async2<bool> =
        Async2.AwaitWaitHandle(iar.AsyncWaitHandle, ?millisecondsTimeout = millisecondsTimeout)

    static member FromBeginEnd
        (
            beginAction: AsyncCallback * obj -> IAsyncResult,
            endAction: IAsyncResult -> 'T,
            ?cancelAction: unit -> unit
        )
        : Async2<'T> = failwith "not implemented"

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
        let cancellationToken = getToken cancellationToken
        let task = computation.StartInIsolatedTrampoline cancellationToken

        let invoke () =
            let result =
                try
                    Choice1Of2(task.GetAwaiter().GetResult())
                with error ->
                    Choice2Of2 error

            match result with
            | Choice1Of2 value ->
                continuation value
            | Choice2Of2 (:? OperationCanceledException as error) ->
                cancellationContinuation error
            | Choice2Of2 error ->
                exceptionContinuation error

        if task.IsCompleted then
            invoke ()
        else
            task.ContinueWith(
                Action<Task<'T>>(fun _ -> invoke ()),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            )
            |> ignore

    static member StartImmediate(computation: Async2<unit>, ?cancellationToken: CancellationToken) =
        let ct = getToken cancellationToken
        computation |> startImmediate ct |> ignore

    static member StartImmediateAsTask(computation: Async2<'T>, ?cancellationToken: CancellationToken) =
        let ct = getToken cancellationToken
        computation |> startImmediate ct

[<AutoOpen>]
module Async2TaskLikeExtensions =
    type Async2 with
        [<NoEagerConstraintApplication>]
        static member inline Await< ^TaskLike, ^Awaiter, 'T
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (task: ^TaskLike)
            : Async2<'T> = async2 { return! task }

        [<NoEagerConstraintApplication>]
        static member inline StartTaskImmediate< ^TaskLike, ^Awaiter, 'T
            when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
            and ^Awaiter :> ICriticalNotifyCompletion
            and ^Awaiter: (member get_IsCompleted: unit -> bool)
            and ^Awaiter: (member GetResult: unit -> 'T)>
            (createTask: CancellationToken -> ^TaskLike)
            : Async2<'T> = async2 {let! ct = Async2.CancellationToken in return! createTask ct}

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
    let inline result (value: 'T) : Async2<'T> = async2 { return value }

    let inline map (mapping: 'T -> 'U) (computation: Async2<'T>) : Async2<'U> =
        async2 {
            let! result = computation
            return mapping result
        }

    let inline bind (binder: 'T -> Async2<'U>) (computation: Async2<'T>) : Async2<'U> =
        async2 {
            let! result = computation
            return! binder result
        }

    let inline ignore<'T> (computation: Async2<'T>) : Async2<unit> =
        Async2.Ignore computation

    let catchWith (handler: exn -> 'T) (computation: Async2<'T>) : Async2<'T> =
        async2 {
            try 
                let! result = computation
                return result
            with error ->
                return handler error
        }

    let catch (computation: Async2<'T>) : Async2<Result<'T, exn>> =
        async2 {
            try
                let! result = computation
                return Ok result
            with
            | error when not (error :? OperationCanceledException) ->
                return Error error
        }

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
