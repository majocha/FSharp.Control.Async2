
// Copyright (c) Microsoft Corporation. All Rights Reserved.
// Runtime-async adaptation for Async2.

namespace Microsoft.FSharp.Control

open System
open System.IO
open System.Net
open System.Reflection
open System.Threading
open System.Threading.Tasks

open Microsoft.FSharp.Core
open Microsoft.FSharp.Core.CompilerServices

module internal Async2RuntimeHelpers =
    type TaskCompletionSourceWithCancellation<'T>(cancellationToken: CancellationToken) =
        let completion = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
        let cancellationRegistration =
            cancellationToken.Register(
                fun () ->
                    completion.TrySetCanceled(cancellationToken) |> ignore
            )

        member _.TrySetResult(value: 'T) = completion.TrySetResult(value)
        member _.Task = completion.Task

        member _.Dispose() = cancellationRegistration.Dispose()

    let mutable defaultCancellationTokenSource = new CancellationTokenSource()

    let getDefaultCancellationToken () =
        defaultCancellationTokenSource.Token

    let replaceDefaultCancellationToken () =
        let oldSource = defaultCancellationTokenSource
        defaultCancellationTokenSource <- new CancellationTokenSource()
        oldSource.Cancel()
        oldSource.Dispose()

    let getToken token =
        defaultArg token (getDefaultCancellationToken ())

    let startOnThreadPool cancellationToken (computation: Async2<_>) =
        Task.Run<'T>(fun () -> computation.StartInIsolatedTrampoline cancellationToken)

    let startImmediate cancellationToken (computation: Async2<_>) =
        computation.StartInIsolatedTrampoline cancellationToken

    let cancellationTokenAsync = Async2(fun ctx -> Task.FromResult(ctx.CancellationToken))

    /// Starts the workers and completes once all of them have finished, whatever their outcome.
    /// Every bind checks for cancellation before it starts its source, so starting the workers inside
    /// the bound computation leaves no check between starting and awaiting them: once any worker has
    /// started, the caller waits for all of them.
    let startAndJoin (startWorkers: unit -> Task<'W> array) =
        Async2<unit>(fun _ ->
            Task.WhenAll(startWorkers ()).ContinueWith(
                Func<Task<'W array>, unit>(ignore),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            ))

    /// Starts `computation` with `start`. When creation options are given, the returned task is created with
    /// them and completes once the computation has finished.
    let startAsTask start cancellationToken (computation: Async2<'T>) taskCreationOptions : Task<'T> =
        match taskCreationOptions with
        | None
        | Some TaskCreationOptions.None -> start cancellationToken computation
        | Some (options: TaskCreationOptions) ->
            let completion = TaskCompletionSource<'T>(options)
            let running: Task<'T> = start cancellationToken computation
            running.ContinueWith(
                Action<Task<'T>>(fun running -> completion.TrySetFromTask running |> ignore),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            )
            |> ignore
            completion.Task

    /// The result of a callback-driven operation, settled exactly once by whichever comes first:
    /// the operation itself, after a successful `TryClaim`, or cancellation.
    [<Sealed>]
    type OnceCompletion<'T>() =
        let completion = TaskCompletionSource<'T>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable claimed = 0
        let mutable registration = new CancellationTokenRegistration()

        member _.Task = completion.Task

        member _.IsClaimed = Volatile.Read(&claimed) = 1

        /// True only for the first caller, which then stops listening to cancellation.
        member _.TryClaim() =
            if Interlocked.Exchange(&claimed, 1) = 0 then
                registration.Dispose()
                true
            else
                false

        /// On cancellation before the operation claims the result, runs `onCancel` and completes as canceled.
        member this.RegisterCancellation(cancellationToken: CancellationToken, onCancel: unit -> unit) =
            registration <-
                cancellationToken.Register(fun () ->
                    if this.TryClaim() then
                        onCancel ()
                        completion.TrySetCanceled cancellationToken |> ignore)

        member _.Complete(getResult: unit -> 'T) =
            try
                completion.TrySetResult(getResult ()) |> ignore
            with error ->
                completion.TrySetException error |> ignore

    /// Exceptions from a cooperative cancellation routine are ignored, as in FSharp.Core.
    let runCancelAction cancelAction =
        match cancelAction with
        | Some cancel ->
            try
                cancel ()
            with _ ->
                ()
        | None -> ()

    /// Creates a delegate of an arbitrary `'Delegate: delegate<'T, unit>` type that calls `f`.
    [<Sealed>]
    type FuncDelegate<'T>(f: 'T -> unit) =
        member _.Invoke(sender: obj, args: 'T) : unit =
            ignore sender
            f args

        static member Create<'Delegate when 'Delegate :> Delegate>(f) =
            let invoke =
                typeof<FuncDelegate<'T>>.GetMethod("Invoke", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)

            Delegate.CreateDelegate(typeof<'Delegate>, FuncDelegate<'T>(f), invoke) :?> 'Delegate

    /// Awaits a child started by `StartChild`. Like FSharp.Core, the timeout runs from the start of the await,
    /// and on expiry the child is canceled and allowed to finish before `TimeoutException` is raised.
    let awaitChildWithTimeout (childCancellation: CancellationTokenSource) (running: Task<'T>) millisecondsTimeout =
        match millisecondsTimeout with
        | 0 ->
            async2 {
                if running.IsCompleted then
                    return! running
                else
                    return raise (TimeoutException())
            }
        | timeout ->
            async2 {
                if not running.IsCompleted then
                    let! ct = CurrentCancellationToken
                    do!
                        (running :> Task)
                            .WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)
                            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing)

                    if not running.IsCompleted then
                        ct.ThrowIfCancellationRequested()
                        childCancellation.Cancel()
                        do! (running :> Task).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing)
                        raise (TimeoutException())

                return! running
            }

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
        let pending = computation.StartInIsolatedTrampoline(getToken cancellationToken)
        pending.GetAwaiter().GetResult()

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
        let ct = getToken cancellationToken
        startAsTask startOnThreadPool ct computation taskCreationOptions

    static member StartChildAsTask(computation, ?taskCreationOptions) =
        async2 {
            let! ct = CurrentCancellationToken
            return startAsTask startImmediate ct computation taskCreationOptions
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
            let! ct = CurrentCancellationToken
            try
                return! computation
            with
            | :? OperationCanceledException as ex when ex.CancellationToken = ct ->
                compensation ex
                return Unchecked.defaultof<'T>
        }

    static member OnCancel(interruption: unit -> unit) : Async2<IDisposable> =
        async2 {
            let! ct = CurrentCancellationToken
            return ct.Register(Action interruption) :> IDisposable
        }

    static member StartChild
        (computation: Async2<'T>, ?millisecondsTimeout: int)
        : Async2<Async2<'T>> =
            async2 {
                let! ct = CurrentCancellationToken

                match millisecondsTimeout with
                | None
                | Some Timeout.Infinite ->
                    let started = computation |> startOnThreadPool ct
                    return async2 {
                        return! started
                    }
                | Some timeout ->
                    // A timeout cancels the child, possibly after it finished, so its source is
                    // never disposed; only the link to the parent token is dropped once it finishes.
                    let childCancellation = new CancellationTokenSource()
                    let link = ct.Register(fun () -> childCancellation.Cancel())
                    let started = computation |> startOnThreadPool childCancellation.Token
                    started.ContinueWith(
                        Action<Task<'T>>(fun _ -> link.Dispose()),
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default
                    )
                    |> ignore
                    return awaitChildWithTimeout childCancellation started timeout
            }

    static member Parallel
        (computations: seq<Async2<'T>>, ?maxDegreeOfParallelism: int)
        : Async2<'T array> =
        let computations = computations |> Seq.toArray
        let maxDegreeOfParallelism = defaultArg maxDegreeOfParallelism (max 1 computations.Length)

        if maxDegreeOfParallelism < 1 then
            invalidArg "maxDegreeOfParallelism" (sprintf "maxDegreeOfParallelism must be positive, was %d" maxDegreeOfParallelism)

        async2 {
            let! ct = CurrentCancellationToken
            use cts = CancellationTokenSource.CreateLinkedTokenSource ct
            let innerToken = cts.Token
            let mutable firstFailure : ExceptionDispatchInfo option = None
            let mutable index = -1
            let workerCount = min computations.Length maxDegreeOfParallelism
            let results = Array.zeroCreate<'T> computations.Length
            do! startAndJoin (fun () ->
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
                    Task.Run<unit>(fun () -> worker.StartInIsolatedTrampoline innerToken)))

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
            let! ct = CurrentCancellationToken
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

            do! startAndJoin (fun () -> computations |> Array.map worker)
            return result
        }

    static member SwitchToNewThread() : Async2<unit> =
        async2 {
            let! ct = CurrentCancellationToken
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
            let! ct = CurrentCancellationToken
            do! Task.Run(Action(fun () -> ()), ct)
        }

    static member SwitchToContext(syncContext: SynchronizationContext | null) : Async2<unit> =
        Async2(fun ctx ->
            let completion = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

            ctx.CancellationToken.ThrowIfCancellationRequested()

            if isNull syncContext then
                Task.Run(Action(fun () -> completion.SetResult()), ctx.CancellationToken)
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
        async2 {
            let! ct = CurrentCancellationToken
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
        Async2(fun _ -> task)

    static member AwaitTask(task: Task) : Async2<unit> =
        async2 { return! task }

    static member Await(task: Task<'T>) : Async2<'T> =
        Async2(fun _ -> task)

    static member Await(task: Task) : Async2<unit> =
        async2 { return! task }

    static member Await(task: ValueTask<'T>) : Async2<'T> =
        async2 { return! task }

    static member Await(task: ValueTask) : Async2<unit> =
        async2 { return! task }

    static member StartTaskImmediate(createTask: CancellationToken -> Task<'T>) : Async2<'T> =
        async2 { let! ct = CurrentCancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> Task) : Async2<unit> =
        async2 { let! ct = CurrentCancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask<'T>) : Async2<'T> =
        async2 { let! ct = CurrentCancellationToken in return! createTask ct }

    static member StartTaskImmediate(createTask: CancellationToken -> ValueTask) : Async2<unit> =
        async2 { let! ct = CurrentCancellationToken in return! createTask ct }

    static member Sleep(millisecondsDueTime: int) : Async2<unit> =
        async2 {
            let! ct = CurrentCancellationToken
            return! Task.Delay(millisecondsDueTime, ct)
        }

    static member Sleep(dueTime: TimeSpan) : Async2<unit> =
        async2 {
            let! ct = CurrentCancellationToken
            return! Task.Delay(dueTime, ct)
        }

    static member AwaitWaitHandle(waitHandle: WaitHandle, ?millisecondsTimeout: int) : Async2<bool> =
        async2 {
            let! ct = CurrentCancellationToken

            let tcs = TaskCompletionSourceWithCancellation<bool>(ct)

            let callback =
                WaitOrTimerCallback(fun _ timedOut -> tcs.TrySetResult(not timedOut) |> ignore)

            let handle =
                ThreadPool.RegisterWaitForSingleObject(waitHandle, callback, null, defaultArg millisecondsTimeout Timeout.Infinite, true)
            try
                return! tcs.Task
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
        : Async2<'T> =
        async2 {
            let! ct = CurrentCancellationToken
            let completion = OnceCompletion<'T>()
            completion.RegisterCancellation(ct, fun () -> runCancelAction cancelAction)

            let callback =
                AsyncCallback(fun iar ->
                    if not iar.CompletedSynchronously && completion.TryClaim() then
                        completion.Complete(fun () -> endAction iar))

            let iar = beginAction (callback, null)

            if iar.CompletedSynchronously then
                completion.TryClaim() |> ignore
                return endAction iar
            else
                return! completion.Task
        }

    static member FromBeginEnd
        (
            arg: 'Arg1,
            beginAction: 'Arg1 * AsyncCallback * obj -> IAsyncResult,
            endAction: IAsyncResult -> 'T,
            ?cancelAction: unit -> unit
        )
        : Async2<'T> =
        Async2.FromBeginEnd((fun (callback, state) -> beginAction (arg, callback, state)), endAction, ?cancelAction = cancelAction)

    static member FromBeginEnd
        (
            arg1: 'Arg1,
            arg2: 'Arg2,
            beginAction: 'Arg1 * 'Arg2 * AsyncCallback * obj -> IAsyncResult,
            endAction: IAsyncResult -> 'T,
            ?cancelAction: unit -> unit
        )
        : Async2<'T> =
        Async2.FromBeginEnd(
            (fun (callback, state) -> beginAction (arg1, arg2, callback, state)),
            endAction,
            ?cancelAction = cancelAction
        )

    static member FromBeginEnd
        (
            arg1: 'Arg1,
            arg2: 'Arg2,
            arg3: 'Arg3,
            beginAction: 'Arg1 * 'Arg2 * 'Arg3 * AsyncCallback * obj -> IAsyncResult,
            endAction: IAsyncResult -> 'T,
            ?cancelAction: unit -> unit
        )
        : Async2<'T> =
        Async2.FromBeginEnd(
            (fun (callback, state) -> beginAction (arg1, arg2, arg3, callback, state)),
            endAction,
            ?cancelAction = cancelAction
        )

    static member AwaitEvent(event: IEvent<'Delegate, 'T>, ?cancelAction: unit -> unit) : Async2<'T> =
        async2 {
            let! ct = CurrentCancellationToken
            let completion = OnceCompletion<'T>()
            let handler = ref Unchecked.defaultof<'Delegate>

            handler.Value <-
                FuncDelegate<'T>.Create<'Delegate>(fun args ->
                    if completion.TryClaim() then
                        event.RemoveHandler handler.Value
                        completion.Complete(fun () -> args))

            completion.RegisterCancellation(ct, fun () ->
                event.RemoveHandler handler.Value
                runCancelAction cancelAction)

            event.AddHandler handler.Value

            // Cancellation may have run before the handler was added.
            if completion.IsClaimed then
                event.RemoveHandler handler.Value

            return! completion.Task
        }

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
    // Prefer the SRTP sources while the concrete awaitable type is still unknown.
    open Async2BuilderAwaitableExtensions

    type Async2 with
        [<NoEagerConstraintApplication>]
        static member inline Await(task: AwaitableHelpers.Awaitable<_, _, _>)
            : Async2<'T> = async2 { return! task }

        [<NoEagerConstraintApplication>]
        static member inline StartTaskImmediate
            (createTask: AwaitableHelpers.CancellableAwaitable<_, _, _>)
            : Async2<'T> = async2 { return! createTask }

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

    let parallelDoLimit maxDegreeOfParallelism (computations: seq<Async2<'T>>) : Async2<unit> =
        parallelLimit maxDegreeOfParallelism computations |> Async2.Ignore
