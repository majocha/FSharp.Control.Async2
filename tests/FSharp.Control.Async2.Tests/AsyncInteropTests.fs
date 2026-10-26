namespace FSharp.Control.Async2.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Microsoft.FSharp.Control

module AsyncInteropTests =

    let private wait (task: Task) =
        Assert.True(task.Wait(TimeSpan.FromSeconds 30.0), "Task did not finish within 30 seconds.")

    /// A Begin/End operation that completes when `gate` does, on another thread.
    let private beginGated (gate: Task<'T>) (callback: AsyncCallback, state: obj) : IAsyncResult =
        let operation = gate.ContinueWith(Func<Task<'T>, 'T>(fun gate -> gate.Result), TaskScheduler.Default)
        operation.ContinueWith(Action<Task<'T>>(fun operation -> callback.Invoke operation), TaskScheduler.Default)
        |> ignore
        operation :> IAsyncResult

    let private endGated<'T> (iar: IAsyncResult) = (iar :?> Task<'T>).GetAwaiter().GetResult()

    type private CompletedSynchronously(value: int) =
        member _.Value = value

        interface IAsyncResult with
            member _.AsyncState = null
            member _.AsyncWaitHandle = new ManualResetEvent(true)
            member _.CompletedSynchronously = true
            member _.IsCompleted = true

    /// An event that counts its handlers.
    type private CountingEvent<'T>() =
        let inner = Event<'T>()
        let mutable handlers = 0

        member _.Handlers = handlers
        member _.Trigger value = inner.Trigger value

        interface IEvent<Handler<'T>, 'T> with
            member _.AddHandler handler =
                Interlocked.Increment(&handlers) |> ignore
                inner.Publish.AddHandler handler
            member _.RemoveHandler handler =
                Interlocked.Decrement(&handlers) |> ignore
                inner.Publish.RemoveHandler handler
            member _.Subscribe observer = inner.Publish.Subscribe observer

    type CustomArgs(value: int) =
        inherit EventArgs()
        member _.Value = value

    [<Fact>]
    let ``FromBeginEnd returns the result of endAction once the operation completes`` () =
        let gate = TaskCompletionSource<int>()
        let running = Async2.FromBeginEnd(beginGated gate.Task, endGated<int>) |> Async2.StartAsTask
        Assert.False(running.IsCompleted)
        gate.SetResult 42
        wait running
        Assert.Equal(42, running.Result)

    [<Fact>]
    let ``FromBeginEnd calls endAction inline when the operation completes synchronously`` () =
        let callbacks = ref 0
        let beginAction (callback: AsyncCallback, _: obj) =
            let iar = CompletedSynchronously 7 :> IAsyncResult
            callback.Invoke iar
            callbacks.Value <- callbacks.Value + 1
            iar

        let result =
            Async2.FromBeginEnd(beginAction, fun iar -> (iar :?> CompletedSynchronously).Value)
            |> Async2.RunSynchronously

        Assert.Equal(7, result)
        Assert.Equal(1, callbacks.Value)

    [<Fact>]
    let ``FromBeginEnd propagates a failure of endAction`` () =
        let gate = TaskCompletionSource<int>()
        let failure = InvalidOperationException "end failed"
        let running =
            Async2.FromBeginEnd(beginGated gate.Task, fun (_: IAsyncResult) -> raise failure)
            |> Async2.StartAsTask
        gate.SetResult 0
        let actual = Assert.Throws<InvalidOperationException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Same(failure, actual)

    [<Fact>]
    let ``FromBeginEnd cancellation runs cancelAction and does not call endAction`` () =
        let gate = TaskCompletionSource<int>()
        let begun = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let cancelled = ref 0
        let ended = ref 0
        use cts = new CancellationTokenSource()
        let running =
            Async2.StartAsTask(
                Async2.FromBeginEnd(
                    (fun args ->
                        let iar = beginGated gate.Task args
                        begun.SetResult()
                        iar),
                    (fun iar ->
                        Interlocked.Increment(&ended.contents) |> ignore
                        endGated<int> iar),
                    fun () ->
                        Interlocked.Increment(&cancelled.contents) |> ignore
                        failwith "cancel actions may fail"),
                cancellationToken = cts.Token)

        wait begun.Task
        cts.Cancel()
        Assert.ThrowsAny<OperationCanceledException>(fun () -> running.GetAwaiter().GetResult() |> ignore) |> ignore
        gate.SetResult 1
        Thread.Sleep 50
        Assert.Equal(1, cancelled.Value)
        Assert.Equal(0, ended.Value)

    [<Fact>]
    let ``FromBeginEnd passes its arguments to beginAction`` () =
        let one = Async2.FromBeginEnd(1, (fun (a, callback, state) -> beginGated (Task.FromResult a) (callback, state)), endGated<int>)
        let two = Async2.FromBeginEnd(1, 2, (fun (a, b, callback, state) -> beginGated (Task.FromResult(a + b)) (callback, state)), endGated<int>)
        let three =
            Async2.FromBeginEnd(1, 2, 3, (fun (a, b, c, callback, state) -> beginGated (Task.FromResult(a + b + c)) (callback, state)), endGated<int>)

        Assert.Equal<int array>([| 1; 3; 6 |], Async2.Parallel [ one; two; three ] |> Async2.RunSynchronously)

    [<Fact>]
    let ``AwaitEvent returns the first event and stops listening`` () =
        let event = CountingEvent<int>()
        let running = Async2.AwaitEvent event |> Async2.StartAsTask
        SpinWait.SpinUntil((fun () -> event.Handlers = 1), 5000) |> ignore
        Assert.Equal(1, event.Handlers)
        event.Trigger 5
        event.Trigger 6
        wait running
        Assert.Equal(5, running.Result)
        Assert.Equal(0, event.Handlers)

    [<Fact>]
    let ``AwaitEvent works with EventHandler delegates`` () =
        let event = Event<EventHandler<CustomArgs>, CustomArgs>()
        let running = Async2.AwaitEvent event.Publish |> Async2.StartImmediateAsTask
        event.Trigger(null, CustomArgs 3)
        wait running
        Assert.Equal(3, running.Result.Value)

    [<Fact>]
    let ``AwaitEvent cancellation runs cancelAction and stops listening`` () =
        let event = CountingEvent<int>()
        let cancelled = ref 0
        use cts = new CancellationTokenSource()
        let running =
            Async2.StartImmediateAsTask(
                Async2.AwaitEvent(event, fun () -> Interlocked.Increment(&cancelled.contents) |> ignore),
                cts.Token)
        Assert.Equal(1, event.Handlers)
        cts.Cancel()
        Assert.ThrowsAny<OperationCanceledException>(fun () -> running.GetAwaiter().GetResult() |> ignore) |> ignore
        Assert.Equal(1, cancelled.Value)
        Assert.Equal(0, event.Handlers)
        event.Trigger 1

    [<Fact>]
    let ``StartChild with a timeout returns a child that finishes in time`` () =
        let result =
            async2 {
                let! child = Async2.StartChild(async2 { return 4 }, 5000)
                return! child
            }
            |> Async2.RunSynchronously
        Assert.Equal(4, result)

    [<Fact>]
    let ``StartChild timeout cancels the child and waits for it before raising`` () =
        let childFinished = ref false
        let child =
            async2 {
                try
                    do! Async2.Sleep Timeout.Infinite
                finally
                    Thread.Sleep 50
                    childFinished.Value <- true
            }

        let outcome =
            async2 {
                let! running = Async2.StartChild(child, 50)
                try
                    do! running
                    return "completed"
                with :? TimeoutException ->
                    return "timed out"
            }
            |> Async2.RunSynchronously

        Assert.Equal("timed out", outcome)
        Assert.True(childFinished.Value)

    [<Fact>]
    let ``StartChild timeout runs from the start of the await`` () =
        let result =
            async2 {
                let! child = Async2.StartChild(Async2.Sleep 1000 |> Async2.map (fun () -> 1), 800)
                do! Async2.Sleep 600
                return! child
            }
            |> Async2.RunSynchronously
        Assert.Equal(1, result)

    [<Fact>]
    let ``StartChild with a zero timeout only returns a finished child`` () =
        let gate = TaskCompletionSource<int>()
        let outcome =
            async2 {
                let! finished = Async2.StartChild(async2 { return 2 }, 0)
                do! Async2.Sleep 50
                let! value = finished
                let! pending = Async2.StartChild(Async2.AwaitTask gate.Task, 0)
                try
                    let! _ = pending
                    return -1
                with :? TimeoutException ->
                    return value
            }
            |> Async2.RunSynchronously
        gate.SetResult 0
        Assert.Equal(2, outcome)

    [<Fact>]
    let ``StartChild with a timeout is cancelled with its parent`` () =
        let childFinished = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        use cts = new CancellationTokenSource()
        let parent =
            async2 {
                let! _ =
                    Async2.StartChild(
                        async2 {
                            try
                                do! Async2.Sleep Timeout.Infinite
                            finally
                                childFinished.SetResult()
                        },
                        60_000)
                do! Async2.Sleep Timeout.Infinite
            }
        let running = Async2.StartAsTask(parent, cancellationToken = cts.Token)
        cts.CancelAfter 50
        wait childFinished.Task
        Assert.ThrowsAny<OperationCanceledException>(fun () -> running.GetAwaiter().GetResult()) |> ignore

    [<Fact>]
    let ``StartAsTask creates its task with the given options`` () =
        let task = Async2.StartAsTask(async2 { return 1 }, TaskCreationOptions.RunContinuationsAsynchronously)
        wait task
        Assert.Equal(TaskCreationOptions.RunContinuationsAsynchronously, task.CreationOptions)
        Assert.Equal(1, task.Result)

    [<Fact>]
    let ``StartAsTask rejects options a task completion source does not support`` () =
        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            Async2.StartAsTask(async2 { return 1 }, TaskCreationOptions.LongRunning) |> ignore)
        |> ignore

    [<Fact>]
    let ``StartAsTask with AttachedToParent keeps the parent task running`` () =
        let gate = TaskCompletionSource<unit>()
        let child: Task<unit> ref = ref null
        let parent =
            Task.Factory.StartNew(fun () ->
                child.Value <- Async2.StartAsTask(Async2.AwaitTask gate.Task, TaskCreationOptions.AttachedToParent))
        Assert.True(SpinWait.SpinUntil((fun () -> not (isNull child.Value)), 5000))
        Thread.Sleep 50
        Assert.False(parent.IsCompleted)
        gate.SetResult()
        wait parent
        Assert.True(child.Value.IsCompletedSuccessfully)

    [<Fact>]
    let ``StartAsTask with options propagates failure and cancellation`` () =
        let failure = InvalidOperationException "failed"
        let failed = Async2.StartAsTask(async2 { return raise failure }, TaskCreationOptions.RunContinuationsAsynchronously)
        Assert.Same(failure, Assert.Throws<InvalidOperationException>(fun () -> failed.GetAwaiter().GetResult() |> ignore))

        use cts = new CancellationTokenSource()
        let cancelled =
            Async2.StartAsTask(Async2.Sleep Timeout.Infinite, TaskCreationOptions.RunContinuationsAsynchronously, cts.Token)
        cts.Cancel()
        let error = Assert.ThrowsAny<OperationCanceledException>(fun () -> cancelled.GetAwaiter().GetResult())
        Assert.True(cancelled.IsCanceled)
        Assert.Equal(cts.Token, error.CancellationToken)

    [<Fact>]
    let ``StartChildAsTask creates its task with the given options`` () =
        let task =
            async2 {
                let! child = Async2.StartChildAsTask(async2 { return 2 }, TaskCreationOptions.RunContinuationsAsynchronously)
                let! value = child
                return child.CreationOptions, value
            }
            |> Async2.RunSynchronously
        Assert.Equal((TaskCreationOptions.RunContinuationsAsynchronously, 2), task)
