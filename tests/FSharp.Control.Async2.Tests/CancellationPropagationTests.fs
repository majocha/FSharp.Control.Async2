namespace FSharp.Control.Async2.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Runtime.CompilerServices
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Xunit

module CancellationPropagationTests =
    let private countCancellationThrows token action =
        let mutable throws = 0
        let observer =
            EventHandler<FirstChanceExceptionEventArgs>(fun _ args ->
                match args.Exception with
                | :? OperationCanceledException as error when error.CancellationToken = token ->
                    Interlocked.Increment(&throws) |> ignore
                | _ -> ())
        AppDomain.CurrentDomain.FirstChanceException.AddHandler observer
        try
            action ()
            Volatile.Read(&throws)
        finally
            AppDomain.CurrentDomain.FirstChanceException.RemoveHandler observer

    let private canceledTask token (computation: Async2<'T>) =
        let running = Async2.StartImmediateAsTask(computation, cancellationToken = token)
        let error =
            Assert.ThrowsAny<OperationCanceledException>(fun () ->
                running.GetAwaiter().GetResult() |> ignore)
        Assert.True(running.IsCanceled)
        Assert.Equal(token, error.CancellationToken)
        error

    [<Fact>]
    let ``Native completion is a struct and preserves payloads across suspension`` () =
        Assert.True(typeof<Async2Result<int>>.IsValueType)
        let roundTrip (expected: 'T) =
            let gate = TaskCompletionSource<_>(TaskCreationOptions.RunContinuationsAsynchronously)
            let computation = async2 {
                let! value = async2 { return! gate.Task }
                return struct (value, value)
            }
            let running = Async2.StartImmediateAsTask(computation, cancellationToken = CancellationToken.None)
            Assert.False(running.IsCompleted)
            gate.SetResult expected
            let struct (first, second) = running.WaitAsync(TimeSpan.FromSeconds 5.0).GetAwaiter().GetResult()
            Assert.Equal<'T>(expected, first)
            Assert.Equal<'T>(expected, second)
        roundTrip 42L
        roundTrip (struct (17, "payload", Guid.NewGuid()))
        roundTrip (None: int option)
        roundTrip Unchecked.defaultof<string>

    [<Fact>]
    let ``External ambient cancellation is converted once and keeps its exception`` () =
        use cts = new CancellationTokenSource()
        let gate = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
        let original = OperationCanceledException("external cancellation", cts.Token)
        let mutable finalized = 0
        let mutable handled = 0
        let completed = TaskCompletionSource<OperationCanceledException>(TaskCreationOptions.RunContinuationsAsynchronously)
        let rec loop remaining =
            async2 {
                try
                    try
                        if remaining = 0 then return! gate.Task
                        else return! loop (remaining - 1)
                    with _ ->
                        handled <- handled + 1
                        return -1
                finally finalized <- finalized + 1
            }
        let throws =
            countCancellationThrows cts.Token (fun () ->
                Async2.StartWithContinuations(
                    loop 128,
                    (fun _ -> Assert.Fail("Cancellation invoked success")),
                    (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
                    completed.SetResult,
                    cancellationToken = cts.Token)
                cts.Cancel()
                gate.SetException original
                completed.Task.WaitAsync(TimeSpan.FromSeconds 5.0).GetAwaiter().GetResult() |> ignore)
        Assert.Same(original, completed.Task.Result)
        Assert.InRange(throws, 1, 4)
        Assert.Equal(129, finalized)
        Assert.Equal(0, handled)

    [<Fact>]
    let ``TryWith normalizes cancellation observed by a suspended native child`` () =
        use cts = new CancellationTokenSource()
        let gate = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
        let child = async2 { return! gate.Task }
        let computation = async2 {
            try
                return! child
            with _ ->
                Assert.Fail("Ambient cancellation entered an exception handler")
                return -1
        }
        let running = Async2.StartImmediateAsTask(computation, cancellationToken = cts.Token)
        cts.Cancel()
        gate.SetCanceled(cts.Token)
        let error = Assert.Throws<OperationCanceledException>(fun () ->
            running.WaitAsync(TimeSpan.FromSeconds 5.0).GetAwaiter().GetResult() |> ignore)
        Assert.Equal(cts.Token, error.CancellationToken)

    [<Fact>]
    let ``Deep cancellation invokes its continuation and unwinds every finally`` () =
        use cts = new CancellationTokenSource()
        let mutable finalized = 0
        let mutable handled = 0
        let mutable continued = 0
        let rec loop remaining =
            async2 {
                try
                    try
                        if remaining = 0 then
                            cts.Cancel()
                            return 42
                        else
                            let! value = loop (remaining - 1)
                            continued <- continued + 1
                            return value
                    with _ ->
                        handled <- handled + 1
                        return -1
                finally
                    finalized <- finalized + 1
            }
        let mutable cancellation: OperationCanceledException option = None
        let throws =
            countCancellationThrows cts.Token (fun () ->
                Async2.StartWithContinuations(
                    loop 10_000,
                    (fun _ -> Assert.Fail("Cancellation invoked success")),
                    (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
                    (fun error -> cancellation <- Some error),
                    cancellationToken = cts.Token))
        Assert.Equal(0, throws)
        Assert.Equal(10_001, finalized)
        Assert.Equal(0, handled)
        Assert.Equal(0, continued)
        Assert.Equal(cts.Token, cancellation.Value.CancellationToken)

    [<Fact>]
    let ``Nested cancellation throws once at the task boundary`` () =
        use cts = new CancellationTokenSource()
        let rec loop remaining =
            async2 {
                if remaining = 0 then
                    cts.Cancel()
                    return 42
                else
                    return! loop (remaining - 1)
            }
        let mutable running: Task<int> = null
        let throws =
            countCancellationThrows cts.Token (fun () ->
                running <- Async2.StartImmediateAsTask(loop 128, cancellationToken = cts.Token))
        Assert.Equal(1, throws)
        Assert.True(running.IsCanceled)
        let error = Assert.Throws<OperationCanceledException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Equal(cts.Token, error.CancellationToken)
        Assert.InRange(StackTrace(error).FrameCount, 1, 64)

    [<Fact>]
    let ``Cancellation after suspension waits for asynchronous disposal`` () = task {
        use cts = new CancellationTokenSource()
        let suspended = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowDispose = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let disposing = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable disposed = false
        let mutable continued = false
        let resource =
            { new IAsyncDisposable with
                member _.DisposeAsync() =
                    ValueTask(task {
                        disposing.SetResult()
                        do! allowDispose.Task
                        disposed <- true
                    }) }
        let child = async2 {
            use _resource = resource
            do! suspended.Task
            return 42
        }
        let parent = async2 {
            let! value = child
            continued <- true
            return value
        }
        let running = Async2.StartImmediateAsTask(parent, cancellationToken = cts.Token)
        cts.Cancel()
        suspended.SetResult()
        do! disposing.Task.WaitAsync(TimeSpan.FromSeconds 5.0)
        Assert.False(running.IsCompleted)
        allowDispose.SetResult()
        let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> running)
        Assert.Equal(cts.Token, error.CancellationToken)
        Assert.True(disposed)
        Assert.False(continued)
    }

    [<Theory; InlineData(false); InlineData(true)>]
    let ``Successful external await enters cleanup even if its token was canceled`` asynchronous =
        use cts = new CancellationTokenSource()
        let mutable disposed = false
        let acquired =
            Async2<unit>(fun _ ->
                cts.Cancel()
                ValueTask<unit>(()))
        let computation =
            if asynchronous then
                async2 {
                    do! acquired
                    use _resource =
                        { new IAsyncDisposable with
                            member _.DisposeAsync() =
                                disposed <- true
                                ValueTask.CompletedTask }
                    return 42
                }
            else
                async2 {
                    do! acquired
                    use _resource =
                        { new IDisposable with
                            member _.Dispose() = disposed <- true }
                    return 42
                }
        canceledTask cts.Token computation |> ignore
        Assert.True(disposed)

    [<Fact>]
    let ``Foreign canceled Async2 task remains catchable`` () =
        use foreign = new CancellationTokenSource()
        foreign.Cancel()
        let child = async2 { return! Task.FromCanceled<int>(foreign.Token) }
        let result =
            async2 {
                try
                    let! _ = child
                    return false
                with :? OperationCanceledException as error ->
                    return error.CancellationToken = foreign.Token
            }
            |> fun computation -> Async2.RunSynchronouslyImmediate(computation, cancellationToken = CancellationToken.None)
        Assert.True(result)

    [<Theory; InlineData(false); InlineData(true)>]
    let ``Canceled loops stop before evaluating another iteration`` useFor =
        use cts = new CancellationTokenSource()
        let mutable iterations = 0
        let mutable enumeratorDisposed = false
        let sequence =
            seq {
                try
                    for i in 1..10 do yield i
                finally enumeratorDisposed <- true
            }
        let child = async2 {
            cts.Cancel()
            return ()
        }
        let computation =
            if useFor then
                async2 {
                    for _ in sequence do
                        iterations <- iterations + 1
                        do! child
                }
            else
                async2 {
                    while iterations < 10 do
                        iterations <- iterations + 1
                        do! child
                }
        canceledTask cts.Token computation |> ignore
        Assert.Equal(1, iterations)
        if useFor then Assert.True(enumeratorDisposed)

    [<Fact>]
    let ``Cancellation in asynchronous enumeration disposes its enumerator`` () =
        use cts = new CancellationTokenSource()
        let mutable moves = 0
        let mutable disposed = false
        let source =
            { new IAsyncEnumerable<int> with
                member _.GetAsyncEnumerator _ =
                    { new IAsyncEnumerator<int> with
                        member _.Current = moves
                        member _.MoveNextAsync() =
                            moves <- moves + 1
                            ValueTask<bool>(true)
                        member _.DisposeAsync() =
                            disposed <- true
                            ValueTask.CompletedTask } }
        let computation = async2 {
            for _ in source do
                do! async2 {
                    cts.Cancel()
                    return ()
                }
        }
        canceledTask cts.Token computation |> ignore
        Assert.Equal(1, moves)
        Assert.True(disposed)

    [<Fact>]
    let ``TryCancelled compensates once and propagates cancellation`` () =
        use cts = new CancellationTokenSource()
        let mutable compensated = 0
        let child = async2 {
            cts.Cancel()
            return 42
        }
        let computation =
            child
            |> fun computation -> Async2.TryCancelled(computation, fun error ->
                Assert.Equal(cts.Token, error.CancellationToken)
                compensated <- compensated + 1)
        let throws =
            countCancellationThrows cts.Token (fun () ->
                Async2.StartWithContinuations(
                    computation,
                    (fun _ -> Assert.Fail("Cancellation invoked success")),
                    (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
                    (fun _ -> ()),
                    cancellationToken = cts.Token))
        Assert.Equal(0, throws)
        Assert.Equal(1, compensated)

    [<Fact>]
    let ``Nested TryCancelled wrappers share the cancellation exception`` () =
        use cts = new CancellationTokenSource()
        let errors = ResizeArray<OperationCanceledException>()
        let computation =
            async2 {
                cts.Cancel()
                return 42
            }
            |> fun child -> Async2.TryCancelled(child, errors.Add)
            |> fun child -> Async2.TryCancelled(child, errors.Add)
        Async2.StartWithContinuations(
            computation,
            (fun _ -> Assert.Fail("Cancellation invoked success")),
            (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
            errors.Add,
            cancellationToken = cts.Token)
        Assert.Equal(3, errors.Count)
        Assert.Same(errors[0], errors[1])
        Assert.Same(errors[0], errors[2])

    [<Theory; InlineData(false); InlineData(true)>]
    let ``TryCancelled compensates once for synchronous cancellation factory failures`` requestCancellation =
        use cts = new CancellationTokenSource()
        let original = OperationCanceledException("factory cancellation", cts.Token)
        let mutable compensated = 0
        let computation =
            Async2<int>(fun _ ->
                if requestCancellation then cts.Cancel()
                raise original)
            |> fun child -> Async2.TryCancelled(child, fun error ->
                Assert.Same(original, error)
                compensated <- compensated + 1)
        let mutable actual: OperationCanceledException option = None
        Async2.StartWithContinuations(
            computation,
            (fun _ -> Assert.Fail("Cancellation invoked success")),
            (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
            (fun error -> actual <- Some error),
            cancellationToken = cts.Token)
        Assert.Same(original, actual.Value)
        Assert.Equal(1, compensated)

    [<Fact>]
    let ``Finally failures are not replaced by cooperative cancellation`` () =
        use cts = new CancellationTokenSource()
        let failure = InvalidOperationException("cleanup failed")
        let computation = async2 {
            try
                cts.Cancel()
                return 42
            finally raise failure
        }
        let running = Async2.StartImmediateAsTask(computation, cancellationToken = cts.Token)
        let actual = Assert.Throws<InvalidOperationException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Same(failure, actual)
        Assert.True(running.IsFaulted)

    [<Fact>]
    let ``Cancellation compensation failures reach the exception continuation`` () =
        use cts = new CancellationTokenSource()
        let failure = InvalidOperationException("compensation failed")
        let computation =
            async2 {
                cts.Cancel()
                return 42
            }
            |> fun child -> Async2.TryCancelled(child, fun _ -> raise failure)
        let mutable actual: exn option = None
        Async2.StartWithContinuations(
            computation,
            (fun _ -> Assert.Fail("Compensation failure invoked success")),
            (fun error -> actual <- Some error),
            (fun _ -> Assert.Fail("Compensation failure invoked cancellation")),
            cancellationToken = cts.Token)
        Assert.Same(failure, actual.Value)

    [<Fact>]
    let ``Pre-canceled computations do not evaluate their bodies`` () =
        use cts = new CancellationTokenSource()
        cts.Cancel()
        let mutable started = false
        let computation = async2 {
            started <- true
            return 42
        }
        let throws =
            countCancellationThrows cts.Token (fun () ->
                Async2.StartWithContinuations(
                    computation,
                    (fun _ -> Assert.Fail("Cancellation invoked success")),
                    (fun _ -> Assert.Fail("Cancellation invoked the exception continuation")),
                    (fun error -> Assert.Equal(cts.Token, error.CancellationToken)),
                    cancellationToken = cts.Token))
        Assert.Equal(0, throws)
        Assert.False(started)

    [<Fact>]
    let ``Parallel propagates native worker cancellation`` () =
        use cts = new CancellationTokenSource()
        let child = async2 {
            cts.Cancel()
            return 42
        }
        let mutable running: Task<int array> = null
        let throws =
            countCancellationThrows cts.Token (fun () ->
                running <- Async2.StartImmediateAsTask(Async2.Parallel([child], 1), cancellationToken = cts.Token)
                running.ContinueWith(
                    Action<Task<int array>>(fun _ -> ()),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default)
                    .WaitAsync(TimeSpan.FromSeconds 5.0)
                    .GetAwaiter()
                    .GetResult())
        Assert.Equal(1, throws)
        Assert.True(running.IsCanceled)

    [<Fact>]
    let ``StartChild cancellation stays catchable when joined under another token`` () =
        use cts = new CancellationTokenSource()
        let gate = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let child = async2 {
            do! gate.Task
            cts.Cancel()
            return 42
        }
        let join = Async2.RunSynchronouslyImmediate(Async2.StartChild child, cancellationToken = cts.Token)
        gate.SetResult()
        let caught =
            async2 {
                try
                    let! _ = join
                    return false
                with :? OperationCanceledException as error ->
                    return error.CancellationToken = cts.Token
            }
            |> fun computation -> Async2.RunSynchronouslyImmediate(computation, cancellationToken = CancellationToken.None)
        Assert.True(caught)

    [<Fact>]
    let ``And binds start their right source before cancellation of the left source`` () = task {
        use cts = new CancellationTokenSource()
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let allowFinish = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let finished = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable finalized = false
        let mutable continued = false
        let right = async2 {
            try
                started.SetResult()
                do! allowFinish.Task
                return 2
            finally
                finalized <- true
                finished.SetResult()
        }
        let left = async2 {
            do! started.Task
            cts.Cancel()
            return 1
        }
        let computation = async2 {
            let! left = left
            and! right = right
            continued <- true
            return left + right
        }
        let running = Async2.StartImmediateAsTask(computation, cancellationToken = cts.Token)
        do! started.Task.WaitAsync(TimeSpan.FromSeconds 5.0)
        Assert.True(running.IsCompleted)
        allowFinish.SetResult()
        let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> running)
        Assert.Equal(cts.Token, error.CancellationToken)
        do! finished.Task.WaitAsync(TimeSpan.FromSeconds 5.0)
        Assert.True(finalized)
        Assert.False(continued)
    }

    [<Fact>]
    let ``And binds all native and external source combinations`` () =
        let native = async2 { return 20 }
        let cold = fun (_: CancellationToken) -> ValueTask<int>(22)
        let hot = Task.FromResult 22
        let computations = [
            async2 { let! a = native
                     and! b = native
                     return a + b }
            async2 { let! a = native
                     and! b = cold
                     return a + b }
            async2 { let! a = cold
                     and! b = native
                     return a + b }
            async2 { let! a = native
                     and! b = hot
                     return a + b }
            async2 { let! a = hot
                     and! b = native
                     return a + b }
        ]
        let results = computations |> List.map Async2.RunSynchronouslyImmediate
        Assert.Equal<int list>([40; 42; 42; 42; 42], results)

    [<Theory; InlineData(false); InlineData(true)>]
    let ``And binds do not await a hot right source after native left cancellation`` useValueTask =
        use cts = new CancellationTokenSource()
        let right = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)
        let left = async2 {
            cts.Cancel()
            return 1
        }
        let computation =
            if useValueTask then
                async2 {
                    let! a = left
                    and! b = ValueTask<int>(right.Task)
                    return a + b
                }
            else
                async2 {
                    let! a = left
                    and! b = right.Task
                    return a + b
                }
        let running = Async2.StartImmediateAsTask(computation, cancellationToken = cts.Token)
        Assert.True(running.IsCanceled)
        right.SetResult 2

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let private throwAtOrigin (error: exn) : int =
        raise error

    [<Fact>]
    let ``Cached faults retain their origin without accumulating nested async stacks`` () =
        let failure = InvalidOperationException("original failure")
        let rec loop remaining =
            async2 {
                if remaining = 0 then
                    return throwAtOrigin failure
                else
                    return! loop (remaining - 1)
            }
        let actual = Assert.Throws<InvalidOperationException>(fun () -> Async2.RunSynchronouslyImmediate(loop 4096) |> ignore)
        Assert.Same(failure, actual)
        Assert.Contains("throwAtOrigin", actual.StackTrace)
        Assert.InRange(StackTrace(actual).FrameCount, 1, 64)

    [<Fact>]
    let ``ExceptionCache returns one dispatch info under concurrent access`` () =
        let failure = InvalidOperationException("shared failure")
        let captures = Array.zeroCreate<ExceptionDispatchInfo> 128
        Parallel.For(0, captures.Length, fun i ->
            captures[i] <- ExceptionCache.captureOrRetrieve failure)
        |> ignore
        for capture in captures do
            Assert.Same(failure, capture.SourceException)
            Assert.Same(captures[0], capture)

    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let private abandonedCancellation () =
        use cts = new CancellationTokenSource()
        let mutable queued: Task<int> option = None
        let parent =
            Async2<int>(fun ct ->
                queued <- Some ((Async2<int>(fun ct ->
                    cts.Cancel()
                    ValueTask<int>(Task.FromCanceled<int>(ct)))).StartTrampolined ct)
                ValueTask<int>(0))
        parent.StartTrampolined cts.Token |> _.GetAwaiter().GetResult() |> ignore
        let task = queued.Value
        Assert.True(task.IsCanceled)
        let found, error = ExceptionCache.tryGetCancellation task
        Assert.True(found)
        WeakReference(task), WeakReference(error)

    [<Fact>]
    let ``Exception caches do not retain abandoned cancellation tasks or exceptions`` () =
        let task, error = abandonedCancellation ()
        for _ in 1..3 do
            GC.Collect()
            GC.WaitForPendingFinalizers()
        Assert.False(task.IsAlive)
        Assert.False(error.IsAlive)
