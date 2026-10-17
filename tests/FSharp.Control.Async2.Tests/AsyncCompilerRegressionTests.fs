namespace FSharp.Control.Async2.Tests

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources
open Xunit
open Microsoft.FSharp.Control

module AsyncCompilerRegressionTests =

    let rec private wrapToQueueDepth depth (computation: Async2<'T>) =
        if depth = 0 then
            computation
        else
            let inner = wrapToQueueDepth (depth - 1) computation
            Async2<'T>(fun ctx -> inner.StartTrampolined ctx.CancellationToken)

    type private SingleUseSource<'T>() =
        let mutable source = ManualResetValueTaskSourceCore<'T>()
        let mutable reads = 0

        member _.Reads = reads
        member _.SetResult value = source.SetResult value
        member _.SetException error = source.SetException error
        member this.ValueTask = ValueTask<'T>(this, source.Version)

        interface IValueTaskSource<'T> with
            member _.GetResult token =
                if Interlocked.Increment(&reads) <> 1 then
                    invalidOp "ValueTask was consumed more than once."
                source.GetResult token
            member _.GetStatus token = source.GetStatus token
            member _.OnCompleted(continuation, state, token, flags) =
                source.OnCompleted(continuation, state, token, flags)

    [<Theory>]
    [<InlineData(true, 0)>]
    [<InlineData(false, 0)>]
    [<InlineData(true, 1)>]
    [<InlineData(false, 1)>]
    [<InlineData(true, 2)>]
    [<InlineData(false, 2)>]
    let ``Queued starts consume ValueTask sources once and preserve outcomes`` completed outcome =
        let source = SingleUseSource<int>()
        let failure = InvalidOperationException("child failed")
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()
        let finish () =
            match outcome with
            | 0 -> source.SetResult 42
            | 1 -> source.SetException failure
            | _ -> source.SetException(OperationCanceledException(cancellation.Token))

        if completed then finish ()
        let child = Async2<int>(fun _ -> source.ValueTask) |> wrapToQueueDepth 130
        let parent = async2 { return! child }
        let running = (parent.StartTrampolined CancellationToken.None).AsTask()

        if not completed then
            Assert.False(running.IsCompleted)
            // Exercise completion on a different thread from the queued start.
            Task.Run(Action finish).GetAwaiter().GetResult()

        let bounded = running.WaitAsync(TimeSpan.FromSeconds 5.0)
        match outcome with
        | 0 -> Assert.Equal(42, bounded.GetAwaiter().GetResult())
        | 1 ->
            let actual = Assert.Throws<InvalidOperationException>(fun () -> bounded.GetAwaiter().GetResult() |> ignore)
            Assert.Same(failure, actual)
        | _ ->
            let actual = Assert.ThrowsAny<OperationCanceledException>(fun () -> bounded.GetAwaiter().GetResult() |> ignore)
            Assert.Equal(cancellation.Token, actual.CancellationToken)
            Assert.True(running.IsCanceled)
        Assert.Equal(1, source.Reads)

    [<Theory>]
    [<InlineData(true)>]
    [<InlineData(false)>]
    let ``StartWithContinuations consumes ValueTask once and flows caller context`` completed =
        let source = SingleUseSource<int>()
        let local = AsyncLocal<string>()
        let observed = TaskCompletionSource<struct (int * string)>(TaskCreationOptions.RunContinuationsAsynchronously)
        local.Value <- "caller"
        if completed then source.SetResult 42

        Async2.StartWithContinuations(
            (Async2<int>(fun _ -> source.ValueTask) |> wrapToQueueDepth 130),
            (fun value -> observed.SetResult(struct (value, local.Value))),
            observed.SetException,
            (fun error -> observed.SetException error))

        local.Value <- "outside"
        if not completed then
            Assert.False(observed.Task.IsCompleted)
            Task.Run(fun () -> source.SetResult 42).GetAwaiter().GetResult()

        let struct (value, context) = observed.Task.WaitAsync(TimeSpan.FromSeconds 5.0).GetAwaiter().GetResult()
        Assert.Equal(42, value)
        Assert.Equal("caller", context)
        Assert.Equal(1, source.Reads)

    [<Fact>]
    let ``Queued starts propagate synchronous factory failures`` () =
        let failure = InvalidOperationException("factory failed")
        let child = Async2<int>(fun _ -> raise failure) |> wrapToQueueDepth 130
        let parent = async2 { return! child }
        let running = (parent.StartTrampolined CancellationToken.None).AsTask()
        let actual = Assert.Throws<InvalidOperationException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Same(failure, actual)

    [<Fact>]
    let ``Inline starts return synchronous factory failures as ValueTask failures`` () =
        let failure = InvalidOperationException("factory failed")
        let child = Async2<int>(fun _ -> raise failure)
        let parent = async2 { return! child }
        let running = (parent.StartTrampolined CancellationToken.None).AsTask()
        let actual = Assert.Throws<InvalidOperationException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Same(failure, actual)

    [<Theory>]
    [<InlineData(true)>]
    [<InlineData(false)>]
    let ``Queued starts propagate ValueTask status and registration failures`` failStatus =
        let failure = InvalidOperationException("source failed")
        let source =
            { new IValueTaskSource<int> with
                member _.GetResult _ = invalidOp "Failed source must not be consumed."
                member _.GetStatus _ =
                    if failStatus then raise failure
                    ValueTaskSourceStatus.Pending
                member _.OnCompleted(_, _, _, _) = raise failure }
        let child = Async2<int>(fun _ -> ValueTask<int>(source, 0s)) |> wrapToQueueDepth 130
        let parent = async2 { return! child }
        let running = (parent.StartTrampolined CancellationToken.None).AsTask()
        let actual = Assert.Throws<InvalidOperationException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        Assert.Same(failure, actual)

    [<Fact>]
    let ``Queued starts preserve parent and child execution contexts`` () =
        let local = AsyncLocal<string>()
        let child =
            wrapToQueueDepth 130 (async2 {
                Assert.Equal("parent", local.Value)
                local.Value <- "child"
                do! Task.Yield()
                Assert.Equal("child", local.Value)
                return 42
            })
        let parent = async2 {
            local.Value <- "parent"
            let! result = child
            Assert.Equal("parent", local.Value)
            return result
        }
        Assert.Equal(42, (parent.StartTrampolined CancellationToken.None).AsTask().GetAwaiter().GetResult())

    [<Fact>]
    let ``Synchronous recursion and completion remain stack safe`` () =
        let mutable finalized = 0
        let rec loop depth = async2 {
            try
                if depth = 0 then
                    return 42
                else
                    let! result = loop (depth - 1)
                    return result + 1
            finally
                finalized <- finalized + 1
        }
        Assert.Equal(50_042, Async2.RunSynchronouslyImmediate(loop 50_000))
        Assert.Equal(50_001, finalized)

    [<Fact>]
    let ``StartChild result can be consumed repeatedly`` () =
        let result =
            async2 {
                let! child = Async2.StartChild(async2 {
                    do! Async2.Sleep 1
                    return 27
                })
                return! Async2.Parallel [ child; child; child; child ]
            }
            |> Async2.RunSynchronously

        Assert.Equal([| 27; 27; 27; 27 |], result)

    [<Fact>]
    let ``StartChild starts before its result is awaited`` () =
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let child =
            async2 {
                let! running = Async2.StartChild(async2 {
                    started.SetResult(())
                    return 42
                })
                do! started.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
                return! running
            }
        Assert.Equal(42, Async2.RunSynchronously child)

    [<Fact>]
    let ``Joining StartChild propagates a sibling failure`` () =
        let join (first: Async2<'T>) (second: Async2<'U>) =
            async2 {
                let! firstChild = Async2.StartChild first
                let! secondChild = Async2.StartChild second
                let! firstResult = firstChild
                let! secondResult = secondChild
                return firstResult, secondResult
            }

        let result =
            try
                Async2.RunSynchronously(
                    join
                        (async2 {
                            do! Async2.Sleep 30
                            failwith "fail"
                            return 6
                        })
                        (async2 {
                            do! Async2.Sleep 30
                            return 4
                        })
                )
            with _ ->
                0, 0

        Assert.Equal((0, 0), result)

    [<Fact>]
    let ``StartChild can be run after its parent returns`` () =
        let child = Async2.StartChild(async2 { return 5 }) |> Async2.RunSynchronously
        Assert.Equal(5, child |> Async2.RunSynchronously)

    [<Fact>]
    let ``Deep StartChild chains do not overflow the stack`` () =
        let rec loop remaining =
            async2 {
                if remaining = 0 then
                    return 0
                else
                    let! child = Async2.StartChild(async2 { return remaining })
                    let! _ = child
                    return! loop (remaining - 1)
            }

        Assert.Equal(0, Async2.RunSynchronously(loop 10_000))

    [<Fact>]
    let ``Recursive computations unwind handlers and finalizers without overflowing the stack`` () =
        let mutable finalized = 0
        let mutable iterations = 0

        let rec loop remaining =
            async2 {
                try
                    iterations <- iterations + 1
                    try
                        do! Task.Yield()
                        if remaining = 1_000 then
                            failwith "boom"
                        if remaining = 0 then
                            return 42
                        else
                            return! loop (remaining - 1)
                    with error when remaining = 20_000 ->
                        return 99
                finally
                    finalized <- finalized + 1
            }

        let result = Async2.RunSynchronously(loop 50_000)

        Assert.Equal(99, result)
        Assert.Equal(iterations, finalized)

    [<Fact>]
    let ``A failed start does not leave queued children pending`` () =
        let mutable firstChildTask: Task<int> = null
        let mutable secondChildTask: Task<int> = null
        let mutable startError: exn option = None
        let thread =
            Thread(ThreadStart(fun () ->
                let parent =
                    Async2<int>(fun ctx ->
                        firstChildTask <- ((async2 { return 42 }).StartTrampolined ctx.CancellationToken).AsTask()
                        secondChildTask <- ((async2 { return 43 }).StartTrampolined ctx.CancellationToken).AsTask()
                        raise (InvalidOperationException("parent start failed")))

                try
                    parent.StartTrampolined CancellationToken.None |> ignore
                with error ->
                    startError <- Some error))

        thread.Start()
        Assert.True(thread.Join(5000), "Parent start did not finish")
        Assert.IsType<InvalidOperationException>(startError.Value) |> ignore
        Assert.NotNull(firstChildTask)
        Assert.NotNull(secondChildTask)
        Assert.True(firstChildTask.IsCompleted, "The first queued child was left pending after the parent failed")
        Assert.True(secondChildTask.IsCompleted, "The second queued child was left pending after the parent failed")
        Assert.Equal(42, firstChildTask.GetAwaiter().GetResult())
        Assert.Equal(43, secondChildTask.GetAwaiter().GetResult())

    [<Fact>]
    let ``Immediate entry points drain nested work in a running trampoline`` () =
        let nested = async2 {
            do! Async2.FromContinuations(fun (success, _, _) -> success())
        }

        let parent = async2 {
            do! Async2.FromContinuations(fun (success, _, _) ->
                let mutable immediate = false
                let mutable continued = false

                Async2.StartImmediate(async2 {
                    do! nested
                    immediate <- true
                })

                Async2.StartWithContinuations(
                    nested,
                    (fun () -> continued <- true),
                    (fun error -> raise error),
                    (fun error -> raise error)
                )

                Assert.True(immediate, "StartImmediate deferred work to the enclosing trampoline")
                Assert.True(continued, "StartWithContinuations deferred work to the enclosing trampoline")
                success())
        }

        Async2.RunSynchronouslyImmediate parent

module AsyncCompatibilityTests =

    [<Fact>]
    let ``RunSynchronously with timeout preserves a computation failure`` () =
        let failure = InvalidOperationException("boom")
        let computation = async2 { return raise failure }

        let actual =
            Assert.Throws<InvalidOperationException>(fun () ->
                Async2.RunSynchronously(computation, timeout = 1000) |> ignore)

        Assert.Same(failure, actual)

    [<Fact>]
    let ``RunSynchronously ignores timeout for an explicit cancellable token`` () =
        use cts = new CancellationTokenSource()
        let computation = async2 {
            do! Async2.Sleep 50
            return 42
        }

        Assert.Equal(42, Async2.RunSynchronously(computation, timeout = 1, cancellationToken = cts.Token))

    [<Fact>]
    let ``RunSynchronously waits for timeout cancellation to unwind`` () =
        use started = new ManualResetEventSlim(false)
        use finished = new ManualResetEventSlim(false)
        let computation = async2 {
            try
                started.Set()
                do! Async2.Sleep 10_000
            finally
                finished.Set()
        }

        Assert.Throws<TimeoutException>(fun () ->
            Async2.RunSynchronously(computation, timeout = 500)) |> ignore
        Assert.True(started.IsSet, "Computation did not start before timeout")
        Assert.True(finished.IsSet, "Timed-out computation was still running")

    [<Fact>]
    let ``Parallel of empty sequence returns empty array`` () =
        Assert.Empty(Async2.RunSynchronously(Async2.Parallel(Seq.empty<Async2<int>>)))

    [<Fact>]
    let ``Parallel waits for started children after parent cancellation`` () =
        use cts = new CancellationTokenSource()
        use started = new ManualResetEventSlim(false)
        use unwinding = new ManualResetEventSlim(false)
        use allowFinish = new ManualResetEventSlim(false)
        use finished = new ManualResetEventSlim(false)
        let child = async2 {
            try
                started.Set()
                do! Async2.Sleep 10_000
            finally
                unwinding.Set()
                allowFinish.Wait(5000) |> ignore
                finished.Set()
        }
        let computation = Async2.Parallel([ child; async2 { do! Async2.Sleep 10_000 } ], 1)
        let running = Async2.StartAsTask(computation, cancellationToken = cts.Token)

        Assert.True(started.Wait(5000), "Child did not start")
        let cancellation = Task.Run(fun () -> cts.Cancel())
        try
            Assert.True(unwinding.Wait(5000), "Child did not begin unwinding")
            Assert.False(running.IsCompleted, "Parallel finished before its started child unwound")
        finally
            allowFinish.Set()

        cancellation.GetAwaiter().GetResult()
        Assert.ThrowsAny<OperationCanceledException>(fun () -> running.GetAwaiter().GetResult() |> ignore)
        |> ignore
        Assert.True(finished.IsSet, "Parallel finished before its started child unwound")

    [<Fact>]
    let ``Choice cancels its siblings when one returns Some`` () =
        use started = new ManualResetEventSlim(false)
        use canceled = new ManualResetEventSlim(false)
        let sibling = async2 {
            let! ct = Async2.CancellationToken
            use _registration = ct.Register(fun () -> canceled.Set())
            started.Set()
            do! Async2.Sleep 10_000
            return None
        }
        let winner = async2 {
            do! Task.Run(fun () -> started.Wait())
            return Some 42
        }

        Assert.Equal(Some 42, Async2.RunSynchronously(Async2.Choice [ sibling; winner ]))
        Assert.True(canceled.Wait(1000), "Choice did not cancel the losing computation")

    let rec private tailCountDown (n: int) : Async2<int> = async2 {
        if n = 0 then return 0 else return! tailCountDown (n - 1)
    }

    let rec private tailEven (n: int) : Async2<bool> = async2 {
        if n = 0 then return true else return! tailOdd (n - 1)
    }
    and private tailOdd (n: int) : Async2<bool> = async2 {
        if n = 0 then return false else return! tailEven (n - 1)
    }

    [<Fact>]
    let ``Deep tail recursion completes`` () =
        Assert.Equal(0, Async2.RunSynchronously(tailCountDown 1_000_000))

    [<Fact>]
    let ``Mutual tail recursion completes`` () =
        Assert.True(Async2.RunSynchronously(tailEven 1_000_000))

    [<Fact>]
    let ``Tail call result flows from the last computation`` () =
        let leaf = async2 { return 42 }
        let middle = async2 { return! leaf }
        let root = async2 { return! middle }
        Assert.Equal(42, Async2.RunSynchronously root)

    [<Fact>]
    let ``Tail call propagates exceptions`` () =
        let failure = InvalidOperationException("boom")
        let leaf = async2 {
            if true then raise failure
            return 0
        }
        let middle = async2 { return! leaf }
        let root = async2 { return! middle }
        let actual = Assert.Throws<InvalidOperationException>(fun () -> Async2.RunSynchronously root |> ignore)
        Assert.Same(failure, actual)

    [<Fact>]
    let ``Tail call inside try-finally runs finally after the callee`` () =
        let log = System.Collections.Generic.List<string>()
        let leaf = async2 {
            log.Add "leaf"
            return 1
        }
        let root = async2 {
            try
                return! leaf
            finally
                log.Add "finally"
        }
        Assert.Equal(1, Async2.RunSynchronously root)
        Assert.Equal<string list>([ "leaf"; "finally" ], List.ofSeq log)

    [<Fact>]
    let ``Tail call inside try-with catches callee exceptions`` () =
        let leaf = async2 {
            if true then failwith "x"
            return 0
        }
        let root = async2 {
            try
                return! leaf
            with _ -> return -1
        }
        Assert.Equal(-1, Async2.RunSynchronously root)

    [<Fact>]
    let ``Tail call through a custom Async2 value works`` () =
        let custom = Async2<int>(fun _ -> ValueTask<int>(7))
        let middle = async2 { return! custom }
        let root = async2 { return! middle }
        Assert.Equal(7, Async2.RunSynchronously root)

    [<Fact>]
    let ``Tail call after awaits in callee and caller works`` () =
        let leaf = async2 {
            do! Async2.Sleep 5
            return 3
        }
        let middle = async2 {
            do! Async2.Sleep 5
            return! leaf
        }
        let root = async2 {
            do! Async2.Sleep 5
            return! middle
        }
        Assert.Equal(3, Async2.RunSynchronously root)

    [<Fact>]
    let ``Tail call observes cancellation`` () =
        use cts = new CancellationTokenSource()
        let leaf = async2 {
            cts.Cancel()
            do! Async2.Sleep 5000
            return 1
        }
        let root = async2 { return! leaf }
        Assert.ThrowsAny<OperationCanceledException>(fun () ->
            Async2.RunSynchronously(root, cancellationToken = cts.Token) |> ignore)
        |> ignore
