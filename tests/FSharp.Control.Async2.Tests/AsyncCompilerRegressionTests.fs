namespace FSharp.Control.Async2.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Microsoft.FSharp.Control

module AsyncCompilerRegressionTests =

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
    let ``A failed start does not leave queued children pending`` () =
        let mutable firstChildTask: Task<int> = null
        let mutable secondChildTask: Task<int> = null
        let mutable startError: exn option = None
        let thread =
            Thread(ThreadStart(fun () ->
                let parent =
                    Async2<int>(fun ct ->
                        firstChildTask <- (async2 { return 42 }).StartTrampolined ct
                        secondChildTask <- (async2 { return 43 }).StartTrampolined ct
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
