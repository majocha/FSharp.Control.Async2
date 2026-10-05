namespace FSharp.Control.Async2.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Microsoft.FSharp.Control

module MailboxProcessor2Tests =

    let private raceReceiveWithPost timeout tryReceive iterations = task {
        use receiveSignal = new AutoResetEvent(false)
        use postSignal = new AutoResetEvent(false)
        use received = new AutoResetEvent(false)
        use stopPoster = new CancellationTokenSource()

        let mailbox =
            MailboxProcessor2<unit>.Start(fun inbox ->
                async2 {
                    let! ct = Async2.CancellationToken
                    let mutable running = true

                    while running do
                        if receiveSignal.WaitOne(50) then
                            if tryReceive then
                                let! _ = inbox.TryReceive(?timeout = timeout)
                                ()
                            else
                                let! _ = inbox.Receive(?timeout = timeout)
                                ()

                            received.Set() |> ignore

                        running <- not ct.IsCancellationRequested
                })

        let poster =
            Task.Run(fun () ->
                while not stopPoster.IsCancellationRequested do
                    postSignal.WaitOne() |> ignore
                    mailbox.Post())

        for i in 0 .. iterations - 1 do
            if i % 2 = 0 then
                receiveSignal.Set() |> ignore
                postSignal.Set() |> ignore
            else
                postSignal.Set() |> ignore
                receiveSignal.Set() |> ignore

            Assert.True(received.WaitOne(TimeSpan.FromSeconds 2.0), "A receive did not complete.")

        stopPoster.Cancel()
        postSignal.Set() |> ignore
        do! poster
        mailbox.Dispose()
    }

    [<Fact>]
    let ``Receive and TryReceive preserve message order and support timeout`` () =
        let mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        mailbox.Post 1
        mailbox.Post 2

        Assert.Equal(1, Async2.RunSynchronously(mailbox.Receive()))
        Assert.Equal(Some 2, Async2.RunSynchronously(mailbox.TryReceive(timeout = 100)))
        Assert.Equal(None, Async2.RunSynchronously(mailbox.TryReceive(timeout = 10)))

    [<Fact>]
    let ``Receive races safely with Post`` () =
        raceReceiveWithPost None false 10_000

    [<Fact>]
    let ``Receive with timeout races safely with Post`` () =
        raceReceiveWithPost (Some 5_000) false 2_000

    [<Fact>]
    let ``TryReceive with timeout races safely with Post`` () =
        raceReceiveWithPost (Some 5_000) true 2_000

    [<Fact>]
    let ``Scan leaves skipped messages in their original order`` () =
        let mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        [ 1; 2; 3 ] |> List.iter mailbox.Post

        let matched =
            mailbox.TryScan(
                (fun message ->
                    if message = 2 then Some(async2 { return message })
                    else None),
                timeout = 100
            )
            |> Async2.RunSynchronously

        Assert.Equal(Some 2, matched)
        Assert.Equal(2, mailbox.CurrentQueueLength)
        Assert.Equal(1, Async2.RunSynchronously(mailbox.Receive()))
        Assert.Equal(3, Async2.RunSynchronously(mailbox.Receive()))

    [<Fact>]
    let ``TryScan honors default timeout and keeps unmatched messages`` () =
        let mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        mailbox.DefaultTimeout <- 10
        mailbox.Post 1

        let result =
            mailbox.TryScan((fun _ -> None))
            |> Async2.RunSynchronously

        Assert.Equal(None, result)
        Assert.Equal(1, mailbox.CurrentQueueLength)
        Assert.Equal(1, Async2.RunSynchronously(mailbox.Receive()))

    [<Fact>]
    let ``PostAndAsyncReply exchanges messages using Async2`` () =
        let mailbox =
            MailboxProcessor2<int * AsyncReplyChannel2<int>>.Start(fun inbox ->
                let rec loop () =
                    async2 {
                        let! value, reply = inbox.Receive()
                        reply.Reply(value * 2)
                        return! loop ()
                    }

                loop ())

        let result =
            mailbox.PostAndAsyncReply((fun reply -> 21, reply), timeout = 1000)
            |> Async2.RunSynchronously

        Assert.Equal(42, result)
        mailbox.Dispose()

    [<Fact>]
    let ``PostAndReply synchronously returns a reply`` () =
        let mailbox =
            MailboxProcessor2<AsyncReplyChannel2<int>>.Start(fun inbox ->
                async2 {
                    let! reply = inbox.Receive()
                    reply.Reply 42
                })

        Assert.Equal(42, mailbox.PostAndReply id)
        mailbox.Dispose()

    [<Fact>]
    let ``PostAndTryAsyncReply returns None when the reply times out`` () =
        let mailbox = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })

        let result =
            mailbox.PostAndTryAsyncReply(id, timeout = 10)
            |> Async2.RunSynchronously

        Assert.Equal(None, result)

    [<Fact>]
    let ``StartImmediate begins the body on the caller thread`` () =
        let callerThread = Thread.CurrentThread.ManagedThreadId
        let startedOn = TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously)

        let mailbox =
            MailboxProcessor2<AsyncReplyChannel2<int>>.StartImmediate(fun inbox ->
                async2 {
                    startedOn.TrySetResult(Thread.CurrentThread.ManagedThreadId) |> ignore
                    let! reply = inbox.Receive()
                    reply.Reply callerThread
                })

        let startedThread = startedOn.Task.GetAwaiter().GetResult()
        Assert.Equal(callerThread, startedThread)
        mailbox.Dispose()

    [<Fact>]
    let ``Cancellation token interrupts a pending receive`` () = task {
        use cancellation = new CancellationTokenSource()
        let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let canceled = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let mailbox =
            MailboxProcessor2<int>.Start(
                (fun inbox ->
                    async2 {
                        started.TrySetResult(()) |> ignore
                        try
                            let! _ = inbox.Receive()
                            ()
                        finally
                            canceled.TrySetResult(()) |> ignore
                    }),
                cancellationToken = cancellation.Token
            )

        do! started.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
        cancellation.Cancel()
        do! canceled.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
        mailbox.Dispose()
    }

    [<Fact>]
    let ``Dispose clears pending messages and supports throwing posts`` () =
        let mailbox =
            new MailboxProcessor2<int>(
                (fun _ -> async2 { return () }),
                throwOnPostAfterDispose = true
            )

        mailbox.Post 1
        mailbox.Dispose()

        Assert.Equal(0, mailbox.CurrentQueueLength)
        Assert.Throws<ObjectDisposedException>(fun () -> mailbox.Post 2)

    [<Fact>]
    let ``Disposing a running mailbox stops processing and permits later posts`` () = task {
        let reachedSecond = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let releaseBody = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let ended = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let mutable processed = 0

        let mailbox =
            MailboxProcessor2<int>.Start(fun inbox ->
                let rec loop () =
                    async2 {
                        let! _ = inbox.Receive()
                        processed <- processed + 1

                        if processed = 2 then
                            reachedSecond.TrySetResult(()) |> ignore
                            do! releaseBody.Task

                        return! loop ()
                    }

                async2 {
                    try
                        do! loop ()
                    finally
                        ended.TrySetResult(()) |> ignore
                })

        [ 1; 2; 3; 4 ] |> List.iter mailbox.Post
        do! reachedSecond.Task.WaitAsync(TimeSpan.FromSeconds 2.0)

        mailbox.Dispose()
        releaseBody.TrySetResult(()) |> ignore
        do! ended.Task.WaitAsync(TimeSpan.FromSeconds 2.0)

        Assert.Equal(2, processed)
        Assert.Equal(0, mailbox.CurrentQueueLength)
        mailbox.Post 5
    }
