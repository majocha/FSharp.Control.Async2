namespace FSharp.Control.Async2.Tests

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Microsoft.FSharp.Control

module MailboxProcessor2Tests =

    type private MailboxOperations =
        {
            Post: int -> unit
            Length: unit -> int
            Receive: int -> int
            TryReceive: int -> int option
            TryScan: (int -> int option) -> int -> int option
            Dispose: unit -> unit
        }

    let private compareMailboxes (exercise: MailboxOperations -> 'T) =
        use original = new MailboxProcessor<int>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        let originalResult =
            exercise {
                Post = original.Post
                Length = fun () -> original.CurrentQueueLength
                Receive = fun timeout -> Async.RunSynchronously(original.Receive(timeout = timeout))
                TryReceive = fun timeout -> Async.RunSynchronously(original.TryReceive(timeout = timeout))
                TryScan = fun scanner timeout ->
                    original.TryScan(
                        (fun message -> scanner message |> Option.map (fun value -> async { return value })),
                        timeout = timeout
                    )
                    |> Async.RunSynchronously
                Dispose = original.Dispose
            }

        let replacementResult =
            exercise {
                Post = replacement.Post
                Length = fun () -> replacement.CurrentQueueLength
                Receive = fun timeout -> Async2.RunSynchronously(replacement.Receive(timeout = timeout))
                TryReceive = fun timeout -> Async2.RunSynchronously(replacement.TryReceive(timeout = timeout))
                TryScan = fun scanner timeout ->
                    replacement.TryScan(
                        (fun message -> scanner message |> Option.map (fun value -> async2 { return value })),
                        timeout = timeout
                    )
                    |> Async2.RunSynchronously
                Dispose = replacement.Dispose
            }

        Assert.Equal<'T>(originalResult, replacementResult)

    let private replyOutcome (pending: Task<'T>) = task {
        try
            let! reply = pending.WaitAsync(TimeSpan.FromSeconds 2.0)
            return Some reply
        with :? OperationCanceledException ->
            return None
    }

    let private raceReceiveWithPost timeout tryReceive iterations = task {
        use receiveSignal = new AutoResetEvent(false)
        use postSignal = new AutoResetEvent(false)
        use received = new AutoResetEvent(false)
        use stopPoster = new CancellationTokenSource()
        use stopMailbox = new CancellationTokenSource()
        let ended = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let mailbox =
            MailboxProcessor2<unit>.Start(
                (fun inbox ->
                    async2 {
                        try
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
                        finally
                            ended.TrySetResult(()) |> ignore
                    }),
                cancellationToken = stopMailbox.Token
            )

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
        stopMailbox.Cancel()
        do! ended.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
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

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Expired waits do not steal later receive signals`` scan = task {
        use mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })

        for _ in 1 .. 3 do
            let operation =
                if scan then
                    mailbox.TryScan((fun message -> Some(async2 { return message })), timeout = 10)
                else
                    mailbox.TryReceive(timeout = 10)

            let! result = Async2.StartAsTask operation
            Assert.Equal(None, result)

        let pending = Async2.StartImmediateAsTask(mailbox.TryReceive(timeout = 1000))
        mailbox.Post 42
        let! result = pending.WaitAsync(TimeSpan.FromSeconds 2.0)
        Assert.Equal(Some 42, result)
        Assert.Equal(0, mailbox.CurrentQueueLength)
    }

    [<Fact>]
    let ``Receive with zero timeout consumes queued messages or throws`` () =
        use mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        mailbox.Post 42
        Assert.Equal(42, Async2.RunSynchronously(mailbox.Receive(timeout = 0)))
        Assert.Throws<TimeoutException>(fun () ->
            Async2.RunSynchronously(mailbox.Receive(timeout = 0)) |> ignore)

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
    let ``Repeated scans preserve deferred messages before new arrivals`` () =
        use mailbox = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        [ 1; 2; 3 ] |> List.iter mailbox.Post

        let scan target =
            mailbox.Scan(
                (fun message ->
                    if message = target then Some(async2 { return message })
                    else None),
                timeout = 0
            )
            |> Async2.RunSynchronously

        Assert.Equal(2, scan 2)
        mailbox.Post 4
        Assert.Equal(3, scan 3)
        mailbox.Post 5
        Assert.Equal(5, scan 5)
        Assert.Equal(2, mailbox.CurrentQueueLength)
        Assert.Equal(1, Async2.RunSynchronously(mailbox.Receive()))
        Assert.Equal(4, Async2.RunSynchronously(mailbox.Receive()))

    [<Fact>]
    let ``Canceling a scan retains skipped messages`` () = task {
        use cancellation = new CancellationTokenSource()
        use mailbox =
            new MailboxProcessor2<int>(
                (fun _ -> async2 { return () }),
                cancellationToken = cancellation.Token
            )
        [ 1; 2 ] |> List.iter mailbox.Post

        let pending =
            Async2.StartImmediateAsTask(
                mailbox.TryScan(fun _ -> None : Async2<int> option),
                cancellationToken = cancellation.Token
            )

        cancellation.Cancel()
        let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> pending :> Task)
        mailbox.Post 3
        Assert.Equal(3, mailbox.CurrentQueueLength)
        Assert.Equal(1, Async2.RunSynchronously(mailbox.Receive()))
        Assert.Equal(2, Async2.RunSynchronously(mailbox.Receive()))
        Assert.Equal(3, Async2.RunSynchronously(mailbox.Receive()))
    }

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

    [<Theory>]
    [<InlineData(-1)>]
    [<InlineData(5000)>]
    let ``Canceling a reply wait does not require a reply`` timeout = task {
        use cancellation = new CancellationTokenSource()
        use mailbox =
            new MailboxProcessor2<AsyncReplyChannel2<int>>(
                (fun _ -> async2 { return () }),
                cancellationToken = cancellation.Token
            )

        let pending =
            Async2.StartImmediateAsTask(
                mailbox.PostAndTryAsyncReply(id, timeout = timeout),
                cancellationToken = cancellation.Token
            )

        let reply = Async2.RunSynchronously(mailbox.Receive(timeout = 0))
        cancellation.Cancel()

        try
            let! _ =
                Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                    pending.WaitAsync(TimeSpan.FromSeconds 2.0) :> Task)
            Assert.True(pending.IsCanceled)
        finally
            reply.Reply 42
    }

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
                isThrowExceptionAfterDisposed = true
            )

        mailbox.Post 1
        mailbox.Dispose()

        Assert.Equal(0, mailbox.CurrentQueueLength)
        Assert.Throws<ObjectDisposedException>(fun () -> mailbox.Post 2)

    [<Fact>]
    let ``Dispose clears messages without canceling the body like the original`` () = task {
        let releaseBody = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        let originalEnded = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
        let replacementEnded = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
        use original =
            MailboxProcessor<int>.StartImmediate(fun _ ->
                async {
                    let! ct = Async.CancellationToken
                    do! Async.AwaitTask releaseBody.Task
                    originalEnded.TrySetResult(ct.IsCancellationRequested) |> ignore
                })
        use replacement =
            MailboxProcessor2<int>.StartImmediate(fun _ ->
                async2 {
                    let! ct = Async2.CancellationToken
                    do! releaseBody.Task
                    replacementEnded.TrySetResult(ct.IsCancellationRequested) |> ignore
                })

        [ 1; 2 ] |> List.iter original.Post
        [ 1; 2 ] |> List.iter replacement.Post
        original.Dispose()
        replacement.Dispose()
        original.Post 3
        replacement.Post 3
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
        Assert.Equal(0, replacement.CurrentQueueLength)
        releaseBody.TrySetResult(()) |> ignore
        let! originalCanceled = originalEnded.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
        let! replacementCanceled = replacementEnded.Task.WaitAsync(TimeSpan.FromSeconds 2.0)
        Assert.Equal(originalCanceled, replacementCanceled)
        Assert.False(replacementCanceled)
    }

    [<Fact>]
    let ``Scanning and receiving match original ordering and queue lengths`` () =
        compareMailboxes (fun mailbox ->
            [ 1; 2; 3 ] |> List.iter mailbox.Post
            let first = mailbox.TryScan (fun message -> if message = 2 then Some message else None) 0
            mailbox.Post 4
            let second = mailbox.TryScan (fun message -> if message = 3 then Some message else None) 0
            let lengths = ResizeArray<int>()
            let unmatched = mailbox.TryScan (fun _ -> lengths.Add(mailbox.Length()); None) 0
            let count = mailbox.Length()
            let remaining = [ mailbox.Receive 0; mailbox.Receive 0 ]
            first, second, unmatched, List.ofSeq lengths, count, remaining, mailbox.TryReceive 0)

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Scanner exceptions match original inbox and arrival semantics`` retained =
        compareMailboxes (fun mailbox ->
            [ 1; 2; 3 ] |> List.iter mailbox.Post
            if retained then mailbox.TryScan (fun _ -> None) 0 |> ignore

            Assert.Throws<InvalidOperationException>(fun () ->
                mailbox.TryScan
                    (fun message ->
                        if message = 2 then raise (InvalidOperationException("scanner"))
                        None)
                    0
                |> ignore)
            |> ignore

            let count = mailbox.Length()
            count, List.init count (fun _ -> mailbox.Receive 0))

    [<Fact>]
    let ``Waiting scans retain messages without rescanning them`` () = task {
        use original = new MailboxProcessor<int>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        let originalVisits = ResizeArray<int>()
        let replacementVisits = ResizeArray<int>()
        [ 1; 2 ] |> List.iter original.Post
        [ 1; 2 ] |> List.iter replacement.Post
        let originalPending =
            original.TryScan(fun message ->
                originalVisits.Add message
                if message = 3 then Some(async { return message }) else None)
            |> Async.StartImmediateAsTask
        let replacementPending =
            replacement.TryScan(fun message ->
                replacementVisits.Add message
                if message = 3 then Some(async2 { return message }) else None)
            |> Async2.StartImmediateAsTask

        Assert.False(originalPending.IsCompleted)
        Assert.False(replacementPending.IsCompleted)
        Assert.Equal(2, replacement.CurrentQueueLength)
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
        original.Post 3
        replacement.Post 3
        let! originalResult = originalPending.WaitAsync(TimeSpan.FromSeconds 2.0)
        let! replacementResult = replacementPending.WaitAsync(TimeSpan.FromSeconds 2.0)
        Assert.Equal(originalResult, replacementResult)
        Assert.Equal<int list>(List.ofSeq originalVisits, List.ofSeq replacementVisits)
        Assert.Equal<int list>([ 1; 2; 3 ], List.ofSeq replacementVisits)
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
    }

    [<Fact>]
    let ``Selected scan computations leave unmatched messages visible`` () = task {
        use original = new MailboxProcessor<int>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
        [ 1; 2; 3 ] |> List.iter original.Post
        [ 1; 2; 3 ] |> List.iter replacement.Post
        let originalPending =
            original.Scan(fun message ->
                if message = 2 then
                    Some(async {
                        do! Async.AwaitTask release.Task
                        return message
                    })
                else None)
            |> Async.StartImmediateAsTask
        let replacementPending =
            replacement.Scan(fun message ->
                if message = 2 then
                    Some(async2 {
                        do! release.Task
                        return message
                    })
                else None)
            |> Async2.StartImmediateAsTask

        try
            Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
            Assert.Equal(2, replacement.CurrentQueueLength)
        finally
            release.TrySetResult(()) |> ignore

        let! originalResult = originalPending.WaitAsync(TimeSpan.FromSeconds 2.0)
        let! replacementResult = replacementPending.WaitAsync(TimeSpan.FromSeconds 2.0)
        Assert.Equal(originalResult, replacementResult)
        Assert.Equal(1, Async2.RunSynchronously(replacement.Receive(timeout = 0)))
        Assert.Equal(3, Async2.RunSynchronously(replacement.Receive(timeout = 0)))
    }

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Async reply methods post eagerly once and keep the first reply`` tryReply =
        use original = new MailboxProcessor<AsyncReplyChannel<int>>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        let originalReply =
            if tryReply then original.PostAndTryAsyncReply id
            else
                let reply = original.PostAndAsyncReply id
                async {
                    let! value = reply
                    return Some value
                }
        let replacementReply =
            if tryReply then replacement.PostAndTryAsyncReply id
            else
                let reply = replacement.PostAndAsyncReply id
                async2 {
                    let! value = reply
                    return Some value
                }

        Assert.Equal(1, original.CurrentQueueLength)
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
        let originalChannel = Async.RunSynchronously(original.Receive(timeout = 0))
        let replacementChannel = Async2.RunSynchronously(replacement.Receive(timeout = 0))
        originalChannel.Reply 42
        originalChannel.Reply 99
        replacementChannel.Reply 42
        replacementChannel.Reply 99
        for _ in 1 .. 2 do
            Assert.Equal(Async.RunSynchronously originalReply, Async2.RunSynchronously replacementReply)
        Assert.Equal(0, replacement.CurrentQueueLength)

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Async reply builders throw at method invocation like the original`` tryReply =
        use original = new MailboxProcessor<AsyncReplyChannel<int>>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        let fail _ = raise (InvalidOperationException("buildMessage"))
        Assert.Throws<InvalidOperationException>(fun () ->
            if tryReply then original.PostAndTryAsyncReply fail |> ignore
            else original.PostAndAsyncReply fail |> ignore)
        |> ignore
        Assert.Throws<InvalidOperationException>(fun () ->
            if tryReply then replacement.PostAndTryAsyncReply fail |> ignore
            else replacement.PostAndAsyncReply fail |> ignore)
        |> ignore
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)

    [<Fact>]
    let ``Timed out reply workflows drop late replies and dispose their cells`` () =
        use original = new MailboxProcessor<AsyncReplyChannel<int>>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        let originalReply = original.PostAndTryAsyncReply(id, timeout = 0)
        let replacementReply = replacement.PostAndTryAsyncReply(id, timeout = 0)
        Assert.Equal(Async.RunSynchronously originalReply, Async2.RunSynchronously replacementReply)
        Async.RunSynchronously(original.Receive(timeout = 0)).Reply 42
        Async2.RunSynchronously(replacement.Receive(timeout = 0)).Reply 42
        Assert.Throws<ObjectDisposedException>(fun () -> Async.RunSynchronously originalReply |> ignore)
        |> ignore
        Assert.Throws<ObjectDisposedException>(fun () -> Async2.RunSynchronously replacementReply |> ignore)
        |> ignore

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Infinite reply cancellation matches constructor token support`` supported = task {
        use cancellation = new CancellationTokenSource()
        let token = if supported then Some cancellation.Token else None
        use original =
            new MailboxProcessor<AsyncReplyChannel<int>>(
                (fun _ -> async { return () }), ?cancellationToken = token)
        use replacement =
            new MailboxProcessor2<AsyncReplyChannel2<int>>(
                (fun _ -> async2 { return () }), ?cancellationToken = token)
        let originalPending =
            Async.StartImmediateAsTask(original.PostAndTryAsyncReply id, cancellationToken = cancellation.Token)
        let replacementPending =
            Async2.StartImmediateAsTask(replacement.PostAndTryAsyncReply id, cancellationToken = cancellation.Token)
        let originalChannel = Async.RunSynchronously(original.Receive(timeout = 0))
        let replacementChannel = Async2.RunSynchronously(replacement.Receive(timeout = 0))
        cancellation.Cancel()

        if supported then
            let! originalResult = replyOutcome originalPending
            let! replacementResult = replyOutcome replacementPending
            Assert.Equal(originalResult, replacementResult)
            Assert.True(replacementPending.IsCanceled)
        else
            Assert.False(originalPending.IsCompleted)
            Assert.False(replacementPending.IsCompleted)
            originalChannel.Reply 42
            replacementChannel.Reply 42
            let! originalResult = replyOutcome originalPending
            let! replacementResult = replyOutcome replacementPending
            Assert.Equal(originalResult, replacementResult)
    }

    [<Fact>]
    let ``Receive and scan workflows capture the default timeout at invocation`` () =
        use original = new MailboxProcessor<int>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        original.DefaultTimeout <- 0
        replacement.DefaultTimeout <- 0
        let originalReceive = original.TryReceive()
        let replacementReceive = replacement.TryReceive()
        let originalScan = original.Scan(fun _ -> None : Async<int> option)
        let replacementScan = replacement.Scan(fun _ -> None : Async2<int> option)
        original.DefaultTimeout <- 5000
        replacement.DefaultTimeout <- 5000
        Assert.Equal(Async.RunSynchronously originalReceive, Async2.RunSynchronously replacementReceive)
        Assert.Throws<TimeoutException>(fun () -> Async.RunSynchronously originalScan |> ignore) |> ignore
        Assert.Throws<TimeoutException>(fun () -> Async2.RunSynchronously replacementScan |> ignore) |> ignore

    [<Fact>]
    let ``Receive after disposal matches an empty original mailbox`` () =
        compareMailboxes (fun mailbox ->
            mailbox.Post 1
            mailbox.Dispose()
            mailbox.Post 2
            mailbox.Length(), mailbox.TryReceive 0)

    [<Fact>]
    let ``A rejecting inbox scanner can dispose the mailbox like the original`` () =
        compareMailboxes (fun mailbox ->
            [ 1; 2; 3 ] |> List.iter mailbox.Post
            mailbox.TryScan (fun _ -> None) 0 |> ignore
            let result = mailbox.TryScan (fun _ -> mailbox.Dispose(); None) 0
            result, mailbox.Length())

    [<Fact>]
    let ``Starting after disposal and rejecting duplicate starts match the original`` () =
        let mutable originalStarted = false
        let mutable replacementStarted = false
        use original = new MailboxProcessor<int>(fun _ -> async { originalStarted <- true })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { replacementStarted <- true })
        original.Dispose()
        replacement.Dispose()
        original.StartImmediate()
        replacement.StartImmediate()
        Assert.True(originalStarted)
        Assert.Equal(originalStarted, replacementStarted)
        Assert.Throws<InvalidOperationException>(fun () -> original.Start()) |> ignore
        Assert.Throws<InvalidOperationException>(fun () -> replacement.Start()) |> ignore

    [<Fact>]
    let ``Body exceptions are published to Error like the original`` () =
        let originalErrors = ResizeArray<Type>()
        let replacementErrors = ResizeArray<Type>()
        use original =
            new MailboxProcessor<int>(fun _ -> async { return raise (InvalidOperationException("body")) })
        use replacement =
            new MailboxProcessor2<int>(fun _ -> async2 { return raise (InvalidOperationException("body")) })
        original.Error.Add(fun error -> originalErrors.Add(error.GetType()))
        replacement.Error.Add(fun error -> replacementErrors.Add(error.GetType()))
        original.StartImmediate()
        replacement.StartImmediate()
        Assert.Equal<Type list>(List.ofSeq originalErrors, List.ofSeq replacementErrors)
        Assert.Single(replacementErrors) |> ignore

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Scan timeout does not time out the selected computation`` retained =
        use original = new MailboxProcessor<int>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<int>(fun _ -> async2 { return () })
        original.Post 42
        replacement.Post 42
        if retained then
            Async.RunSynchronously(original.TryScan((fun _ -> None : Async<int> option), timeout = 0)) |> ignore
            Async2.RunSynchronously(replacement.TryScan((fun _ -> None : Async2<int> option), timeout = 0)) |> ignore

        let originalResult =
            original.Scan(
                (fun message -> Some(async {
                    do! Async.Sleep 20
                    return message
                })),
                timeout = 1
            )
            |> Async.RunSynchronously
        let replacementResult =
            replacement.Scan(
                (fun message -> Some(async2 {
                    do! Async2.Sleep 20
                    return message
                })),
                timeout = 1
            )
            |> Async2.RunSynchronously
        Assert.Equal(originalResult, replacementResult)

    [<Fact>]
    let ``Synchronous replies ignore constructor cancellation and accept already available results`` () =
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()
        use original =
            new MailboxProcessor<AsyncReplyChannel<int>>(
                (fun _ -> async { return () }), cancellationToken = cancellation.Token)
        use replacement =
            new MailboxProcessor2<AsyncReplyChannel2<int>>(
                (fun _ -> async2 { return () }), cancellationToken = cancellation.Token)
        let originalReply =
            original.TryPostAndReply(
                (fun reply -> reply.Reply 42; reply.Reply 99; reply), timeout = -2)
        let replacementReply =
            replacement.TryPostAndReply(
                (fun reply -> reply.Reply 42; reply.Reply 99; reply), timeout = -2)
        Assert.Equal(originalReply, replacementReply)
        Assert.Equal(Some 42, replacementReply)

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Static start overloads accept the original disposal parameter name`` immediate =
        use original =
            if immediate then
                MailboxProcessor<int>.StartImmediate(
                    (fun _ -> async { return () }), isThrowExceptionAfterDisposed = true)
            else
                MailboxProcessor<int>.Start(
                    (fun _ -> async { return () }), isThrowExceptionAfterDisposed = true)
        use replacement =
            if immediate then
                MailboxProcessor2<int>.StartImmediate(
                    (fun _ -> async2 { return () }), isThrowExceptionAfterDisposed = true)
            else
                MailboxProcessor2<int>.Start(
                    (fun _ -> async2 { return () }), isThrowExceptionAfterDisposed = true)
        original.Dispose()
        replacement.Dispose()
        let originalError = Assert.Throws<ObjectDisposedException>(fun () -> original.Post 1)
        let replacementError = Assert.Throws<ObjectDisposedException>(fun () -> replacement.Post 1)
        Assert.Equal(originalError.ObjectName, replacementError.ObjectName)

    [<Theory>]
    [<InlineData(-1)>]
    [<InlineData(0)>]
    [<InlineData(1000)>]
    let ``Successful disposable reply workflows cannot be reused like the original`` timeout =
        use original =
            new MailboxProcessor<AsyncReplyChannel<int>>(
                (fun _ -> async { return () }), cancellationToken = CancellationToken.None)
        use replacement =
            new MailboxProcessor2<AsyncReplyChannel2<int>>(
                (fun _ -> async2 { return () }), cancellationToken = CancellationToken.None)
        let originalReply =
            original.PostAndTryAsyncReply((fun reply -> reply.Reply 42; reply), timeout = timeout)
        let replacementReply =
            replacement.PostAndTryAsyncReply((fun reply -> reply.Reply 42; reply), timeout = timeout)
        Assert.Equal(Async.RunSynchronously originalReply, Async2.RunSynchronously replacementReply)
        Assert.Throws<ObjectDisposedException>(fun () -> Async.RunSynchronously originalReply |> ignore)
        |> ignore
        Assert.Throws<ObjectDisposedException>(fun () -> Async2.RunSynchronously replacementReply |> ignore)
        |> ignore

    [<Theory>]
    [<InlineData(false)>]
    [<InlineData(true)>]
    let ``Invalid asynchronous reply timeouts match the original even with an available reply`` ready =
        use original = new MailboxProcessor<AsyncReplyChannel<int>>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        let originalReply =
            original.PostAndTryAsyncReply(
                (fun reply ->
                    if ready then reply.Reply 42
                    reply), timeout = -2)
        let replacementReply =
            replacement.PostAndTryAsyncReply(
                (fun reply ->
                    if ready then reply.Reply 42
                    reply), timeout = -2)
        Assert.Equal(original.CurrentQueueLength, replacement.CurrentQueueLength)
        Assert.Throws<ArgumentOutOfRangeException>(fun () -> Async.RunSynchronously originalReply |> ignore)
        |> ignore
        Assert.Throws<ArgumentOutOfRangeException>(fun () -> Async2.RunSynchronously replacementReply |> ignore)
        |> ignore

    [<Fact>]
    let ``Concurrent replies retain exactly one result like the original`` () =
        use original = new MailboxProcessor<AsyncReplyChannel<int>>(fun _ -> async { return () })
        use replacement = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        let originalReply = original.PostAndAsyncReply id
        let replacementReply = replacement.PostAndAsyncReply id
        let originalChannel = Async.RunSynchronously(original.Receive(timeout = 0))
        let replacementChannel = Async2.RunSynchronously(replacement.Receive(timeout = 0))
        Parallel.For(0, 64, fun value -> originalChannel.Reply value) |> ignore
        Parallel.For(0, 64, fun value -> replacementChannel.Reply value) |> ignore
        let originalResult = Async.RunSynchronously originalReply
        let replacementResult = Async2.RunSynchronously replacementReply
        Assert.InRange(originalResult, 0, 63)
        Assert.InRange(replacementResult, 0, 63)
        originalChannel.Reply -1
        replacementChannel.Reply -1
        Assert.Equal(originalResult, Async.RunSynchronously originalReply)
        Assert.Equal(replacementResult, Async2.RunSynchronously replacementReply)

    [<Fact>]
    let ``Replies racing a timeout cannot revive a disposed reply workflow`` () = task {
        use mailbox = new MailboxProcessor2<AsyncReplyChannel2<int>>(fun _ -> async2 { return () })
        for _ in 1 .. 100 do
            let reply = mailbox.PostAndTryAsyncReply(id, timeout = 0)
            let channel = Async2.RunSynchronously(mailbox.Receive(timeout = 0))
            let poster = Task.Run(fun () -> channel.Reply 42)
            let! result = Async2.StartImmediateAsTask reply
            Assert.True(result.IsNone || result = Some 42)
            do! poster.WaitAsync(TimeSpan.FromSeconds 2.0)
            channel.Reply 99
            Assert.Throws<ObjectDisposedException>(fun () -> Async2.RunSynchronously reply |> ignore)
            |> ignore
    }
