namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

/// A reply channel for MailboxProcessor2.PostAndAsyncReply.
[<Sealed>]
type AsyncReplyChannel2<'Reply>(reply: 'Reply -> unit) =
    member _.Reply(value: 'Reply) = reply value

[<Sealed>]
type private MailboxProcessor2Queue<'Msg>(throwOnPostAfterDispose: bool) =
    let gate = obj ()
    let arrivals = Queue<'Msg>()
    let deferred = Queue<'Msg>()
    let signal =
        Channel.CreateBounded<unit>(
            BoundedChannelOptions(1, SingleReader = false, SingleWriter = false)
        )
    let mutable disposed = false

    member _.Count =
        lock gate (fun () -> arrivals.Count + deferred.Count)

    member _.Post(message: 'Msg) =
        lock gate (fun () ->
            if disposed then
                if throwOnPostAfterDispose then
                    raise (ObjectDisposedException("MailboxProcessor2"))
            else
                arrivals.Enqueue message
                signal.Writer.TryWrite(()) |> ignore)

    member _.TryTake() =
        lock gate (fun () ->
            if disposed then
                raise (ObjectDisposedException("MailboxProcessor2"))

            if deferred.Count > 0 then
                Some(deferred.Dequeue())
            elif arrivals.Count > 0 then
                Some(arrivals.Dequeue())
            else
                None)

    member _.Restore(messages: ResizeArray<'Msg>) =
        if messages.Count > 0 then
            lock gate (fun () ->
                if not disposed then
                    let restored = Queue<'Msg>(messages.Count + deferred.Count)
                    for message in messages do
                        restored.Enqueue message
                    while deferred.Count > 0 do
                        restored.Enqueue(deferred.Dequeue())
                    while arrivals.Count > 0 do
                        restored.Enqueue(arrivals.Dequeue())
                    while restored.Count > 0 do
                        deferred.Enqueue(restored.Dequeue()))

    member _.Wait(timeout: int) : Async2<bool> =
        async2 {
            let! ct = Async2.CancellationToken
            let read = signal.Reader.ReadAsync(ct).AsTask()

            try
                if timeout < 0 then
                    do! read
                else
                    do! read.WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)

                return true
            with
            | :? TimeoutException -> return false
            | :? ChannelClosedException ->
                return raise (ObjectDisposedException("MailboxProcessor2"))
        }

    member _.Dispose() =
        lock gate (fun () ->
            if not disposed then
                disposed <- true
                arrivals.Clear()
                deferred.Clear()
                signal.Writer.TryWrite(()) |> ignore
                signal.Writer.TryComplete() |> ignore)

[<Sealed; AutoSerializable(false); CompiledName("FSharpMailboxProcessor2`1")>]
type MailboxProcessor2<'Msg> private
    (
        body: MailboxProcessor2<'Msg> -> Async2<unit>,
        options: bool * CancellationToken
    ) =

    let throwOnPostAfterDispose, cancellationToken = options
    let mailbox = MailboxProcessor2Queue<'Msg>(throwOnPostAfterDispose)
    let lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
    let errorEvent = Event<Exception>()
    let gate = obj ()
    let mutable defaultTimeout = Timeout.Infinite
    let mutable started = false
    let mutable bodyCompleted = false
    let mutable disposed = false

    new(body: MailboxProcessor2<'Msg> -> Async2<unit>, ?cancellationToken: CancellationToken) =
        new MailboxProcessor2<'Msg>(
            body,
            (false, defaultArg cancellationToken Async2.DefaultCancellationToken)
        )

    new
        (
            body: MailboxProcessor2<'Msg> -> Async2<unit>,
            throwOnPostAfterDispose: bool,
            ?cancellationToken: CancellationToken
        ) =
        new MailboxProcessor2<'Msg>(
            body,
            (throwOnPostAfterDispose, defaultArg cancellationToken Async2.DefaultCancellationToken)
        )

    member _.CurrentQueueLength = mailbox.Count

    member _.DefaultTimeout
        with get () = defaultTimeout
        and set value = defaultTimeout <- value

    [<CLIEvent>]
    member _.Error = errorEvent.Publish

    member _.Post(message: 'Msg) = mailbox.Post message

    member _.Receive(?timeout: int) : Async2<'Msg> =
        let timeout = defaultArg timeout defaultTimeout
        async2 {
            let startedAt = Stopwatch.GetTimestamp()
            let mutable received = ValueNone

            while received.IsNone do
                match mailbox.TryTake() with
                | Some message -> received <- ValueSome message
                | None ->
                    let remaining =
                        if timeout < 0 then
                            Timeout.Infinite
                        else
                            max 0 (timeout - int (Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds))

                    let! signaled = mailbox.Wait remaining
                    if not signaled then
                        return raise (TimeoutException("The mailbox receive operation timed out."))

            return received.Value
        }

    member _.TryReceive(?timeout: int) : Async2<'Msg option> =
        let timeout = defaultArg timeout defaultTimeout
        async2 {
            let startedAt = Stopwatch.GetTimestamp()
            let mutable received = ValueNone
            let mutable timedOut = false

            while received.IsNone && not timedOut do
                match mailbox.TryTake() with
                | Some message -> received <- ValueSome message
                | None ->
                    let remaining =
                        if timeout < 0 then
                            Timeout.Infinite
                        else
                            max 0 (timeout - int (Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds))

                    if remaining = 0 then
                        timedOut <- true
                    else
                        let! signaled = mailbox.Wait remaining
                        timedOut <- not signaled

            return
                match received with
                | ValueSome message -> Some message
                | ValueNone -> None
        }

    member _.TryScan(scanner: 'Msg -> Async2<'T> option, ?timeout: int) : Async2<'T option> =
        let timeout = defaultArg timeout defaultTimeout
        async2 {
            let startedAt = Stopwatch.GetTimestamp()
            let skipped = ResizeArray<'Msg>()
            let mutable result = ValueNone
            let mutable timedOut = false

            try
                while result.IsNone && not timedOut do
                    match mailbox.TryTake() with
                    | Some message ->
                        match scanner message with
                        | None -> skipped.Add message
                        | Some computation ->
                            let! value = computation
                            result <- ValueSome value
                    | None ->
                        let remaining =
                            if timeout < 0 then
                                Timeout.Infinite
                            else
                                max 0 (timeout - int (Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds))

                        if remaining = 0 then
                            timedOut <- true
                        else
                            let! signaled = mailbox.Wait remaining
                            timedOut <- not signaled

                return
                    match result with
                    | ValueSome value -> Some value
                    | ValueNone -> None
            finally
                mailbox.Restore skipped
        }

    member this.Scan(scanner: 'Msg -> Async2<'T> option, ?timeout: int) : Async2<'T> =
        async2 {
            match! this.TryScan(scanner, ?timeout = timeout) with
            | Some value -> return value
            | None -> return raise (TimeoutException("The mailbox scan operation timed out."))
        }

    member this.PostAndTryAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply option> =
        let timeout = defaultArg timeout defaultTimeout

        async2 {
            let completion =
                TaskCompletionSource<'Reply>(TaskCreationOptions.RunContinuationsAsynchronously)

            this.Post(buildMessage (AsyncReplyChannel2(fun reply -> completion.TrySetResult(reply) |> ignore)))
            let! ct = Async2.CancellationToken

            try
                let! reply =
                    if timeout < 0 then
                        completion.Task
                    else
                        completion.Task.WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)

                return Some reply
            with :? TimeoutException ->
                return None
        }

    member this.PostAndAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply> =
        async2 {
            match! this.PostAndTryAsyncReply(buildMessage, ?timeout = timeout) with
            | Some reply -> return reply
            | None -> return raise (TimeoutException("The mailbox reply operation timed out."))
        }

    member this.TryPostAndReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : 'Reply option =
        Async2.RunSynchronously(this.PostAndTryAsyncReply(buildMessage, ?timeout = timeout))

    member this.PostAndReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : 'Reply =
        match this.TryPostAndReply(buildMessage, ?timeout = timeout) with
        | Some reply -> reply
        | None -> raise (TimeoutException("The mailbox reply operation timed out."))

    member private this.StartBody(immediate: bool) =
        let token =
            lock gate (fun () ->
                if started then
                    raise (InvalidOperationException("The mailbox processor has already been started."))
                if disposed then
                    raise (ObjectDisposedException("MailboxProcessor2"))
                started <- true
                lifetime.Token)

        let computation =
            async2 {
                try
                    do! body this
                with error ->
                    errorEvent.Trigger error
            }

        let task =
            if immediate then
                Async2.StartImmediateAsTask(computation, cancellationToken = token)
            else
                Async2.StartAsTask(computation, cancellationToken = token)

        task.ContinueWith(
            Action<Task<unit>>(fun _ ->
                lock gate (fun () ->
                    bodyCompleted <- true
                    if disposed then
                        lifetime.Dispose())),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        )
        |> ignore

    member this.Start() = this.StartBody(false)

    member this.StartImmediate() = this.StartBody(true)

    member _.Dispose() =
        lock gate (fun () ->
            if not disposed then
                disposed <- true
                lifetime.Cancel()
                mailbox.Dispose()
                if not started || bodyCompleted then
                    lifetime.Dispose())

    interface IDisposable with
        member this.Dispose() = this.Dispose()

    static member Start
        (body: MailboxProcessor2<'Msg> -> Async2<unit>, ?cancellationToken: CancellationToken)
        =
        let processor = new MailboxProcessor2<'Msg>(body, ?cancellationToken = cancellationToken)
        processor.Start()
        processor

    static member Start
        (
            body: MailboxProcessor2<'Msg> -> Async2<unit>,
            throwOnPostAfterDispose: bool,
            ?cancellationToken: CancellationToken
        )
        =
        let processor =
            new MailboxProcessor2<'Msg>(
                body,
                throwOnPostAfterDispose,
                ?cancellationToken = cancellationToken
            )

        processor.Start()
        processor

    static member StartImmediate
        (body: MailboxProcessor2<'Msg> -> Async2<unit>, ?cancellationToken: CancellationToken)
        =
        let processor = new MailboxProcessor2<'Msg>(body, ?cancellationToken = cancellationToken)
        processor.StartImmediate()
        processor

    static member StartImmediate
        (
            body: MailboxProcessor2<'Msg> -> Async2<unit>,
            throwOnPostAfterDispose: bool,
            ?cancellationToken: CancellationToken
        )
        =
        let processor =
            new MailboxProcessor2<'Msg>(
                body,
                throwOnPostAfterDispose,
                ?cancellationToken = cancellationToken
            )

        processor.StartImmediate()
        processor
