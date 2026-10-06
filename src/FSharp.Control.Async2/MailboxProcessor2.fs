namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open System.Threading.Tasks.Sources

/// A reply channel for MailboxProcessor2.PostAndAsyncReply.
[<Sealed; CompiledName("FSharpAsyncReplyChannel2`1")>]
type AsyncReplyChannel2<'Reply>(reply: 'Reply -> unit) =
    member _.Reply(value: 'Reply) = reply value

[<Sealed>]
type private MailboxProcessor2Reply<'Reply>() =
    let source = ManualResetValueTaskSourceCore<'Reply>(RunContinuationsAsynchronously = true)
    // 0 = open, 1 = reply claimed, 2 = disposed.
    let mutable state = 0

    member this.ValueTask =
        if Volatile.Read(&state) = 2 then
            raise (ObjectDisposedException("ResultCell"))
        ValueTask<'Reply>(this, source.Version)

    member _.Reply(value: 'Reply) =
        if Interlocked.CompareExchange(&state, 1, 0) = 0 then
            source.SetResult value

    interface IDisposable with
        member _.Dispose() =
            Interlocked.Exchange(&state, 2) |> ignore

    interface IValueTaskSource<'Reply> with
        member this.GetResult(token) = source.GetResult token
        member this.GetStatus(token) = source.GetStatus token
        member this.OnCompleted(continuation, state, token, flags) = source.OnCompleted(continuation, state, token, flags)

[<Sealed>]
type private MailboxProcessor2Queue<'Msg>(cancellationSupported: bool, isThrowExceptionAfterDisposed: bool) =
    let gate = obj ()
    let arrivals = Queue<'Msg>()
    let inbox = ResizeArray<'Msg>(1)
    let mutable reader: TaskCompletionSource<bool> option = None
    let mutable pulse: AutoResetEvent option = None
    let mutable disposed = false

    member _.Count =
        lock gate (fun () -> arrivals.Count + inbox.Count)

    member _.Post(message: 'Msg) =
        lock gate (fun () ->
            if disposed then
                if isThrowExceptionAfterDisposed then
                    raise (ObjectDisposedException("Mailbox"))
            else
                arrivals.Enqueue message

            match reader with
            | Some waiting ->
                reader <- None
                waiting.TrySetResult(true) |> ignore
            | None ->
                match pulse with
                | Some event -> event.Set() |> ignore
                | None -> ())

    member private _.TakeInbox() =
        if inbox.Count = 0 then
            None
        else
            let message = inbox.[0]
            inbox.RemoveAt 0
            Some message

    member private _.TakeArrival() =
        lock gate (fun () ->
            if arrivals.Count > 0 then
                Some(arrivals.Dequeue())
            else
                None)

    member private _.ScanInbox(scanner: 'Msg -> Async2<'T> option) =
        let rec scan index =
            if index >= inbox.Count then
                None
            else
                match scanner inbox.[index] with
                | None -> scan (index + 1)
                | Some computation ->
                    inbox.RemoveAt index
                    Some computation

        scan 0

    member private _.ScanArrivals(scanner: 'Msg -> Async2<'T> option) =
        lock gate (fun () ->
            let rec scan () =
                if arrivals.Count = 0 then
                    None
                else
                    let message = arrivals.Dequeue()
                    match scanner message with
                    | None ->
                        inbox.Add message
                        scan ()
                    | Some computation -> Some computation

            scan ())

    member private _.AwaitArrival() =
        lock gate (fun () ->
            match reader with
            | Some _ -> failwith "multiple waiting reader continuations for mailbox"
            | None when arrivals.Count > 0 -> Task.FromResult true
            | None ->
                let waiting = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
                reader <- Some waiting
                waiting.Task)

    member private this.Wait(timeout: int) : Async2<bool> =
        if timeout < 0 && not cancellationSupported then
            async2 { return! this.AwaitArrival() }
        else
            async2 {
                let ready, event =
                    lock gate (fun () ->
                        let event =
                            match pulse with
                            | Some event -> event
                            | None ->
                                let event = new AutoResetEvent(false)
                                pulse <- Some event
                                event

                        arrivals.Count > 0, event)

                if ready then
                    return true
                else
                    return! Async2.AwaitWaitHandle(event, millisecondsTimeout = timeout)
            }

    member private this.WaitForScan(timeout: int) : Async2<bool> =
        async2 {
            let! ct = Async2.CancellationToken
            let waiting = this.AwaitArrival()
            try
                try
                    return! waiting.WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)
                with :? TimeoutException ->
                    return false
            finally
                lock gate (fun () ->
                    match reader with
                    | Some pending when obj.ReferenceEquals(pending.Task, waiting) -> reader <- None
                    | _ -> ())
        }

    member this.TryReceive(timeout: int) : Async2<'Msg option> =
        let rec receive () =
            async2 {
                match this.TakeArrival() with
                | Some message -> return Some message
                | None ->
                    let! signaled = this.Wait timeout
                    if signaled then return! receive ()
                    else return None
            }

        async2 {
            match this.TakeInbox() with
            | Some message -> return Some message
            | None -> return! receive ()
        }

    member this.Receive(timeout: int) : Async2<'Msg> =
        async2 {
            match! this.TryReceive timeout with
            | Some message -> return message
            | None -> return raise (TimeoutException("The mailbox receive operation timed out."))
        }

    member this.TryScan(scanner: 'Msg -> Async2<'T> option, timeout: int) : Async2<'T option> =
        async2 {
            match this.ScanInbox scanner with
            | Some computation ->
                let! value = computation
                return Some value
            | None ->
                let startedAt = Stopwatch.GetTimestamp()
                let rec scan () =
                    async2 {
                        match this.ScanArrivals scanner with
                        | Some computation ->
                            let! value = computation
                            return Some value
                        | None ->
                            let! signaled =
                                if timeout < 0 then
                                    this.Wait Timeout.Infinite
                                else
                                    let elapsed = min (float timeout) (Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds)
                                    this.WaitForScan(timeout - int elapsed)

                            if signaled then return! scan ()
                            else return None
                    }

                return! scan ()
        }

    member this.Scan(scanner: 'Msg -> Async2<'T> option, timeout: int) : Async2<'T> =
        async2 {
            match! this.TryScan(scanner, timeout) with
            | Some value -> return value
            | None -> return raise (TimeoutException("The mailbox scan operation timed out."))
        }

    member _.Dispose() =
        lock gate (fun () ->
            inbox.Clear()
            arrivals.Clear()
            disposed <- true
            match pulse with
            | Some event ->
                event.Dispose()
                pulse <- None
            | None -> ())

#if DEBUG
    member _.UnsafeContents = (inbox, arrivals, Option.toObj pulse, reader) |> box
#endif

[<Sealed; AutoSerializable(false); CompiledName("FSharpMailboxProcessor2`1")>]
type MailboxProcessor2<'Msg>
    (
        body: MailboxProcessor2<'Msg> -> Async2<unit>,
        isThrowExceptionAfterDisposed: bool,
        ?cancellationToken: CancellationToken
    ) =

    let cancellationSupported = cancellationToken.IsSome
    let cancellationToken = defaultArg cancellationToken Async2.DefaultCancellationToken
    let mailbox = MailboxProcessor2Queue<'Msg>(cancellationSupported, isThrowExceptionAfterDisposed)
    let errorEvent = Event<Exception>()
    let gate = obj ()
    let mutable defaultTimeout = Timeout.Infinite
    let mutable started = false

    new(body: MailboxProcessor2<'Msg> -> Async2<unit>, ?cancellationToken: CancellationToken) =
        new MailboxProcessor2<'Msg>(body, false, ?cancellationToken = cancellationToken)

    member _.CurrentQueueLength = mailbox.Count

    member _.DefaultTimeout
        with get () = defaultTimeout
        and set value = defaultTimeout <- value

    [<CLIEvent>]
    member _.Error = errorEvent.Publish

#if DEBUG
    member _.UnsafeMessageQueueContents = mailbox.UnsafeContents
#endif

    member _.Post(message: 'Msg) = mailbox.Post message

    member _.Receive(?timeout: int) : Async2<'Msg> =
        mailbox.Receive(defaultArg timeout defaultTimeout)

    member _.TryReceive(?timeout: int) : Async2<'Msg option> =
        mailbox.TryReceive(defaultArg timeout defaultTimeout)

    member _.TryScan(scanner: 'Msg -> Async2<'T> option, ?timeout: int) : Async2<'T option> =
        mailbox.TryScan(scanner, defaultArg timeout defaultTimeout)

    member _.Scan(scanner: 'Msg -> Async2<'T> option, ?timeout: int) : Async2<'T> =
        mailbox.Scan(scanner, defaultArg timeout defaultTimeout)

    member this.PostAndTryAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply option> =
        let timeout = defaultArg timeout defaultTimeout
        let result = new MailboxProcessor2Reply<'Reply>()
        this.Post(buildMessage (AsyncReplyChannel2(result.Reply)))

        if timeout = Timeout.Infinite && not cancellationSupported then
            async2 {
                let! reply = result.ValueTask
                return Some reply
            }
        else
            async2 {
                use _disposeResult = result
                let! ct = Async2.CancellationToken
                try
                    let task = result.ValueTask.AsTask()
                    let! reply = task.WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)
                    return Some reply
                with :? TimeoutException ->
                    return None
            }

    member this.PostAndAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply> =
        let timeout = defaultArg timeout defaultTimeout
        if timeout = Timeout.Infinite && not cancellationSupported then
            let result = new MailboxProcessor2Reply<'Reply>()
            this.Post(buildMessage (AsyncReplyChannel2(result.Reply)))
            Async2.Await result.ValueTask
        else
            let reply = this.PostAndTryAsyncReply(buildMessage, timeout = timeout)
            async2 {
                match! reply with
                | Some value -> return value
                | None -> return raise (TimeoutException("The mailbox reply operation timed out."))
            }

    member this.TryPostAndReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : 'Reply option =
        let timeout = defaultArg timeout defaultTimeout
        use result = new MailboxProcessor2Reply<'Reply>()
        this.Post(buildMessage (AsyncReplyChannel2(result.Reply)))
        let task = result.ValueTask.AsTask()
        if task.IsCompleted || task.Wait(timeout) then
            Some(task.GetAwaiter().GetResult())
        else
            None

    member this.PostAndReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : 'Reply =
        match this.TryPostAndReply(buildMessage, ?timeout = timeout) with
        | Some reply -> reply
        | None -> raise (TimeoutException("The mailbox reply operation timed out."))

    member private this.StartBody(immediate: bool) =
        lock gate (fun () ->
            if started then
                raise (InvalidOperationException("The mailbox processor has already been started."))
            started <- true)

        let computation =
            async2 {
                try
                    do! body this
                with error ->
                    errorEvent.Trigger error
            }

        if immediate then
            Async2.StartImmediate(computation, cancellationToken = cancellationToken)
        else
            Async2.Start(computation, cancellationToken = cancellationToken)

    member this.Start() = this.StartBody(false)

    member this.StartImmediate() = this.StartBody(true)

    member _.Dispose() = mailbox.Dispose()

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
            isThrowExceptionAfterDisposed: bool,
            ?cancellationToken: CancellationToken
        )
        =
        let processor =
            new MailboxProcessor2<'Msg>(
                body,
                isThrowExceptionAfterDisposed,
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
            isThrowExceptionAfterDisposed: bool,
            ?cancellationToken: CancellationToken
        )
        =
        let processor =
            new MailboxProcessor2<'Msg>(
                body,
                isThrowExceptionAfterDisposed,
                ?cancellationToken = cancellationToken
            )

        processor.StartImmediate()
        processor
