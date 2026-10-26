namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// A reply channel for MailboxProcessor2.PostAndAsyncReply.
[<Sealed; CompiledName("FSharpAsyncReplyChannel2`1")>]
type AsyncReplyChannel2<'Reply>(reply: 'Reply -> unit) =
    member _.Reply(value: 'Reply) = reply value

module private MailboxProcessor2Reply =
    /// A reply channel and the task its first reply completes. Continuations run asynchronously, so the
    /// waiting caller never resumes inside the agent's `Reply` call.
    let create () =
        let completion = TaskCompletionSource<'Reply>(TaskCreationOptions.RunContinuationsAsynchronously)
        AsyncReplyChannel2(fun reply -> completion.TrySetResult reply |> ignore), completion.Task

/// The message store. `arrivals` receives posts; `inbox` keeps messages a scan skipped, in order.
/// Both are guarded by `gate`, so `Dispose` and `CurrentQueueLength` are safe from any thread.
/// Only one reader may wait at a time.
[<Sealed>]
type private MailboxProcessor2Queue<'Msg>(cancellationSupported: bool, isThrowExceptionAfterDisposed: bool) =
    let gate = obj ()
    let arrivals = Queue<'Msg>()
    let inbox = ResizeArray<'Msg>(1)
    let mutable reader: TaskCompletionSource<bool> voption = ValueNone
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
                | ValueSome waiting ->
                    reader <- ValueNone
                    // Runs the reader's continuation asynchronously, never under the gate.
                    waiting.TrySetResult true |> ignore
                | ValueNone -> ())

    /// Takes the oldest skipped message, or else the oldest arrival.
    member private _.TryTake() =
        lock gate (fun () ->
            if inbox.Count > 0 then
                let message = inbox[0]
                inbox.RemoveAt 0
                ValueSome message
            else
                match arrivals.TryDequeue() with
                | true, message -> ValueSome message
                | _ -> ValueNone)

    member private _.ScanInbox(scanner: 'Msg -> Async2<'T> option) =
        lock gate (fun () ->
            let rec scan index =
                if index >= inbox.Count then
                    ValueNone
                else
                    match scanner inbox[index] with
                    | None -> scan (index + 1)
                    | Some computation ->
                        inbox.RemoveAt index
                        ValueSome computation

            scan 0)

    /// Scans new arrivals once each, moving the ones the scanner skips to the inbox.
    member private _.ScanArrivals(scanner: 'Msg -> Async2<'T> option) =
        lock gate (fun () ->
            let rec scan () =
                match arrivals.TryDequeue() with
                | false, _ -> ValueNone
                | true, message ->
                    match scanner message with
                    | None ->
                        inbox.Add message
                        scan ()
                    | Some computation -> ValueSome computation

            scan ())

    member private _.AwaitArrival() =
        lock gate (fun () ->
            if reader.IsSome then
                failwith "multiple waiting reader continuations for mailbox"
            elif arrivals.Count > 0 then
                Task.FromResult true
            else
                let waiting = TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
                reader <- ValueSome waiting
                waiting.Task)

    member private _.ClearReader(waiting: Task<bool>) =
        lock gate (fun () ->
            match reader with
            | ValueSome pending when obj.ReferenceEquals(pending.Task, waiting) -> reader <- ValueNone
            | _ -> ())

    /// True when a message may have arrived, false when `timeout` passes first. An expired or canceled
    /// wait unregisters itself, so it cannot take the signal meant for a later wait.
    member private this.WaitForArrival(timeout: TimeSpan, observeCancellation: bool) : Async2<bool> =
        async2 {
            let! ct = CurrentCancellationToken
            let waiting = this.AwaitArrival()

            try
                try
                    return! waiting.WaitAsync(timeout, (if observeCancellation then ct else CancellationToken.None))
                with :? TimeoutException ->
                    return false
            finally
                this.ClearReader waiting
        }

    /// Like FSharp.Core, an infinite wait observes cancellation only when the mailbox was given a token.
    member private this.WaitForArrival(timeout: int) =
        if timeout < 0 && not cancellationSupported then
            this.WaitForArrival(Timeout.InfiniteTimeSpan, false)
        else
            this.WaitForArrival(TimeSpan.FromMilliseconds(float timeout), true)

    member private this.ReceiveCore(timeout: int) : Async2<'Msg voption> =
        let rec receive () =
            async2 {
                match this.TryTake() with
                | ValueSome message -> return ValueSome message
                | ValueNone ->
                    let! signaled = this.WaitForArrival timeout
                    if signaled then return! receive ()
                    else return ValueNone
            }

        receive ()

    member this.TryReceive(timeout: int) : Async2<'Msg option> =
        async2 {
            match! this.ReceiveCore timeout with
            | ValueSome message -> return Some message
            | ValueNone -> return None
        }

    member this.Receive(timeout: int) : Async2<'Msg> =
        async2 {
            match! this.ReceiveCore timeout with
            | ValueSome message -> return message
            | ValueNone -> return raise (TimeoutException("The mailbox receive operation timed out."))
        }

    member this.TryScan(scanner: 'Msg -> Async2<'T> option, timeout: int) : Async2<'T option> =
        let rec scan (wait: unit -> Async2<bool>) =
            async2 {
                match this.ScanArrivals scanner with
                | ValueSome computation ->
                    let! value = computation
                    return Some value
                | ValueNone ->
                    let! signaled = wait ()
                    if signaled then return! scan wait
                    else return None
            }

        async2 {
            match this.ScanInbox scanner with
            | ValueSome computation ->
                let! value = computation
                return Some value
            | ValueNone ->
                if timeout < 0 then
                    return! scan (fun () -> this.WaitForArrival Timeout.Infinite)
                else
                    // One deadline for the whole scan: unmatched arrivals do not restart the timeout,
                    // and it no longer applies once a computation is selected.
                    let deadline = Environment.TickCount64 + int64 timeout
                    return! scan (fun () ->
                        let remaining = max 0L (deadline - Environment.TickCount64)
                        this.WaitForArrival(TimeSpan.FromMilliseconds(float remaining), true))
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
            disposed <- true)

#if DEBUG
    member _.UnsafeContents = (inbox, arrivals, reader) |> box
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

    member private this.PostWithReply(buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg) =
        let channel, reply = MailboxProcessor2Reply.create ()
        this.Post(buildMessage channel)
        reply

    member this.PostAndTryAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply option> =
        let timeout = defaultArg timeout defaultTimeout
        let reply = this.PostWithReply buildMessage

        if timeout = Timeout.Infinite && not cancellationSupported then
            async2 {
                let! value = reply
                return Some value
            }
        else
            async2 {
                let! ct = CurrentCancellationToken
                try
                    let! value = reply.WaitAsync(TimeSpan.FromMilliseconds(float timeout), ct)
                    return Some value
                with :? TimeoutException ->
                    return None
            }

    member this.PostAndAsyncReply
        (buildMessage: AsyncReplyChannel2<'Reply> -> 'Msg, ?timeout: int)
        : Async2<'Reply> =
        let timeout = defaultArg timeout defaultTimeout
        if timeout = Timeout.Infinite && not cancellationSupported then
            Async2.Await(this.PostWithReply buildMessage)
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
        let reply = this.PostWithReply buildMessage
        // Checked first, like the original: an available reply is returned even for an invalid timeout.
        if reply.IsCompleted || reply.Wait(timeout) then
            Some reply.Result
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
