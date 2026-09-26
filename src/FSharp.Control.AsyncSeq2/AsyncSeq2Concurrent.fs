namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Microsoft.FSharp.Control.AsyncSeq2Implementation
open Microsoft.FSharp.Core.CompilerServices

[<AutoOpen>]
module AsyncSeq2ConcurrentOperations =
    let private mapParallel unordered parallelism (mapping: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        AsyncSeq2Internal.checkNonNull (nameof source) source
        if parallelism < 1 then invalidArg (nameof parallelism) "Parallelism must be positive."
        asyncSeq2 {
            let! ct = Async2.CancellationToken
            use linked = CancellationTokenSource.CreateLinkedTokenSource(ct)
            use throttle = new SemaphoreSlim(parallelism)
            let token = linked.Token
            let ordered = Channel.CreateUnbounded<Task<'U>>()
            let unorderedResults = Channel.CreateUnbounded<'U>()

            let producer = task {
                let jobs = ResizeArray<Task>()
                try
                    use enumerator = source.GetAsyncEnumerator(token)
                    let mutable running = true
                    while running do
                        let! next = enumerator.MoveNextAsync()
                        if next then
                            do! throttle.WaitAsync(token)
                            let current = enumerator.Current
                            let work = task {
                                try
                                    try
                                        let! result = Async2.StartAsTask(mapping current, cancellationToken = token)
                                        if unordered then
                                            do! unorderedResults.Writer.WriteAsync(result, token)
                                        return result
                                    with error ->
                                        if unordered && not token.IsCancellationRequested then
                                            unorderedResults.Writer.TryComplete(error) |> ignore
                                        return raise error
                                finally
                                    throttle.Release() |> ignore
                            }
                            jobs.Add(work :> Task)
                            if not unordered then
                                do! ordered.Writer.WriteAsync(work, token)
                        else
                            running <- false
                    try
                        do! Task.WhenAll(jobs)
                    with
                    | :? OperationCanceledException when token.IsCancellationRequested -> ()
                    | error ->
                        ordered.Writer.TryComplete(error) |> ignore
                        unorderedResults.Writer.TryComplete(error) |> ignore
                with
                | :? OperationCanceledException when token.IsCancellationRequested -> ()
                | error ->
                    ordered.Writer.TryComplete(error) |> ignore
                    unorderedResults.Writer.TryComplete(error) |> ignore
                ordered.Writer.TryComplete() |> ignore
                unorderedResults.Writer.TryComplete() |> ignore
            }

            try
                if unordered then
                    for result in unorderedResults.Reader.ReadAllAsync(ct) do
                        yield result
                else
                    for work in ordered.Reader.ReadAllAsync(ct) do
                        let! result = work
                        yield result
            finally
                linked.Cancel()
                AsyncHelpers.Await(producer)
        }

    type AsyncSeq2 with
        static member mapAsyncParallel(mapping: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            mapParallel false Int32.MaxValue mapping source

        static member mapAsyncParallelThrottled(parallelism: int) (mapping: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            mapParallel false parallelism mapping source

        static member mapAsyncUnorderedParallel(mapping: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            mapParallel true Int32.MaxValue mapping source

        static member mapAsyncUnorderedParallelThrottled(parallelism: int) (mapping: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            mapParallel true parallelism mapping source

        static member iterAsyncParallel(action: 'T -> Async2<unit>) (source: AsyncSeq2<'T>) : Async2<unit> =
            AsyncSeq2.mapAsyncParallel action source |> AsyncSeq2.iter ignore

        static member iterAsyncParallelThrottled(parallelism: int) (action: 'T -> Async2<unit>) (source: AsyncSeq2<'T>) : Async2<unit> =
            AsyncSeq2.mapAsyncParallelThrottled parallelism action source |> AsyncSeq2.iter ignore

        static member takeUntilSignal(signal: Async2<unit>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use linked = CancellationTokenSource.CreateLinkedTokenSource(ct)
                use enumerator = source.GetAsyncEnumerator(linked.Token)
                let stopped = Async2.StartAsTask(signal, cancellationToken = linked.Token)
                try
                    let mutable running = true
                    while running do
                        let next = enumerator.MoveNextAsync().AsTask()
                        let! winner = Task.WhenAny(next :> Task, stopped :> Task)
                        if obj.ReferenceEquals(winner, stopped) then
                            do! stopped
                            linked.Cancel()
                            let! nextError =
                                next.ContinueWith(fun (task: Task<bool>) ->
                                    if task.IsFaulted then Some task.Exception.InnerException
                                    elif task.IsCanceled then Some (OperationCanceledException(linked.Token) :> exn)
                                    else None)
                            match nextError with
                            | Some (:? OperationCanceledException) when linked.IsCancellationRequested -> ()
                            | Some error -> raise error
                            | None -> ()
                            running <- false
                        else
                            let! hasValue = next
                            if hasValue then yield enumerator.Current
                            else running <- false
                finally
                    linked.Cancel()
            }

        static member takeUntil(signal: Async2<unit>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2.takeUntilSignal signal source

        static member skipUntilSignal(signal: Async2<unit>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use linked = CancellationTokenSource.CreateLinkedTokenSource(ct)
                let started = Async2.StartAsTask(signal, cancellationToken = linked.Token)
                try
                    for value in source do
                        if started.IsCompleted then
                            do! started
                            yield value
                finally
                    linked.Cancel()
            }

        static member skipUntil(signal: Async2<unit>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2.skipUntilSignal signal source

        static member mergeChoice(first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<Choice<'T, 'U>> =
            AsyncSeq2Internal.checkNonNull (nameof first) first
            AsyncSeq2Internal.checkNonNull (nameof second) second
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use linked = CancellationTokenSource.CreateLinkedTokenSource(ct)
                let token = linked.Token
                let channel = Channel.CreateUnbounded<Choice<'T, 'U>>()

                let produce (source: AsyncSeq2<'V>) wrap = task {
                    try
                        use enumerator = source.GetAsyncEnumerator(token)
                        let mutable running = true
                        while running do
                            let! next = enumerator.MoveNextAsync()
                            if next then
                                do! channel.Writer.WriteAsync(wrap enumerator.Current, token)
                            else
                                running <- false
                    with
                    | :? OperationCanceledException when token.IsCancellationRequested -> ()
                    | error -> channel.Writer.TryComplete(error) |> ignore
                }

                let left = produce first Choice1Of2
                let right = produce second Choice2Of2
                let completed = Task.WhenAll(left, right)
                completed.ContinueWith(fun (_: Task) -> channel.Writer.TryComplete() |> ignore) |> ignore
                try
                    for value in channel.Reader.ReadAllAsync(ct) do
                        yield value
                finally
                    linked.Cancel()
                    AsyncHelpers.Await(completed) |> ignore
            }

        static member merge(first: AsyncSeq2<'T>) (second: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2.mergeChoice first second
            |> AsyncSeq2.map (function Choice1Of2 value | Choice2Of2 value -> value)

        static member mergeAll(sources: seq<AsyncSeq2<'T>>) : AsyncSeq2<'T> =
            AsyncSeq2Internal.checkNonNull (nameof sources) sources
            AsyncSeq2.delay (fun () ->
                sources |> Seq.fold AsyncSeq2.merge (AsyncSeq2.empty ()))

        static member combineLatestWithAsync(combine: 'T -> 'U -> Async2<'V>) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'V> =
            AsyncSeq2Internal.checkNonNull (nameof first) first
            AsyncSeq2Internal.checkNonNull (nameof second) second
            asyncSeq2 {
                let mutable left = ValueNone
                let mutable right = ValueNone
                for next in AsyncSeq2.mergeChoice first second do
                    match next with
                    | Choice1Of2 value -> left <- ValueSome value
                    | Choice2Of2 value -> right <- ValueSome value
                    match left, right with
                    | ValueSome a, ValueSome b ->
                        let! result = combine a b
                        yield result
                    | _ -> ()
            }

        static member combineLatestWith(combine: 'T -> 'U -> 'V) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'V> =
            AsyncSeq2.combineLatestWithAsync (fun left right -> async2 { return combine left right }) first second

        static member combineLatest(first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'T * 'U> =
            AsyncSeq2.combineLatestWith (fun left right -> left, right) first second

        static member bufferByCountAndTime(bufferSize: int) (timeoutMs: int) (source: AsyncSeq2<'T>) : AsyncSeq2<'T[]> =
            if bufferSize < 1 then invalidArg (nameof bufferSize) "Buffer size must be positive."
            if timeoutMs < 1 then invalidArg (nameof timeoutMs) "Timeout must be positive."
            AsyncSeq2Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let completed = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
                let values = asyncSeq2 {
                    yield! source
                    completed.TrySetResult(()) |> ignore
                }
                let ticks =
                    AsyncSeq2.intervalMs timeoutMs
                    |> AsyncSeq2.skip 1
                    |> AsyncSeq2.takeUntilSignal (async2 { do! completed.Task })
                let buffer = ResizeArray<'T>()
                for next in AsyncSeq2.mergeChoice values ticks do
                    match next with
                    | Choice1Of2 value ->
                        buffer.Add value
                        if buffer.Count = bufferSize then
                            yield buffer.ToArray()
                            buffer.Clear()
                    | Choice2Of2 _ ->
                        if buffer.Count > 0 then
                            yield buffer.ToArray()
                            buffer.Clear()
                if buffer.Count > 0 then yield buffer.ToArray()
            }

        static member bufferByTime(timeoutMs: int) (source: AsyncSeq2<'T>) : AsyncSeq2<'T[]> =
            AsyncSeq2.bufferByCountAndTime Int32.MaxValue timeoutMs source

        static member prefetch(source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2Internal.checkNonNull (nameof source) source
            AsyncSeq2.merge (AsyncSeq2.empty ()) source
