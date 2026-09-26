namespace Microsoft.FSharp.Control

// AsyncSeq-compatible semantics adapted to the Async2 carrier and IAsyncEnumerable.
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<AutoOpen>]
module AsyncSeq2AdditionalOperations =
    module Internal = AsyncSeq2Internal

    type AsyncSeq2 with
        static member ofAsyncEnum(source: IAsyncEnumerable<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            source

        static member ofSeqAsync(source: seq<Async2<'T>>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for computation in source do
                    let! value = computation
                    yield value
            }

        /// Enumerate Async2 computations in source order.
        static member ofAsync2Seq(source: seq<Async2<'T>>) : AsyncSeq2<'T> =
            AsyncSeq2.ofSeqAsync source

        /// Enumerate a list of Async2 computations in source order.
        static member ofAsync2List(source: Async2<'T> list) : AsyncSeq2<'T> =
            AsyncSeq2.ofSeqAsync source

        /// Enumerate an array of Async2 computations in source order.
        static member ofAsync2Array(source: Async2<'T> array) : AsyncSeq2<'T> =
            AsyncSeq2.ofSeqAsync source

        static member fromChannel(reader: ChannelReader<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof reader) reader
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                yield! reader.ReadAllAsync(ct)
            }

        static member concatSeq(source: AsyncSeq2<#seq<'T>>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for values in source do
                    yield! (values :> seq<'T>)
            }

        static member allPairs(first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'T * 'U> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            asyncSeq2 {
                for left in first do
                    for right in second do
                        yield left, right
            }

        static member map2(mapping: 'T -> 'U -> 'V) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'V> =
            AsyncSeq2.zip first second |> AsyncSeq2.map (fun (left, right) -> mapping left right)

        static member map3(mapping: 'T -> 'U -> 'V -> 'W) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) (third: AsyncSeq2<'V>) : AsyncSeq2<'W> =
            AsyncSeq2.zip3 first second third |> AsyncSeq2.map (fun (a, b, c) -> mapping a b c)

        static member zipWithIndexAsync(mapping: int64 -> 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable index = 0L
                for value in source do
                    let! mapped = mapping index value
                    yield mapped
                    index <- index + 1L
            }

        static member tryFirst(source: AsyncSeq2<'T>) : Async2<'T option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let! hasNext = enumerator.MoveNextAsync()
                return if hasNext then Some enumerator.Current else None
            }

        static member tryFindBack(predicate: 'T -> bool) (source: AsyncSeq2<'T>) : Async2<'T option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable found = None
                for value in source do
                    if predicate value then found <- Some value
                return found
            }

        static member tryFindBackAsync(predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : Async2<'T option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable found = None
                for value in source do
                    let! matches = predicate value
                    if matches then found <- Some value
                return found
            }

        static member findBack(predicate: 'T -> bool) (source: AsyncSeq2<'T>) : Async2<'T> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! result = AsyncSeq2.tryFindBack predicate source
                return result |> Option.defaultWith Internal.raiseNotFound
            }

        static member findBackAsync(predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : Async2<'T> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! result = AsyncSeq2.tryFindBackAsync predicate source
                return result |> Option.defaultWith Internal.raiseNotFound
            }

        static member tryFindIndexBack(predicate: 'T -> bool) (source: AsyncSeq2<'T>) : Async2<int option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable index = 0
                let mutable found = None
                for value in source do
                    if predicate value then found <- Some index
                    index <- index + 1
                return found
            }

        static member tryFindIndexBackAsync(predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : Async2<int option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable index = 0
                let mutable found = None
                for value in source do
                    let! matches = predicate value
                    if matches then found <- Some index
                    index <- index + 1
                return found
            }

        static member findIndexBack(predicate: 'T -> bool) (source: AsyncSeq2<'T>) : Async2<int> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! result = AsyncSeq2.tryFindIndexBack predicate source
                return result |> Option.defaultWith Internal.raiseNotFound
            }

        static member findIndexBackAsync(predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : Async2<int> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! result = AsyncSeq2.tryFindIndexBackAsync predicate source
                return result |> Option.defaultWith Internal.raiseNotFound
            }

        static member exists2(predicate: 'T -> 'U -> bool) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : Async2<bool> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = (AsyncSeq2.zip first second).GetAsyncEnumerator ct
                let mutable found = false
                let mutable scanning = true
                while scanning && not found do
                    let! next = enumerator.MoveNextAsync()
                    scanning <- next
                    if next then
                        let left, right = enumerator.Current
                        found <- predicate left right
                return found
            }

        static member exists2Async(predicate: 'T -> 'U -> Async2<bool>) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : Async2<bool> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = (AsyncSeq2.zip first second).GetAsyncEnumerator ct
                let mutable found = false
                let mutable scanning = true
                while scanning && not found do
                    let! next = enumerator.MoveNextAsync()
                    scanning <- next
                    if next then
                        let left, right = enumerator.Current
                        let! matches = predicate left right
                        found <- matches
                return found
            }

        static member forall2(predicate: 'T -> 'U -> bool) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : Async2<bool> =
            AsyncSeq2.exists2 (fun left right -> not (predicate left right)) first second
            |> Async2.map not

        static member forall2Async(predicate: 'T -> 'U -> Async2<bool>) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : Async2<bool> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            async2 {
                let! found = AsyncSeq2.exists2Async (fun left right -> async2 { let! matches = predicate left right in return not matches }) first second
                return not found
            }

        static member unzip(source: AsyncSeq2<'T * 'U>) : Async2<'T[] * 'U[]> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.map fst values, Array.map snd values
            }

        static member unzip3(source: AsyncSeq2<'T * 'U * 'V>) : Async2<'T[] * 'U[] * 'V[]> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.map (fun (a, _, _) -> a) values,
                       Array.map (fun (_, b, _) -> b) values,
                       Array.map (fun (_, _, c) -> c) values
            }

        static member splitInto(count: int) (source: AsyncSeq2<'T>) : Async2<'T[][]> =
            if count < 1 then invalidArg (nameof count) "The chunk count must be positive."
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.splitInto count values
            }

        static member cycle(source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! values = AsyncSeq2.toArray source
                if values.Length > 0 then
                    while true do
                        yield! values
            }

        static member rev(source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! values = AsyncSeq2.toArray source
                for index in values.Length - 1 .. -1 .. 0 do
                    yield values[index]
            }

        static member intervalMs(periodMs: int) : AsyncSeq2<DateTime> =
            if periodMs < 0 then invalidArg (nameof periodMs) "The period must be non-negative."
            asyncSeq2 {
                yield DateTime.UtcNow
                while true do
                    do! Async2.Sleep periodMs
                    yield DateTime.UtcNow
            }

        static member sortAsync(source: AsyncSeq2<'T>) : Async2<'T[]> when 'T : comparison =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.sort values
            }

        static member sortByAsync(projection: 'T -> 'Key) (source: AsyncSeq2<'T>) : Async2<'T[]> when 'Key : comparison =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.sortBy projection values
            }

        static member sortDescendingAsync(source: AsyncSeq2<'T>) : Async2<'T[]> when 'T : comparison =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.sortDescending values
            }

        static member sortByDescendingAsync(projection: 'T -> 'Key) (source: AsyncSeq2<'T>) : Async2<'T[]> when 'Key : comparison =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.sortByDescending projection values
            }

        static member sortWithAsync(comparer: 'T -> 'T -> int) (source: AsyncSeq2<'T>) : Async2<'T[]> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! values = AsyncSeq2.toArray source
                return Array.sortWith comparer values
            }

        static member sort(source: AsyncSeq2<'T>) : Async2<'T[]> when 'T : comparison =
            AsyncSeq2.sortAsync source

        static member sortBy(projection: 'T -> 'Key) (source: AsyncSeq2<'T>) : Async2<'T[]> when 'Key : comparison =
            AsyncSeq2.sortByAsync projection source

        static member sortDescending(source: AsyncSeq2<'T>) : Async2<'T[]> when 'T : comparison =
            AsyncSeq2.sortDescendingAsync source

        static member sortByDescending(projection: 'T -> 'Key) (source: AsyncSeq2<'T>) : Async2<'T[]> when 'Key : comparison =
            AsyncSeq2.sortByDescendingAsync projection source

        static member sortWith(comparer: 'T -> 'T -> int) (source: AsyncSeq2<'T>) : Async2<'T[]> =
            AsyncSeq2.sortWithAsync comparer source

        static member toArraySynchronously(source: AsyncSeq2<'T>) : 'T[] =
            AsyncSeq2.toArray source |> Async2.RunSynchronouslyImmediate

        static member toListSynchronously(source: AsyncSeq2<'T>) : 'T list =
            AsyncSeq2.toList source |> Async2.RunSynchronouslyImmediate

        static member toBlockingSeq(source: AsyncSeq2<'T>) : seq<'T> =
            AsyncSeq2.toSeq source

        static member toAsyncEnum(source: AsyncSeq2<'T>) : IAsyncEnumerable<'T> =
            Internal.checkNonNull (nameof source) source
            source

        static member getIterator(source: AsyncSeq2<'T>) : (unit -> Async2<'T option>) =
            Internal.checkNonNull (nameof source) source
            let mutable enumerator = None
            let mutable finished = false
            fun () -> async2 {
                if finished then return None
                else
                    let! ct = Async2.CancellationToken
                    let current =
                        match enumerator with
                        | Some value -> value
                        | None ->
                            let value = source.GetAsyncEnumerator(ct)
                            enumerator <- Some value
                            value
                    let mutable disposing = false
                    try
                        let! next = current.MoveNextAsync()
                        if next then return Some current.Current
                        else
                            finished <- true
                            disposing <- true
                            do! current.DisposeAsync()
                            return None
                    with error ->
                        finished <- true
                        if not disposing then do! current.DisposeAsync()
                        return raise error
            }

        static member cache(source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            let values = ResizeArray<'T>()
            let gate = new SemaphoreSlim(1, 1)
            let mutable enumerator: IAsyncEnumerator<'T> option = None
            let mutable finished = false
            let mutable failure: exn option = None

            let item index = Async2.StartTaskImmediate(fun (ct: CancellationToken) -> task {
                do! gate.WaitAsync(ct)
                try
                    match failure with
                    | _ when index < values.Count -> return Some values[index]
                    | Some error -> return raise error
                    | None when finished -> return None
                    | None ->
                        let current =
                            match enumerator with
                            | Some value -> value
                            | None ->
                                let value = source.GetAsyncEnumerator(ct)
                                enumerator <- Some value
                                value
                        let mutable disposing = false
                        try
                            let! next = current.MoveNextAsync()
                            if next then
                                let value = current.Current
                                values.Add value
                                return Some value
                            else
                                finished <- true
                                enumerator <- None
                                disposing <- true
                                do! current.DisposeAsync()
                                return None
                        with error ->
                            finished <- true
                            enumerator <- None
                            failure <- Some error
                            if not disposing then do! current.DisposeAsync()
                            return raise error
                finally
                    gate.Release() |> ignore
            })

            asyncSeq2 {
                let mutable index = 0
                let mutable reading = true
                while reading do
                    match! item index with
                    | Some value ->
                        yield value
                        index <- index + 1
                    | None -> reading <- false
            }

        static member ofIQueryable(source: System.Linq.IQueryable<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            AsyncSeq2.ofSeq source

        static member bufferByCount(bufferSize: int) (source: AsyncSeq2<'T>) : AsyncSeq2<'T[]> =
            AsyncSeq2.chunkBySize bufferSize source

        static member zapp(functions: AsyncSeq2<'T -> 'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            AsyncSeq2.map2 (fun f value -> f value) functions source

        static member zappAsync(functions: AsyncSeq2<'T -> Async2<'U>>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof functions) functions
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for f, value in AsyncSeq2.zip functions source do
                    let! mapped = f value
                    yield mapped
            }

        static member traverseOptionAsync(mapping: 'T -> Async2<'U option>) (source: AsyncSeq2<'T>) : Async2<AsyncSeq2<'U> option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let results = ResizeArray<'U>()
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable complete = false
                let mutable missing = false
                while not complete && not missing do
                    let! next = enumerator.MoveNextAsync()
                    if next then
                        let! value = mapping enumerator.Current
                        match value with
                        | Some item -> results.Add item
                        | None -> missing <- true
                    else
                        complete <- true
                return if missing then None else Some (AsyncSeq2.ofSeq (results.ToArray()))
            }

        static member traverseChoiceAsync(mapping: 'T -> Async2<Choice<'U, 'Error>>) (source: AsyncSeq2<'T>) : Async2<Choice<AsyncSeq2<'U>, 'Error>> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let results = ResizeArray<'U>()
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable complete = false
                let mutable failure = None
                while not complete && Option.isNone failure do
                    let! next = enumerator.MoveNextAsync()
                    if next then
                        let! value = mapping enumerator.Current
                        match value with
                        | Choice1Of2 item -> results.Add item
                        | Choice2Of2 error -> failure <- Some error
                    else
                        complete <- true
                return match failure with
                       | Some error -> Choice2Of2 error
                       | None -> Choice1Of2 (AsyncSeq2.ofSeq (results.ToArray()))
            }

        static member interleaveChoice(first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<Choice<'T, 'U>> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use left = first.GetAsyncEnumerator ct
                use right = second.GetAsyncEnumerator ct
                let mutable leftActive = true
                let mutable rightActive = true
                while leftActive || rightActive do
                    if leftActive then
                        let! next = left.MoveNextAsync()
                        leftActive <- next
                        if next then yield Choice1Of2 left.Current
                    if rightActive then
                        let! next = right.MoveNextAsync()
                        rightActive <- next
                        if next then yield Choice2Of2 right.Current
            }

        static member interleave(first: AsyncSeq2<'T>) (second: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            AsyncSeq2.interleaveChoice first second
            |> AsyncSeq2.map (function Choice1Of2 value | Choice2Of2 value -> value)

        static member interleaveMany(sources: seq<AsyncSeq2<'T>>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof sources) sources
            AsyncSeq2.delay (fun () ->
                sources |> Seq.fold AsyncSeq2.interleave (AsyncSeq2.empty ()))

        static member toChannel(writer: ChannelWriter<'T>) (source: AsyncSeq2<'T>) : Async2<unit> =
            Internal.checkNonNull (nameof writer) writer
            Internal.checkNonNull (nameof source) source
            async2 {
                let! ct = Async2.CancellationToken
                try
                    for value in source do
                        do! writer.WriteAsync(value, ct)
                    writer.TryComplete() |> ignore
                with error ->
                    writer.TryComplete(error) |> ignore
                    return raise error
            }

        static member zipParallel(first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'T * 'U> =
            Internal.checkNonNull (nameof first) first
            Internal.checkNonNull (nameof second) second
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use left = first.GetAsyncEnumerator ct
                use right = second.GetAsyncEnumerator ct
                let mutable active = true
                while active do
                    let leftNext = left.MoveNextAsync().AsTask()
                    let rightNext = right.MoveNextAsync().AsTask()
                    let! next = Task.WhenAll [| leftNext; rightNext |]
                    active <- next[0] && next[1]
                    if active then yield left.Current, right.Current
            }

        static member zipWithParallel(mapping: 'T -> 'U -> 'V) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'V> =
            AsyncSeq2.zipParallel first second |> AsyncSeq2.map (fun (left, right) -> mapping left right)

        static member zipWithAsyncParallel(mapping: 'T -> 'U -> Async2<'V>) (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) : AsyncSeq2<'V> =
            AsyncSeq2.zipParallel first second |> AsyncSeq2.mapAsync (fun (left, right) -> mapping left right)
