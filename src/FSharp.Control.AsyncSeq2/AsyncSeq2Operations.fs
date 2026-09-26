namespace Microsoft.FSharp.Control

open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Microsoft.FSharp.Control.AsyncSeq2Implementation

module Internal = AsyncSeq2Internal

module private AsyncSeq2Cold =
    let withToken (token: CancellationToken) (source: AsyncSeq2<'T>) =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_) = source.GetAsyncEnumerator(token) }

    let terminalWithToken (source: AsyncSeq2<'T>) (run: CancellationToken -> AsyncSeq2<'T> -> Task<'U>) : Async2<'U> =
        Internal.checkNonNull (nameof source) source
        Async2.StartTaskImmediate(fun token -> run token (withToken token source))

    let terminal source run = terminalWithToken source (fun _ -> run)

    let binaryWithToken (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>)
        (run: CancellationToken -> AsyncSeq2<'T> -> AsyncSeq2<'U> -> Task<'V>) : Async2<'V> =
        Internal.checkNonNull (nameof first) first
        Internal.checkNonNull (nameof second) second
        Async2.StartTaskImmediate(fun ct -> run ct (withToken ct first) (withToken ct second))

    let terminalAsync (source: AsyncSeq2<'T>) (run: AsyncSeq2<'T> -> Async2<'U>) =
        Internal.checkNonNull (nameof source) source
        run source

    let binaryAsync (first: AsyncSeq2<'T>) (second: AsyncSeq2<'U>) run =
        Internal.checkNonNull (nameof first) first
        Internal.checkNonNull (nameof second) second
        run first second

    let keyed projection source =
        AsyncSeq2.mapAsync (fun value -> async2 {
            let! key = projection value
            return key, value
        }) source

[<AutoOpen>]
module AsyncSeq2OperationExtensions =
    type AsyncSeq2 with
        static member inline sum (source: AsyncSeq2< ^T >) : Async2< ^T > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^T >
                for value in source do
                    total <- total + value
                return total
            }

        static member inline sumBy (projection: 'T -> ^U) (source: AsyncSeq2<'T>) : Async2< ^U > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^U >
                for value in source do
                    total <- total + projection value
                return total
            }

        static member inline sumByAsync (projection: 'T -> Async2< ^U >) (source: AsyncSeq2<'T>) : Async2< ^U > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^U >
                for value in source do
                    let! mapped = projection value
                    total <- total + mapped
                return total
            }

        static member inline average (source: AsyncSeq2< ^T >) : Async2< ^T > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^T >
                let mutable count = 0
                for value in source do
                    total <- total + value
                    count <- count + 1
                if count = 0 then Internal.raiseEmptySeq ()
                return LanguagePrimitives.DivideByInt total count
            }

        static member inline averageBy (projection: 'T -> ^U) (source: AsyncSeq2<'T>) : Async2< ^U > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^U >
                let mutable count = 0
                for value in source do
                    total <- total + projection value
                    count <- count + 1
                if count = 0 then Internal.raiseEmptySeq ()
                return LanguagePrimitives.DivideByInt total count
            }

        static member inline averageByAsync (projection: 'T -> Async2< ^U >) (source: AsyncSeq2<'T>) : Async2< ^U > =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable total = Unchecked.defaultof< ^U >
                let mutable count = 0
                for value in source do
                    let! mapped = projection value
                    total <- total + mapped
                    count <- count + 1
                if count = 0 then Internal.raiseEmptySeq ()
                return LanguagePrimitives.DivideByInt total count
            }

        // Rules for static classes, see bug report: https://github.com/dotnet/fsharp/issues/8093
        // F# does not need this internally, but C# does
        // 'Abstract & Sealed': makes it a static class in C#
        // the 'private ()' ensure that a constructor is emitted, which is required by IL

        static member replicate count value = Internal.replicate count value
        static member replicateInfinite value = Internal.replicateInfinite value
        static member replicateInfiniteAsync (computation: unit -> Async2<'T>) : AsyncSeq2<'T> =
            asyncSeq2 {
                while true do
                    let! value = computation ()
                    yield value
            }
        static member replicateUntilNoneAsync (computation: unit -> Async2<'T option>) : AsyncSeq2<'T> =
            asyncSeq2 {
                let mutable running = true
                while running do
                    match! computation () with
                    | Some value -> yield value
                    | None -> running <- false
            }

        static member toListSync(source: AsyncSeq2<'T>) = [
            Internal.checkNonNull (nameof source) source
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                while (let vt = e.MoveNextAsync() in if vt.IsCompleted then vt.Result else vt.AsTask().Result) do
                    yield e.Current
            finally
                e.DisposeAsync().AsTask().Wait()
        ]

        static member toArraySync(source: AsyncSeq2<'T>) = [|
            Internal.checkNonNull (nameof source) source
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                while (let vt = e.MoveNextAsync() in if vt.IsCompleted then vt.Result else vt.AsTask().Result) do
                    yield e.Current
            finally
                e.DisposeAsync().AsTask().Wait()
        |]

        static member toSeq(source: AsyncSeq2<'T>) =
            Internal.checkNonNull (nameof source) source

            seq {

                let e = source.GetAsyncEnumerator CancellationToken.None

                try
                    while (let vt = e.MoveNextAsync() in if vt.IsCompleted then vt.Result else vt.AsTask().Result) do
                        yield e.Current
                finally
                    e.DisposeAsync().AsTask().Wait()
            }

        static member toArrayAsync source = AsyncSeq2.toArray source

        static member toListAsync source = AsyncSeq2.toList source

        static member toResizeArrayAsync source =
            async2 {
                let! values = AsyncSeq2.toArray source
                return ResizeArray values
            }

        static member toIListAsync source =
            async2 {
                let! values = AsyncSeq2.toArray source
                return ResizeArray(values) :> IList<_>
            }

        static member toChannelAsync (writer: ChannelWriter<'T>) (source: AsyncSeq2<'T>) : Async2<unit> =
            Internal.checkNonNull (nameof writer) writer
            Internal.checkNonNull (nameof source) source

            async2 {
                let! ct = Async2.CancellationToken
                try
                    for value in source do
                        do! writer.WriteAsync(value, ct)
                    writer.TryComplete() |> ignore
                with error ->
                    writer.TryComplete error |> ignore
                    return raise error
            }

        //
        // Convert 'OfXXX' functions
        //

        static member ofArray(source: 'T[]) =
            Internal.checkNonNull (nameof source) source

            algorithmSeq {
                for c in source do
                    yield c
            }

        static member ofList(source: 'T list) = algorithmSeq {
            for c in source do
                yield c
        }

        static member ofResizeArray(source: 'T ResizeArray) =
            Internal.checkNonNull (nameof source) source

            algorithmSeq {
                for c in source do
                    yield c
            }

        static member ofTaskSeq(source: #Task<'T> seq) =
            Internal.checkNonNull (nameof source) source

            algorithmSeq {
                for c in source do
                    let! c = c
                    yield c
            }

        static member ofTaskList(source: #Task<'T> list) = algorithmSeq {
            for c in source do
                let! c = c
                yield c
        }

        static member ofTaskArray(source: #Task<'T> array) =
            Internal.checkNonNull (nameof source) source

            algorithmSeq {
                for c in source do
                    let! c = c
                    yield c
            }

        static member ofChannel(reader: ChannelReader<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof reader) reader

            asyncSeq2 {
                let! ct = Async2.CancellationToken
                yield! reader.ReadAllAsync(ct)
            }

        static member withCancellation (cancellationToken: CancellationToken) (source: AsyncSeq2<'T>) =
            Internal.checkNonNull (nameof source) source

            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(_ct) = source.GetAsyncEnumerator(cancellationToken)
            }

        //
        // Utility functions
        //

        static member max source = AsyncSeq2Cold.terminal source (Internal.maxMin max)
        static member min source = AsyncSeq2Cold.terminal source (Internal.maxMin min)
        static member tryMax source = AsyncSeq2Cold.terminal source (Internal.tryMaxMin max)
        static member tryMin source = AsyncSeq2Cold.terminal source (Internal.tryMaxMin min)
        static member maxBy projection source = AsyncSeq2Cold.terminal source (Internal.maxMinBy (<) projection)
        static member minBy projection source = AsyncSeq2Cold.terminal source (Internal.maxMinBy (>) projection)
        static member maxByAsync projection source =
            AsyncSeq2Cold.keyed projection source
            |> fun keyed -> AsyncSeq2Cold.terminal keyed (Internal.maxMinBy (<) fst)
            |> Async2.map snd
        static member minByAsync projection source =
            AsyncSeq2Cold.keyed projection source
            |> fun keyed -> AsyncSeq2Cold.terminal keyed (Internal.maxMinBy (>) fst)
            |> Async2.map snd

        static member lengthOrMax max source = AsyncSeq2Cold.terminal source (Internal.lengthBeforeMax max)
        static member lengthBy predicate source = AsyncSeq2Cold.terminal source (Internal.lengthBy (Some(Predicate predicate)))
        static member lengthByAsync predicate source =
            AsyncSeq2.filterAsync predicate source |> AsyncSeq2.length
        static member init count initializer = Internal.init (Some count) (InitAction initializer)
        static member initInfinite initializer = Internal.init None (InitAction initializer)
        static member initAsync count (initializer: int -> Async2<'T>) : AsyncSeq2<'T> =
            Internal.raiseCannotBeNegative (nameof count) count
            asyncSeq2 {
                for index in 0 .. count - 1 do
                    let! value = initializer index
                    yield value
            }
        static member initInfiniteAsync (initializer: int -> Async2<'T>) : AsyncSeq2<'T> =
            asyncSeq2 {
                let mutable index = 0
                while true do
                    let! value = initializer index
                    yield value
                    index <- index + 1
            }

        static member unfold generator state = Internal.unfold generator state
        static member unfoldAsync (generator: 'State -> Async2<('T * 'State) option>) state : AsyncSeq2<'T> =
            asyncSeq2 {
                let mutable current = state
                let mutable running = true
                while running do
                    match! generator current with
                    | Some (value, next) ->
                        yield value
                        current <- next
                    | None -> running <- false
            }

        static member delay(generator: unit -> AsyncSeq2<'T>) =
            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(ct) = generator().GetAsyncEnumerator(ct)
            }

        static member concat(sources: AsyncSeq2<#AsyncSeq2<'T>>) =
            Internal.checkNonNull (nameof sources) sources

            algorithmSeq {
                for ts in sources do
                    // no null-check of inner taskseqs, similar to seq
                    yield! (ts :> AsyncSeq2<'T>)
            }

        static member concat(sources: AsyncSeq2<'T seq>) = // NOTE: we cannot use flex types on two overloads
            Internal.checkNonNull (nameof sources) sources

            algorithmSeq {
                for ts in sources do
                    // no null-check of inner seqs, similar to seq
                    yield! ts
            }

        static member concat(sources: AsyncSeq2<'T[]>) =
            Internal.checkNonNull (nameof sources) sources

            algorithmSeq {
                for ts in sources do
                    // no null-check of inner arrays, similar to seq
                    yield! ts
            }

        static member concat(sources: AsyncSeq2<'T list>) =
            Internal.checkNonNull (nameof sources) sources

            algorithmSeq {
                for ts in sources do
                    // no null-check of inner lists, similar to seq
                    yield! ts
            }

        static member concat(sources: AsyncSeq2<ResizeArray<'T>>) =
            Internal.checkNonNull (nameof sources) sources

            algorithmSeq {
                for ts in sources do
                    // no null-check of inner resize arrays, similar to seq
                    yield! ts
            }

        static member appendSeq (source1: AsyncSeq2<'T>) (source2: seq<'T>) =
            Internal.checkNonNull (nameof source1) source1
            Internal.checkNonNull (nameof source2) source2

            algorithmSeq {
                yield! source1
                yield! source2
            }

        static member prependSeq (source1: seq<'T>) (source2: AsyncSeq2<'T>) =
            Internal.checkNonNull (nameof source1) source1
            Internal.checkNonNull (nameof source2) source2

            algorithmSeq {
                yield! source1
                yield! source2
            }

        //
        // iter/map/collect functions
        //

        static member cast source : AsyncSeq2<'T> = Internal.map (SimpleAction(fun (x: obj) -> x :?> 'T)) source
        static member box source = Internal.map (SimpleAction box) source
        static member unbox<'U when 'U: struct>(source: AsyncSeq2<obj>) : AsyncSeq2<'U> = Internal.map (SimpleAction unbox) source
        static member iteri action source =
            AsyncSeq2Cold.terminal source (Internal.iter (CountableAction action))
        static member iteriAsync action source =
            AsyncSeq2.mapiAsync action source |> AsyncSeq2.iter ignore
        static member mapi (mapper: int -> 'T -> 'U) source = Internal.map (CountableAction mapper) source
        static member mapiAsync (mapper: int -> 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable index = 0
                for value in source do
                    let! result = mapper index value
                    yield result
                    index <- index + 1
            }
        static member collectSeq (binder: 'T -> #seq<'U>) source = Internal.collectSeq binder source
        static member collectAsync (binder: 'T -> Async2<AsyncSeq2<'U>>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for value in source do
                    let! inner = binder value
                    yield! inner
            }
        static member collectSeqAsync (binder: 'T -> Async2<seq<'U>>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for value in source do
                    let! inner = binder value
                    yield! inner
            }

        //
        // choosers, pickers and the like
        //

        static member tryHead(source: AsyncSeq2<'T>) : Async2<'T option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let! next = enumerator.MoveNextAsync()
                return if next then Some enumerator.Current else None
            }

        static member head source =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! value = AsyncSeq2.tryHead source
                return value |> Option.defaultWith Internal.raiseEmptySeq
            }

        static member tryLast(source: AsyncSeq2<'T>) : Async2<'T option> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let mutable last = None
                for value in source do
                    last <- Some value
                return last
            }

        static member last source =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! value = AsyncSeq2.tryLast source
                return value |> Option.defaultWith Internal.raiseEmptySeq
            }

        static member firstOrDefault defaultValue source =
            AsyncSeq2Cold.terminal source (Internal.firstOrDefault defaultValue)
        static member lastOrDefault defaultValue source =
            AsyncSeq2Cold.terminal source (Internal.lastOrDefault defaultValue)
        static member splitAt count source =
            if count < 0 then invalidArg (nameof count) "The value must be non-negative."
            AsyncSeq2Cold.terminal source (Internal.splitAt count)

        static member tryTail source = AsyncSeq2Cold.terminal source Internal.tryTail

        static member tail source =
            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryTail values |> Task.map (Option.defaultWith Internal.raiseEmptySeq))

        static member tryItem index source = AsyncSeq2Cold.terminal source (Internal.tryItem index)

        static member item index source =
            if index < 0 then
                invalidArg (nameof index) "The input must be non-negative."

            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryItem index values |> Task.map (Option.defaultWith Internal.raiseInsufficient))

        static member tryExactlyOne source = AsyncSeq2Cold.terminal source Internal.tryExactlyOne

        static member exactlyOne source =
            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryExactlyOne values
                |> Task.map (Option.defaultWith (fun () -> invalidArg (nameof source) "The input sequence contains more than one element.")))

        static member indexed(source: AsyncSeq2<'T>) =
            Internal.checkNonNull (nameof source) source

            algorithmSeq {
                let mutable i = 0

                for x in source do
                    yield i, x
                    i <- i + 1
            }

        static member choose chooser source = Internal.choose (TryPick chooser) source
        static member chooseV chooser source = Internal.chooseV (TryPickV chooser) source
        static member chooseAsync (chooser: 'T -> Async2<'U option>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for value in source do
                    match! chooser value with
                    | Some result -> yield result
                    | None -> ()
            }
        static member chooseVAsync (chooser: 'T -> Async2<'U voption>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for value in source do
                    match! chooser value with
                    | ValueSome result -> yield result
                    | ValueNone -> ()
            }

        static member filterAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                for value in source do
                    let! keep = predicate value
                    if keep then yield value
            }
        static member where predicate source = Internal.filter (Predicate predicate) source
        static member whereAsync predicate source = AsyncSeq2.filterAsync predicate source

        static member skip count source = Internal.skipOrTake Skip count source
        static member drop count source = Internal.skipOrTake Drop count source
        static member take count source = Internal.skipOrTake Take count source
        static member truncate count source = Internal.skipOrTake Truncate count source

        static member takeWhile predicate source = Internal.takeWhile false (Predicate predicate) source
        static member takeWhileAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable taking = true
                while taking do
                    let! next = enumerator.MoveNextAsync()
                    if next then
                        let! keep = predicate enumerator.Current
                        if keep then yield enumerator.Current
                        else taking <- false
                    else
                        taking <- false
            }
        static member takeWhileInclusive predicate source = Internal.takeWhile true (Predicate predicate) source
        static member takeWhileInclusiveAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable taking = true
                while taking do
                    let! next = enumerator.MoveNextAsync()
                    if next then
                        let! keep = predicate enumerator.Current
                        yield enumerator.Current
                        taking <- keep
                    else
                        taking <- false
            }
        static member skipWhile predicate source = Internal.skipWhile false (Predicate predicate) source
        static member skipWhileAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable skipping = true
                for value in source do
                    if skipping then
                        let! skip = predicate value
                        skipping <- skip
                    if not skipping then yield value
            }
        static member skipWhileInclusive predicate source = Internal.skipWhile true (Predicate predicate) source
        static member skipWhileInclusiveAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable skipping = true
                for value in source do
                    if skipping then
                        let! skip = predicate value
                        skipping <- skip
                    else yield value
            }

        static member tryPick chooser source =
            AsyncSeq2Cold.terminal source (Internal.tryPick (TryPick chooser))
        static member tryPickAsync chooser source =
            AsyncSeq2.chooseAsync chooser source |> AsyncSeq2.tryHead
        static member tryFind predicate source =
            AsyncSeq2Cold.terminal source (Internal.tryFind (Predicate predicate))
        static member tryFindAsync predicate source =
            AsyncSeq2.filterAsync predicate source |> AsyncSeq2.tryHead
        static member tryFindIndex predicate source =
            AsyncSeq2Cold.terminal source (Internal.tryFindIndex (Predicate predicate))
        static member tryFindIndexAsync predicate source =
            AsyncSeq2.indexed source
            |> AsyncSeq2.filterAsync (fun (_, value) -> predicate value)
            |> AsyncSeq2.tryHead
            |> Async2.map (Option.map fst)

        static member insertAt index value source = Internal.insertAt index (One value) source
        static member insertManyAt index values source = Internal.insertAt index (Many values) source
        static member removeAt index source = Internal.removeAt index source
        static member removeManyAt index count source = Internal.removeManyAt index count source
        static member updateAt index value source = Internal.updateAt index value source

        static member except itemsToExclude source = Internal.except itemsToExclude source
        static member exceptOfSeq itemsToExclude source = Internal.exceptOfSeq itemsToExclude source

        static member distinct source = Internal.distinct source
        static member distinctBy projection source = Internal.distinctBy projection source
        static member distinctByAsync (projection: 'T -> Async2<'Key>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let seen = HashSet<'Key>(HashIdentity.Structural)
                for value in source do
                    let! key = projection value
                    if seen.Add key then yield value
            }

        static member distinctUntilChanged source = Internal.distinctUntilChanged source
        static member distinctUntilChangedWith comparer source = Internal.distinctUntilChangedWith comparer source
        static member distinctUntilChangedWithAsync (comparer: 'T -> 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable previous = ValueNone
                for value in source do
                    match previous with
                    | ValueNone ->
                        yield value
                        previous <- ValueSome value
                    | ValueSome last ->
                        let! same = comparer last value
                        if not same then
                            yield value
                            previous <- ValueSome value
            }
        static member pairwise source = Internal.pairwise source
        static member chunkBySize chunkSize source = Internal.chunkBySize chunkSize source
        static member chunkBy projection source = Internal.chunkBy projection source
        static member chunkByAsync (projection: 'T -> Async2<'Key>) (source: AsyncSeq2<'T>) : AsyncSeq2<'Key * 'T[]> =
            let project value = async2 {
                let! key = projection value
                return key, value
            }
            let projected = AsyncSeq2.mapAsync project source
            Internal.chunkBy fst projected
            |> AsyncSeq2.map (fun (key, items) -> key, Array.map snd items)
        static member windowed windowSize source = Internal.windowed windowSize source

        static member forall predicate source =
            AsyncSeq2.exists (predicate >> not) source |> Async2.map not

        static member forallAsync predicate source =
            AsyncSeq2.existsAsync (fun value -> async2 {
                let! matches = predicate value
                return not matches
            }) source |> Async2.map not

        static member exists predicate (source: AsyncSeq2<'T>) : Async2<bool> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable found = false
                let mutable reading = true
                while reading && not found do
                    let! next = enumerator.MoveNextAsync()
                    reading <- next
                    if next then found <- predicate enumerator.Current
                return found
            }

        static member existsAsync (predicate: 'T -> Async2<bool>) (source: AsyncSeq2<'T>) : Async2<bool> =
            Internal.checkNonNull (nameof source) source
            async2 {
                let! ct = Async2.CancellationToken
                use enumerator = source.GetAsyncEnumerator ct
                let mutable found = false
                let mutable reading = true
                while reading && not found do
                    let! next = enumerator.MoveNextAsync()
                    reading <- next
                    if next then
                        let! matches = predicate enumerator.Current
                        found <- matches
                return found
            }

        static member contains value source = AsyncSeq2.exists ((=) value) source

        static member pick chooser source =
            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryPick (TryPick chooser) values |> Task.map (Option.defaultWith Internal.raiseNotFound))

        static member pickAsync chooser source =
            AsyncSeq2.tryPickAsync chooser source
            |> Async2.map (Option.defaultWith Internal.raiseNotFound)

        static member find predicate source =
            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryFind (Predicate predicate) values |> Task.map (Option.defaultWith Internal.raiseNotFound))

        static member findAsync predicate source =
            AsyncSeq2.tryFindAsync predicate source
            |> Async2.map (Option.defaultWith Internal.raiseNotFound)

        static member findIndex predicate source =
            AsyncSeq2Cold.terminal source (fun values ->
                Internal.tryFindIndex (Predicate predicate) values |> Task.map (Option.defaultWith Internal.raiseNotFound))

        static member findIndexAsync predicate source =
            AsyncSeq2.tryFindIndexAsync predicate source
            |> Async2.map (Option.defaultWith Internal.raiseNotFound)

        //
        // zip/unzip/fold etc functions
        //

        static member zip source1 source2 = Internal.zip source1 source2
        static member zip3 source1 source2 source3 = Internal.zip3 source1 source2 source3
        static member zipWith mapping source1 source2 = Internal.zipWith mapping source1 source2
        static member zipWithAsync (mapping: 'T -> 'U -> Async2<'V>) source1 source2 =
            AsyncSeq2.zip source1 source2
            |> AsyncSeq2.mapAsync (fun (left, right) -> mapping left right)
        static member zipWith3 mapping source1 source2 source3 = Internal.zipWith3 mapping source1 source2 source3
        static member zipWithAsync3 (mapping: 'T -> 'U -> 'V -> Async2<'W>) source1 source2 source3 =
            AsyncSeq2.zip3 source1 source2 source3
            |> AsyncSeq2.mapAsync (fun (a, b, c) -> mapping a b c)
        static member compareWith comparer source1 source2 =
            AsyncSeq2Cold.binaryWithToken source1 source2 (fun _ -> Internal.compareWith comparer)
        static member compareWithAsync comparer source1 source2 =
            AsyncSeq2Cold.binaryAsync source1 source2 (fun first second -> async2 {
                let! ct = Async2.CancellationToken
                use left = first.GetAsyncEnumerator ct
                use right = second.GetAsyncEnumerator ct
                let mutable comparison = 0
                let! firstLeft = left.MoveNextAsync()
                let! firstRight = right.MoveNextAsync()
                let mutable hasLeft = firstLeft
                let mutable hasRight = firstRight
                while comparison = 0 && (hasLeft || hasRight) do
                    if not hasLeft then comparison <- -1
                    elif not hasRight then comparison <- 1
                    else
                        let! result = comparer left.Current right.Current
                        comparison <- result
                        if result = 0 then
                            let! nextLeft = left.MoveNextAsync()
                            let! nextRight = right.MoveNextAsync()
                            hasLeft <- nextLeft
                            hasRight <- nextRight
                return comparison
            })
        static member fold folder state source =
            AsyncSeq2Cold.terminal source (Internal.fold (FolderAction folder) state)
        static member foldAsync folder state source =
            AsyncSeq2.scanAsync folder state source |> AsyncSeq2.last
        static member foldWhile predicate folder state source =
            AsyncSeq2Cold.terminal source (Internal.foldWhile predicate folder state)

        static member foldWhileAsync predicate folder state source =
            AsyncSeq2Cold.terminalAsync source (fun source -> async2 {
                let! ct = Async2.CancellationToken
                use e = source.GetAsyncEnumerator ct
                let mutable result = state
                let mutable running = true
                while running do
                    let! next = e.MoveNextAsync()
                    if next then
                        let! keepGoing = predicate result e.Current
                        if keepGoing then
                            let! newState = folder result e.Current
                            result <- newState
                        else running <- false
                    else running <- false
                return result
            })

        static member scan folder state source = Internal.scan (FolderAction folder) state source
        static member scanAsync (folder: 'State -> 'T -> Async2<'State>) state (source: AsyncSeq2<'T>) : AsyncSeq2<'State> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable current = state
                yield current
                for value in source do
                    let! next = folder current value
                    current <- next
                    yield current
            }
        static member reduce folder source = AsyncSeq2Cold.terminal source (Internal.reduce (FolderAction folder))
        static member reduceAsync folder source =
            AsyncSeq2Cold.terminalAsync source (fun source -> async2 {
                let! ct = Async2.CancellationToken
                use e = source.GetAsyncEnumerator ct
                let! first = e.MoveNextAsync()
                if not first then Internal.raiseEmptySeq ()
                let mutable result = e.Current
                let mutable running = true
                while running do
                    let! next = e.MoveNextAsync()
                    if next then
                        let! updated = folder result e.Current
                        result <- updated
                    else running <- false
                return result
            })

        //
        // groupBy/countBy/partition
        //

        static member groupBy projection source =
            AsyncSeq2Cold.terminal source (Internal.groupBy (ProjectorAction projection))
        static member groupByAsync projection source =
            AsyncSeq2Cold.keyed projection source
            |> fun keyed -> AsyncSeq2Cold.terminal keyed (Internal.groupBy (ProjectorAction fst))
            |> Async2.map (Array.map (fun (key, values) -> key, Array.map snd values))
        static member countBy projection source =
            AsyncSeq2Cold.terminal source (Internal.countBy (ProjectorAction projection))
        static member countByAsync projection source =
            AsyncSeq2Cold.keyed projection source
            |> fun keyed -> AsyncSeq2Cold.terminal keyed (Internal.countBy (ProjectorAction fst))
        static member partition predicate source =
            AsyncSeq2Cold.terminal source (Internal.partition (Predicate predicate))
        static member partitionAsync predicate source =
            AsyncSeq2Cold.keyed predicate source
            |> fun keyed -> AsyncSeq2Cold.terminal keyed (Internal.partition (Predicate fst))
            |> Async2.map (fun (yes, no) -> Array.map snd yes, Array.map snd no)
        static member mapFold mapping state source =
            AsyncSeq2Cold.terminal source (Internal.mapFold (MapFolderAction mapping) state)
        static member mapFoldAsync mapping state source =
            AsyncSeq2Cold.terminalAsync source (fun source -> async2 {
                let results = ResizeArray()
                let mutable current = state
                for value in source do
                    let! result, next = mapping current value
                    results.Add result
                    current <- next
                return results.ToArray(), current
            })
        static member threadState folder state source = Internal.threadState folder state source
        static member threadStateAsync (folder: 'State -> 'T -> Async2<'U * 'State>) state (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
            Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let mutable current = state
                for value in source do
                    let! result, next = folder current value
                    current <- next
                    yield result
            }
