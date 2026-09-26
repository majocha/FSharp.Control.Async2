namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic

/// Operations on asynchronous sequences, returning Async2 for terminal operations.
[<Sealed; AbstractClass>]
type AsyncSeq2 private () =

    static member private CheckSource(source: AsyncSeq2<'T>) =
        if isNull source then nullArg (nameof source)

    static member empty<'T> () : AsyncSeq2<'T> = asyncSeq2 { () }

    static member singleton(value: 'T) : AsyncSeq2<'T> = asyncSeq2 { yield value }

    static member ofSeq(source: seq<'T>) : AsyncSeq2<'T> =
        if isNull source then nullArg (nameof source)
        asyncSeq2 { yield! source }

    static member append(first: AsyncSeq2<'T>) (second: AsyncSeq2<'T>) : AsyncSeq2<'T> =
        AsyncSeq2.CheckSource first
        AsyncSeq2.CheckSource second
        asyncSeq2 {
            yield! first
            yield! second
        }

    static member map(projection: 'T -> 'U) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        AsyncSeq2.CheckSource source
        asyncSeq2 {
            for value in source do
                yield projection value
        }

    static member mapAsync(projection: 'T -> Async2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        AsyncSeq2.CheckSource source
        asyncSeq2 {
            for value in source do
                let! result = projection value
                yield result
        }

    static member filter(predicate: 'T -> bool) (source: AsyncSeq2<'T>) : AsyncSeq2<'T> =
        AsyncSeq2.CheckSource source
        asyncSeq2 {
            for value in source do
                if predicate value then
                    yield value
        }

    static member collect(projection: 'T -> AsyncSeq2<'U>) (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        AsyncSeq2.CheckSource source
        asyncSeq2 {
            for value in source do
                yield! projection value
        }

    static member toArray(source: AsyncSeq2<'T>) : Async2<'T array> =
        AsyncSeq2.CheckSource source
        async2 {
            let results = ResizeArray<'T>()
            for value in source do
                results.Add value
            return results.ToArray()
        }

    static member toList(source: AsyncSeq2<'T>) : Async2<'T list> =
        AsyncSeq2.CheckSource source
        async2 {
            let! results = AsyncSeq2.toArray source
            return List.ofArray results
        }

    static member iter(action: 'T -> unit) (source: AsyncSeq2<'T>) : Async2<unit> =
        AsyncSeq2.CheckSource source
        async2 {
            for value in source do
                action value
        }

    static member iterAsync(action: 'T -> Async2<unit>) (source: AsyncSeq2<'T>) : Async2<unit> =
        AsyncSeq2.CheckSource source
        async2 {
            for value in source do
                do! action value
        }

    static member isEmpty(source: AsyncSeq2<'T>) : Async2<bool> =
        AsyncSeq2.CheckSource source
        async2 {
            let! ct = Async2.CancellationToken
            use enumerator = source.GetAsyncEnumerator ct
            let! hasNext = enumerator.MoveNextAsync()
            return not hasNext
        }

    static member length(source: AsyncSeq2<'T>) : Async2<int> =
        AsyncSeq2.CheckSource source
        async2 {
            let mutable count = 0
            for _ in source do
                count <- count + 1
            return count
        }
