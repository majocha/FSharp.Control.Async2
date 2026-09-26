module AsyncSeq2.Tests.ChunkBySize

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.chunkBySize
//

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-chunkBySize with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.chunkBySize 1 null

    [<Fact>]
    let ``AsyncSeq2-chunkBySize with zero raises ArgumentException before awaiting`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.chunkBySize 0 |> ignore // throws eagerly, before enumeration
        |> should throw typeof<System.ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-chunkBySize with negative raises ArgumentException before awaiting`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.chunkBySize -1 |> ignore
        |> should throw typeof<System.ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chunkBySize on empty sequence yields empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.chunkBySize 1
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chunkBySize(99) on empty sequence yields empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.chunkBySize 99
        |> verifyEmpty

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize preserves all elements in order`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 3
            |> AsyncSeq2.collect AsyncSeq2.ofArray
            |> verify1To10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize(2) returns 5 chunks of 2 for a 10-element sequence`` variant = task {
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 2
            |> ColdTask.toArrayAsync

        chunks
        |> should equal [| [| 1; 2 |]; [| 3; 4 |]; [| 5; 6 |]; [| 7; 8 |]; [| 9; 10 |] |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize(5) returns 2 full chunks for a 10-element sequence`` variant = task {
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 5
            |> ColdTask.toArrayAsync

        chunks |> should equal [| [| 1..5 |]; [| 6..10 |] |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize(1) returns each element as its own array`` variant = task {
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 1
            |> ColdTask.toArrayAsync

        chunks |> Array.length |> should equal 10

        chunks
        |> Array.iteri (fun i chunk -> chunk |> should equal [| i + 1 |])
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize last chunk contains remainder when sequence does not divide evenly`` variant = task {
        // 10 elements with chunk size 3 → chunks [1;2;3] [4;5;6] [7;8;9] [10]
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 3
            |> ColdTask.toArrayAsync

        chunks |> Array.length |> should equal 4
        chunks |> Array.last |> should equal [| 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize larger than sequence returns single chunk with all elements`` variant = task {
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 11
            |> ColdTask.toArrayAsync

        chunks |> Array.length |> should equal 1
        chunks.[0] |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize equal to sequence length returns single full chunk`` variant = task {
        let! chunks =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize 10
            |> ColdTask.toArrayAsync

        chunks |> Array.length |> should equal 1
        chunks.[0] |> should equal [| 1..10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBySize each chunk array is independent - modifying one does not affect others`` () = task {
        let! chunks =
            asyncSeq2 { yield! [ 1..6 ] }
            |> AsyncSeq2.chunkBySize 3
            |> ColdTask.toArrayAsync

        // Mutate the first chunk
        chunks.[0].[0] <- 99

        // The second chunk must be unaffected
        chunks.[1] |> should equal [| 4; 5; 6 |]
        chunks.[0].[0] |> should equal 99
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize remainder sizes`` variant = task {
        let verifyLastChunkSize chunkSize expectedLast =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.chunkBySize chunkSize
            |> ColdTask.toArrayAsync
            |> Task.map (Array.last >> Array.length >> should equal expectedLast)

        do! verifyLastChunkSize 3 1 // 10 mod 3 = 1
        do! verifyLastChunkSize 4 2 // 10 mod 4 = 2
        do! verifyLastChunkSize 6 4 // 10 mod 6 = 4
        do! verifyLastChunkSize 7 3 // 10 mod 7 = 3
        do! verifyLastChunkSize 9 1 // 10 mod 9 = 1
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chunkBySize gets all items`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.chunkBySize 5
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| [| 1..5 |]; [| 6..10 |] |])

    [<Fact>]
    let ``AsyncSeq2-chunkBySize executes side-effects from empty source`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            sideEffects <- sideEffects + 1
            sideEffects <- sideEffects + 1
        }

        do! ts |> AsyncSeq2.chunkBySize 1 |> consumeTaskSeq
        do! ts |> AsyncSeq2.chunkBySize 3 |> consumeTaskSeq
        sideEffects |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBySize executes all source side-effects`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            sideEffects <- sideEffects + 1
            yield 1
            sideEffects <- sideEffects + 1
            yield 2
            sideEffects <- sideEffects + 1 // executed even after last yield
        }

        do! ts |> AsyncSeq2.chunkBySize 2 |> consumeTaskSeq
        sideEffects |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBySize propagates exception from source`` () =
        let items = asyncSeq2 {
            yield 1
            yield 2
            failwith "boom"
            yield 3
        }

        fun () -> items |> AsyncSeq2.chunkBySize 2 |> consumeTaskSeq
        |> should throwAsyncExact typeof<System.Exception>
