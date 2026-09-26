module AsyncSeq2.Tests.ChunkBy

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.chunkBy
// AsyncSeq2.chunkByAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.chunkBy id (null: AsyncSeq2<int>)

        assertNullArg
        <| fun () -> AsyncSeq2.chunkByAsync (fun x -> async2 { return x }) (null: AsyncSeq2<int>)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chunkBy on empty gives empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.chunkBy id
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chunkByAsync on empty gives empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.chunkByAsync (fun x -> async2 { return x })
        |> verifyEmpty


module Functionality =
    [<Fact>]
    let ``AsyncSeq2-chunkBy groups consecutive equal elements`` () = task {
        let ts = asyncSeq2 { yield! [ 1; 1; 2; 2; 2; 3 ] }
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 3
        result[0] |> should equal (1, [| 1; 1 |])
        result[1] |> should equal (2, [| 2; 2; 2 |])
        result[2] |> should equal (3, [| 3 |])
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy with all same key yields one chunk`` () = task {
        let ts = asyncSeq2 { yield! [ 5; 5; 5; 5 ] }
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 1
        result[0] |> should equal (5, [| 5; 5; 5; 5 |])
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy with all different keys yields singleton chunks`` () = task {
        let ts = asyncSeq2 { yield! [ 1..5 ] }
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 5

        result
        |> Array.iteri (fun i (k, arr) ->
            k |> should equal (i + 1)
            arr |> should equal [| i + 1 |])
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy with singleton source yields one chunk`` () = task {
        let ts = AsyncSeq2.singleton 42
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 1
        result[0] |> should equal (42, [| 42 |])
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy uses projection key, not element`` () = task {
        let ts = asyncSeq2 {
            yield "a1"
            yield "a2"
            yield "b1"
            yield "b2"
            yield "a3"
        }

        let! result =
            AsyncSeq2.chunkBy (fun (s: string) -> s[0]) ts
            |> ColdTask.toArrayAsync

        result |> should haveLength 3
        let k0, arr0 = result[0]
        k0 |> should equal 'a'
        arr0 |> should equal [| "a1"; "a2" |]
        let k1, arr1 = result[1]
        k1 |> should equal 'b'
        arr1 |> should equal [| "b1"; "b2" |]
        let k2, arr2 = result[2]
        k2 |> should equal 'a'
        arr2 |> should equal [| "a3" |]
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy does not merge non-consecutive equal keys`` () = task {
        // Key alternates: 1, 2, 1, 2 — should produce 4 chunks not 2
        let ts = asyncSeq2 { yield! [ 1; 2; 1; 2 ] }
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 4
    }

    [<Fact>]
    let ``AsyncSeq2-chunkByAsync groups consecutive by async key`` () = task {
        let ts = asyncSeq2 { yield! [ 1; 1; 2; 3; 3 ] }

        let! result =
            AsyncSeq2.chunkByAsync (fun x -> async2 { return x % 2 = 0 }) ts
            |> ColdTask.toArrayAsync
        // odd, even, odd -> 3 chunks
        result |> should haveLength 3
        let k0, arr0 = result[0]
        k0 |> should equal false
        arr0 |> should equal [| 1; 1 |]
        let k1, arr1 = result[1]
        k1 |> should equal true
        arr1 |> should equal [| 2 |]
        let k2, arr2 = result[2]
        k2 |> should equal false
        arr2 |> should equal [| 3; 3 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBy all elements same key as variants`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! result = AsyncSeq2.chunkBy (fun _ -> 0) ts |> ColdTask.toArrayAsync
        result |> should haveLength 1
        let _, arr = result[0]
        arr |> should haveLength 10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkBy each element its own chunk as variants`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! result = AsyncSeq2.chunkBy id ts |> ColdTask.toArrayAsync
        result |> should haveLength 10

        result
        |> Array.iteri (fun i (k, arr) ->
            k |> should equal (i + 1)
            arr |> should haveLength 1
            arr[0] |> should equal (i + 1))
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chunkByAsync all elements same key as variants`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! result =
            AsyncSeq2.chunkByAsync (fun _ -> async2 { return 0 }) ts
            |> ColdTask.toArrayAsync

        result |> should haveLength 1
        let _, arr = result[0]
        arr |> should haveLength 10
    }


module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chunkBy on side-effect seq groups all elements under one key`` variant = task {
        let! result =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.chunkBy (fun _ -> 0)
            |> ColdTask.toArrayAsync

        result |> should haveLength 1
        let _, arr = result[0]
        arr |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chunkByAsync on side-effect seq groups all elements under one key`` variant = task {
        let! result =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.chunkByAsync (fun _ -> async2 { return 0 })
            |> ColdTask.toArrayAsync

        result |> should haveLength 1
        let _, arr = result[0]
        arr |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chunkBy on side-effect seq produces correct singleton chunks`` variant = task {
        let! result =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.chunkBy id
            |> ColdTask.toArrayAsync

        result |> should haveLength 10

        result
        |> Array.iteri (fun i (k, arr) ->
            k |> should equal (i + 1)
            arr |> should equal [| i + 1 |])
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy projection is called exactly once per element`` () = task {
        let mutable callCount = 0

        let ts = asyncSeq2 { yield! [ 1; 1; 2; 3; 3 ] }

        let! result =
            ts
            |> AsyncSeq2.chunkBy (fun x ->
                callCount <- callCount + 1
                x % 2)
            |> ColdTask.toArrayAsync

        callCount |> should equal 5
        result |> should haveLength 3
    }

    [<Fact>]
    let ``AsyncSeq2-chunkByAsync projection is called exactly once per element`` () = task {
        let mutable callCount = 0

        let ts = asyncSeq2 { yield! [ 1; 1; 2; 3; 3 ] }

        let! result =
            ts
            |> AsyncSeq2.chunkByAsync (fun x -> async2 {
                callCount <- callCount + 1
                return x % 2
            })
            |> ColdTask.toArrayAsync

        callCount |> should equal 5
        result |> should haveLength 3
    }

    [<Fact>]
    let ``AsyncSeq2-chunkBy does not evaluate elements before enumeration`` () = task {
        let mutable sourceCount = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                sourceCount <- sourceCount + 1
                yield i
        }

        let chunked = AsyncSeq2.chunkBy (fun x -> x % 2 = 0) ts
        // Building the pipeline does not consume the source
        sourceCount |> should equal 0

        let! _ = ColdTask.toArrayAsync chunked
        // Only after consuming does the source get evaluated
        sourceCount |> should equal 5
    }
