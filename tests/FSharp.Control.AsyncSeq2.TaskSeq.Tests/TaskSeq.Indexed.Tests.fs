module AsyncSeq2.Tests.Indexed

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.indexed
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () = assertNullArg <| fun () -> AsyncSeq2.indexed null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-indexed on empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.indexed
        |> verifyEmpty

module Immutable =
    [<Fact>]
    let ``AsyncSeq2-indexed starts at zero`` () =
        asyncSeq2 { yield 99 }
        |> AsyncSeq2.indexed
        |> ColdTask.head
        |> Task.map (should equal (0, 99))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-indexed`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.indexed
        |> ColdTask.toArrayAsync
        |> Task.map (Array.forall (fun (x, y) -> x + 1 = y))
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-indexed returns all 10 pairs with correct zero-based indices`` variant = task {
        let! pairs =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.indexed
            |> ColdTask.toArrayAsync

        pairs |> should be (haveLength 10)

        pairs
        |> Array.iteri (fun pos (idx, _) -> idx |> should equal pos)
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-indexed returns values 1 to 10 unchanged`` variant = task {
        let! pairs =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.indexed
            |> ColdTask.toArrayAsync

        pairs |> Array.map snd |> should equal [| 1..10 |]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-indexed on side-effect sequence returns correct pairs`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! pairs = ts |> AsyncSeq2.indexed |> ColdTask.toArrayAsync
        pairs |> should be (haveLength 10)

        pairs
        |> Array.iteri (fun pos (idx, _) -> idx |> should equal pos)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-indexed on side-effect sequence is re-evaluated on second iteration`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! firstPairs = ts |> AsyncSeq2.indexed |> ColdTask.toArrayAsync
        let! secondPairs = ts |> AsyncSeq2.indexed |> ColdTask.toArrayAsync

        // indices always start at 0
        firstPairs |> Array.map fst |> should equal [| 0..9 |]
        secondPairs |> Array.map fst |> should equal [| 0..9 |]

        // values advance due to side effects
        firstPairs |> Array.map snd |> should equal [| 1..10 |]
        secondPairs |> Array.map snd |> should equal [| 11..20 |]
    }

    [<Fact>]
    let ``AsyncSeq2-indexed prove index starts at zero even after side effects`` () = task {
        let mutable counter = 0

        let ts = asyncSeq2 {
            for _ in 1..5 do
                counter <- counter + 1
                yield counter
        }

        let! pairs = ts |> AsyncSeq2.indexed |> ColdTask.toArrayAsync
        pairs |> Array.map fst |> should equal [| 0..4 |]
        pairs |> Array.map snd |> should equal [| 1..5 |]
    }
