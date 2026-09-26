module AsyncSeq2.Tests.Pairwise

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.pairwise
//


module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-pairwise with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.pairwise null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-pairwise on empty returns empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.pairwise
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-pairwise on singleton returns empty`` () = asyncSeq2 { yield 42 } |> AsyncSeq2.pairwise |> verifyEmpty

module Immutable =
    [<Fact>]
    let ``AsyncSeq2-pairwise on two elements returns one pair`` () = task {
        let! pairs =
            asyncSeq2 { yield! [ 10; 20 ] }
            |> AsyncSeq2.pairwise
            |> ColdTask.toListAsync

        pairs |> should equal [ (10, 20) ]
    }

    [<Fact>]
    let ``AsyncSeq2-pairwise returns consecutive overlapping pairs`` () = task {
        let! pairs =
            asyncSeq2 { yield! [ 1..5 ] }
            |> AsyncSeq2.pairwise
            |> ColdTask.toListAsync

        pairs |> should equal [ (1, 2); (2, 3); (3, 4); (4, 5) ]
    }

    [<Fact>]
    let ``AsyncSeq2-pairwise output length is source length minus one`` () = task {
        let! len =
            asyncSeq2 { yield! [ 1..10 ] }
            |> AsyncSeq2.pairwise
            |> AsyncSeq2.length |> Async2.StartAsTask

        len |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-pairwise shares elements across adjacent pairs`` () = task {
        // element at index i is the right of pair i-1 and the left of pair i
        let! pairs =
            asyncSeq2 { yield! [ 'A'; 'B'; 'C'; 'D' ] }
            |> AsyncSeq2.pairwise
            |> ColdTask.toListAsync

        pairs |> should equal [ ('A', 'B'); ('B', 'C'); ('C', 'D') ]
        // check that middle elements appear in both adjacent pairs
        let (_, r0) = pairs[0]
        let (l1, _) = pairs[1]
        r0 |> should equal l1
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pairwise all variants - correct count and boundaries`` variant = task {
        // getSeqImmutable yields 1..10 → 9 pairs (1,2)..(9,10)
        let! pairs =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.pairwise
            |> ColdTask.toListAsync

        pairs |> List.length |> should equal 9
        pairs |> List.head |> should equal (1, 2)
        pairs |> List.last |> should equal (9, 10)
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-pairwise consumes every source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! pairs = ts |> AsyncSeq2.pairwise |> ColdTask.toListAsync
        count |> should equal 5
        pairs |> should equal [ (1, 2); (2, 3); (3, 4); (4, 5) ]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-pairwise on side-effect seq yields correct pairs`` variant = task {
        // getSeqWithSideEffect yields 1..10 on first iteration
        let! pairs =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.pairwise
            |> ColdTask.toListAsync

        pairs |> List.length |> should equal 9
        pairs |> List.head |> should equal (1, 2)
        pairs |> List.last |> should equal (9, 10)
    }
