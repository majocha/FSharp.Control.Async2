module AsyncSeq2.Tests.CompareWith

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.compareWith
// ColdTask.compareWithAsync
//

let inline sign x =
    if x < 0 then -1
    elif x > 0 then 1
    else 0

module EmptySeq =
    [<Fact>]
    let ``Null source1 is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.compareWith compare null (AsyncSeq2.empty<int> ())

        assertNullArg
        <| fun () -> ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) null (AsyncSeq2.empty<int> ())

    [<Fact>]
    let ``Null source2 is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.compareWith compare (AsyncSeq2.empty<int> ()) null

        assertNullArg
        <| fun () -> ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) (AsyncSeq2.empty<int> ()) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-compareWith of two empty sequences is 0`` variant = task {
        let empty = Gen.getEmptyVariant variant
        let! result = ColdTask.compareWith compare empty (AsyncSeq2.empty<int> ())
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-compareWithAsync of two empty sequences is 0`` variant = task {
        let empty = Gen.getEmptyVariant variant
        let! result = ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) empty (AsyncSeq2.empty<int> ())
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-compareWith: empty source1 is less than non-empty source2`` variant = task {
        let empty = Gen.getEmptyVariant variant
        let! result = ColdTask.compareWith compare empty (AsyncSeq2.singleton 1)
        result |> should equal -1
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-compareWith: non-empty source1 is greater than empty source2`` variant = task {
        let empty = Gen.getEmptyVariant variant
        let! result = ColdTask.compareWith compare (AsyncSeq2.singleton 1) empty
        result |> should equal 1
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-compareWith: equal sequences return 0`` variant = task {
        let src1 = Gen.getSeqImmutable variant
        let src2 = Gen.getSeqImmutable variant
        let! result = ColdTask.compareWith compare src1 src2
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-compareWithAsync: equal sequences return 0`` variant = task {
        let src1 = Gen.getSeqImmutable variant
        let src2 = Gen.getSeqImmutable variant
        let! result = ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) src1 src2
        result |> should equal 0
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: first element differs`` () = task {
        let src1 = asyncSeq2 {
            1
            2
            3
        }

        let src2 = asyncSeq2 {
            2
            2
            3
        }

        let! result = ColdTask.compareWith compare src1 src2
        sign result |> should equal -1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: last element differs`` () = task {
        let src1 = asyncSeq2 {
            1
            2
            4
        }

        let src2 = asyncSeq2 {
            1
            2
            3
        }

        let! result = ColdTask.compareWith compare src1 src2
        sign result |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: source1 shorter returns negative`` () = task {
        let src1 = asyncSeq2 {
            1
            2
        }

        let src2 = asyncSeq2 {
            1
            2
            3
        }

        let! result = ColdTask.compareWith compare src1 src2
        result |> should equal -1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: source2 shorter returns positive`` () = task {
        let src1 = asyncSeq2 {
            1
            2
            3
        }

        let src2 = asyncSeq2 {
            1
            2
        }

        let! result = ColdTask.compareWith compare src1 src2
        result |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: uses custom comparer result sign`` () = task {
        // comparer returns a large number, not just -1/0/1
        let bigCompare (a: int) (b: int) = (a - b) * 100

        let src1 = asyncSeq2 {
            1
            2
            3
        }

        let src2 = asyncSeq2 {
            1
            2
            5
        }

        let! result = ColdTask.compareWith bigCompare src1 src2
        // 3 compared to 5 gives (3-5)*100 = -200, which is negative
        result |> should be (lessThan 0)
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith: stops at first non-zero comparison`` () = task {
        let mutable callCount = 0

        let countingCompare a b =
            callCount <- callCount + 1
            compare a b

        let src1 = asyncSeq2 {
            1
            99
            99
            99
        }

        let src2 = asyncSeq2 {
            2
            99
            99
            99
        }

        let! result = ColdTask.compareWith countingCompare src1 src2
        sign result |> should equal -1
        // Should stop after first comparison
        callCount |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWithAsync: async comparer works`` () = task {
        let src1 = asyncSeq2 {
            1
            2
            3
        }

        let src2 = asyncSeq2 {
            1
            2
            4
        }

        let! result =
            ColdTask.compareWithAsync
                (fun a b -> task {
                    // simulate async work with a yield point
                    return compare a b
                })
                src1
                src2

        sign result |> should equal -1
    }

    [<Fact>]
    let ``AsyncSeq2-compareWithAsync: async comparer works correctly`` () = task {
        let src1 = asyncSeq2 {
            10
            20
            30
        }

        let src2 = asyncSeq2 {
            10
            20
            30
        }

        let! result = ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) src1 src2
        result |> should equal 0
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-compareWith consumes both sequences exactly once when equal`` () = task {
        let mutable count1 = 0
        let mutable count2 = 0

        let src1 = asyncSeq2 {
            for i in 1..5 do
                count1 <- count1 + 1
                yield i
        }

        let src2 = asyncSeq2 {
            for i in 1..5 do
                count2 <- count2 + 1
                yield i
        }

        let! result = ColdTask.compareWith compare src1 src2
        result |> should equal 0
        count1 |> should equal 5
        count2 |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-compareWith stops consuming sources after first non-zero comparison`` () = task {
        let mutable count1 = 0
        let mutable count2 = 0

        let src1 = asyncSeq2 {
            for i in [ 1; 99; 99; 99; 99 ] do
                count1 <- count1 + 1
                yield i
        }

        let src2 = asyncSeq2 {
            for i in [ 2; 99; 99; 99; 99 ] do
                count2 <- count2 + 1
                yield i
        }

        let! result = ColdTask.compareWith compare src1 src2
        sign result |> should equal -1
        // compareWith calls MoveNextAsync for the first element of each source before entering the loop;
        // when the first comparison is non-zero the loop exits immediately without advancing further.
        count1 |> should equal 1
        count2 |> should equal 1
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-compareWith two fresh side-effect sequences compare as equal`` variant = task {
        // Each call to getSeqWithSideEffect creates an independent counter starting at 0,
        // so both sequences yield 1..10 and should compare as equal.
        let src1 = Gen.getSeqWithSideEffect variant
        let src2 = Gen.getSeqWithSideEffect variant
        let! result = ColdTask.compareWith compare src1 src2
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-compareWithAsync two fresh side-effect sequences compare as equal`` variant = task {
        let src1 = Gen.getSeqWithSideEffect variant
        let src2 = Gen.getSeqWithSideEffect variant
        let! result = ColdTask.compareWithAsync (fun a b -> Task.fromResult (compare a b)) src1 src2
        result |> should equal 0
    }
