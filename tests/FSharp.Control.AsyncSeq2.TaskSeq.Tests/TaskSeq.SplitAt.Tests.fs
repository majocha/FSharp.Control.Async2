module AsyncSeq2.Tests.SplitAt

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.splitAt
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () = assertNullArg <| fun () -> ColdTask.splitAt 0 null

    [<Fact>]
    let ``Negative count raises immediately`` () =
        fun () -> ColdTask.splitAt -1 (asyncSeq2 { yield 1 }) |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-splitAt 0 on empty gives empty prefix and empty rest`` variant = task {
        let! prefix, rest = Gen.getEmptyVariant variant |> ColdTask.splitAt 0
        prefix |> should haveLength 0
        do! verifyEmpty rest
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-splitAt n on empty gives empty prefix and empty rest`` variant = task {
        let! prefix, rest = Gen.getEmptyVariant variant |> ColdTask.splitAt 5
        prefix |> should haveLength 0
        do! verifyEmpty rest
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-splitAt 0 returns empty prefix and full rest`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! prefix, rest = ColdTask.splitAt 0 ts
        prefix |> should haveLength 0
        let! restArr = ColdTask.toArrayAsync rest
        restArr |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-splitAt at length gives full prefix and empty rest`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! prefix, rest = ColdTask.splitAt 10 ts
        prefix |> should equal [| 1..10 |]
        do! verifyEmpty rest
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-splitAt in the middle`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! prefix, rest = ColdTask.splitAt 3 ts
        prefix |> should equal [| 1; 2; 3 |]
        let! restArr = ColdTask.toArrayAsync rest
        restArr |> should equal [| 4..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-splitAt 1 returns singleton prefix`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! prefix, rest = ColdTask.splitAt 1 ts
        prefix |> should equal [| 1 |]
        let! restArr = ColdTask.toArrayAsync rest
        restArr |> should equal [| 2..10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-splitAt beyond length gives full prefix and empty rest`` () = task {
        let ts = asyncSeq2 {
            yield 1
            yield 2
            yield 3
        }

        let! prefix, rest = ColdTask.splitAt 100 ts
        prefix |> should equal [| 1; 2; 3 |]
        do! verifyEmpty rest
    }

    [<Fact>]
    let ``AsyncSeq2-splitAt prefix and rest together contain all elements`` () = task {
        let data = [| 1..20 |]
        let ts = AsyncSeq2.ofArray data
        let splitPoint = 7
        let! prefix, rest = ColdTask.splitAt splitPoint ts
        let! restArr = ColdTask.toArrayAsync rest
        let combined = Array.append prefix restArr
        combined |> should equal data
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-splitAt prefix gets first n elements from side-effect seq`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! prefix, _ = ColdTask.splitAt 3 ts
        prefix |> should equal [| 1; 2; 3 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-splitAt rest yields remaining elements from side-effect seq`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! _, rest = ColdTask.splitAt 3 ts
        let! restArr = ColdTask.toArrayAsync rest
        restArr |> should equal [| 4..10 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-splitAt prefix and rest together cover all elements of side-effect seq`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! prefix, rest = ColdTask.splitAt 5 ts
        let! restArr = ColdTask.toArrayAsync rest
        let combined = Array.append prefix restArr
        combined |> should equal [| 1..10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-splitAt rest is lazy: side effects in rest not triggered until consumed`` () = task {
        let mutable restSideEffectCount = 0

        let ts = asyncSeq2 {
            yield 1
            yield 2
            yield 3

            // These yields are in the "rest" portion
            restSideEffectCount <- restSideEffectCount + 1
            yield 4
            restSideEffectCount <- restSideEffectCount + 1
            yield 5
        }

        let! _prefix, rest = ColdTask.splitAt 3 ts
        // rest has NOT been consumed yet; the side effects in it should not have fired
        restSideEffectCount |> should equal 0

        // Now consume rest
        let! restArr = ColdTask.toArrayAsync rest
        restArr |> should equal [| 4; 5 |]
        restSideEffectCount |> should equal 2
    }

    [<Fact>]
    let ``AsyncSeq2-splitAt second evaluation of side-effect seq yields next batch`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ = 1 to 10 do
                i <- i + 1
                yield i
        }

        // First split
        let! prefix1, rest1 = ColdTask.splitAt 4 ts
        let! restArr1 = ColdTask.toArrayAsync rest1
        prefix1 |> should equal [| 1; 2; 3; 4 |]
        restArr1 |> should equal [| 5..10 |]

        // Second split of the same 'ts' uses the same 'i' capture; i is now 10
        let! prefix2, rest2 = ColdTask.splitAt 4 ts
        let! restArr2 = ColdTask.toArrayAsync rest2
        prefix2 |> should equal [| 11; 12; 13; 14 |]
        restArr2 |> should equal [| 15..20 |]
    }
