module AsyncSeq2.Tests.GroupBy

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.groupBy
// ColdTask.groupByAsync
// ColdTask.countBy
// ColdTask.countByAsync
// ColdTask.partition
// ColdTask.partitionAsync
//

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-groupBy with null source raises`` () =
        assertNullArg <| fun () -> ColdTask.groupBy id null

        assertNullArg
        <| fun () -> ColdTask.groupByAsync (fun x -> Task.fromResult x) null

    [<Fact>]
    let ``AsyncSeq2-countBy with null source raises`` () =
        assertNullArg <| fun () -> ColdTask.countBy id null

        assertNullArg
        <| fun () -> ColdTask.countByAsync (fun x -> Task.fromResult x) null

    [<Fact>]
    let ``AsyncSeq2-partition with null source raises`` () =
        assertNullArg
        <| fun () -> ColdTask.partition (fun _ -> true) null

        assertNullArg
        <| fun () -> ColdTask.partitionAsync (fun _ -> Task.fromResult true) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-groupBy on empty sequence returns empty array`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.groupBy (fun x -> x % 2)

        result |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-groupByAsync on empty sequence returns empty array`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.groupByAsync (fun x -> task { return x % 2 })

        result |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-countBy on empty sequence returns empty array`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.countBy (fun x -> x % 2)

        result |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-countByAsync on empty sequence returns empty array`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.countByAsync (fun x -> task { return x % 2 })

        result |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-partition on empty sequence returns two empty arrays`` variant = task {
        let! trueItems, falseItems =
            Gen.getEmptyVariant variant
            |> ColdTask.partition (fun _ -> true)

        trueItems |> should be Empty
        falseItems |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-partitionAsync on empty sequence returns two empty arrays`` variant = task {
        let! trueItems, falseItems =
            Gen.getEmptyVariant variant
            |> ColdTask.partitionAsync (fun _ -> Task.fromResult true)

        trueItems |> should be Empty
        falseItems |> should be Empty
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-groupBy groups by even/odd`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.groupBy (fun x -> x % 2 = 0)

        // should have exactly two groups
        result |> Array.length |> should equal 2

        let falseKey, oddItems = result[0] // 1 is first, so 'false' (odd) comes first
        let trueKey, evenItems = result[1]
        falseKey |> should equal false
        trueKey |> should equal true
        oddItems |> should equal [| 1; 3; 5; 7; 9 |]
        evenItems |> should equal [| 2; 4; 6; 8; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-groupByAsync groups by even/odd`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.groupByAsync (fun x -> task { return x % 2 = 0 })

        result |> Array.length |> should equal 2

        let falseKey, oddItems = result[0]
        let trueKey, evenItems = result[1]
        falseKey |> should equal false
        trueKey |> should equal true
        oddItems |> should equal [| 1; 3; 5; 7; 9 |]
        evenItems |> should equal [| 2; 4; 6; 8; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-groupBy with identity projection produces one group per element`` variant = task {
        let! result = Gen.getSeqImmutable variant |> ColdTask.groupBy id

        result |> Array.length |> should equal 10

        for key, items in result do
            items |> should equal [| key |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-groupBy preserves first-occurrence key ordering`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.groupBy (fun x -> x % 3)

        // 1 % 3 = 1 → first key is 1
        // 2 % 3 = 2 → second key is 2
        // 3 % 3 = 0 → third key is 0
        let keys = result |> Array.map fst
        keys |> should equal [| 1; 2; 0 |]

        let _, group1 = result[0] // remainder 1: 1, 4, 7, 10
        let _, group2 = result[1] // remainder 2: 2, 5, 8
        let _, group0 = result[2] // remainder 0: 3, 6, 9
        group1 |> should equal [| 1; 4; 7; 10 |]
        group2 |> should equal [| 2; 5; 8 |]
        group0 |> should equal [| 3; 6; 9 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-groupBy with constant key produces single group`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.groupBy (fun _ -> "same")

        result |> Array.length |> should equal 1
        let key, items = result[0]
        key |> should equal "same"
        items |> should equal [| 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-countBy counts by even/odd`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.countBy (fun x -> x % 2 = 0)

        result |> Array.length |> should equal 2

        let falseKey, oddCount = result[0]
        let trueKey, evenCount = result[1]
        falseKey |> should equal false
        trueKey |> should equal true
        oddCount |> should equal 5
        evenCount |> should equal 5
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-countByAsync counts by even/odd`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.countByAsync (fun x -> task { return x % 2 = 0 })

        result |> Array.length |> should equal 2

        let falseKey, oddCount = result[0]
        let trueKey, evenCount = result[1]
        falseKey |> should equal false
        trueKey |> should equal true
        oddCount |> should equal 5
        evenCount |> should equal 5
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-countBy preserves first-occurrence key ordering`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.countBy (fun x -> x % 3)

        let keys = result |> Array.map fst
        keys |> should equal [| 1; 2; 0 |]

        let _, count1 = result[0] // remainder 1: 1, 4, 7, 10 → 4 items
        let _, count2 = result[1] // remainder 2: 2, 5, 8 → 3 items
        let _, count0 = result[2] // remainder 0: 3, 6, 9 → 3 items
        count1 |> should equal 4
        count2 |> should equal 3
        count0 |> should equal 3
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-countBy with constant key counts all`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> ColdTask.countBy (fun _ -> "same")

        result |> should equal [| "same", 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partition splits by even`` variant = task {
        let! evens, odds =
            Gen.getSeqImmutable variant
            |> ColdTask.partition (fun x -> x % 2 = 0)

        evens |> should equal [| 2; 4; 6; 8; 10 |]
        odds |> should equal [| 1; 3; 5; 7; 9 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partitionAsync splits by even`` variant = task {
        let! evens, odds =
            Gen.getSeqImmutable variant
            |> ColdTask.partitionAsync (fun x -> task { return x % 2 = 0 })

        evens |> should equal [| 2; 4; 6; 8; 10 |]
        odds |> should equal [| 1; 3; 5; 7; 9 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partition with always-true predicate puts all in first array`` variant = task {
        let! trueItems, falseItems =
            Gen.getSeqImmutable variant
            |> ColdTask.partition (fun _ -> true)

        trueItems
        |> should equal [| 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 |]

        falseItems |> should be Empty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partition with always-false predicate puts all in second array`` variant = task {
        let! trueItems, falseItems =
            Gen.getSeqImmutable variant
            |> ColdTask.partition (fun _ -> false)

        trueItems |> should be Empty

        falseItems
        |> should equal [| 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partition preserves element order within each partition`` variant = task {
        let! trueItems, falseItems =
            Gen.getSeqImmutable variant
            |> ColdTask.partition (fun x -> x <= 5)

        trueItems |> should equal [| 1; 2; 3; 4; 5 |]
        falseItems |> should equal [| 6; 7; 8; 9; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-partitionAsync preserves element order within each partition`` variant = task {
        let! trueItems, falseItems =
            Gen.getSeqImmutable variant
            |> ColdTask.partitionAsync (fun x -> task { return x <= 5 })

        trueItems |> should equal [| 1; 2; 3; 4; 5 |]
        falseItems |> should equal [| 6; 7; 8; 9; 10 |]
    }


module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-groupBy groups side-effecting sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! result = ts |> ColdTask.groupBy (fun x -> x % 2 = 0)

        result |> Array.length |> should equal 2
        // re-evaluating yields new side-effects (next 10 items: 11..20)
        let! result2 = ts |> ColdTask.groupBy (fun x -> x % 2 = 0)
        result2 |> Array.length |> should equal 2
        let _, group2 = result2[0]
        group2 |> Array.sum |> should equal (11 + 13 + 15 + 17 + 19) // odd items from 11–20
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-countBy counts side-effecting sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! result = ts |> ColdTask.countBy (fun x -> x % 2 = 0)

        // 5 odd, 5 even from 1..10
        let falseKey, oddCount = result[0]
        let trueKey, evenCount = result[1]
        falseKey |> should equal false
        trueKey |> should equal true
        oddCount |> should equal 5
        evenCount |> should equal 5
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-partition splits side-effecting sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! evens, odds = ts |> ColdTask.partition (fun x -> x % 2 = 0)

        evens |> should equal [| 2; 4; 6; 8; 10 |]
        odds |> should equal [| 1; 3; 5; 7; 9 |]

        // second call picks up side effects
        let! evens2, odds2 = ts |> ColdTask.partition (fun x -> x % 2 = 0)
        evens2 |> should equal [| 12; 14; 16; 18; 20 |]
        odds2 |> should equal [| 11; 13; 15; 17; 19 |]
    }
