module AsyncSeq2.Tests.Filter

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.filter
// AsyncSeq2.filterAsync
// AsyncSeq2.where
// AsyncSeq2.whereAsync
//


module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-filter or where with null source raises`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.filter (fun _ -> false) null

        assertNullArg
        <| fun () -> AsyncSeq2.filterAsync (fun _ -> async2 { return false }) null

        assertNullArg
        <| fun () -> AsyncSeq2.where (fun _ -> false) null

        assertNullArg
        <| fun () -> AsyncSeq2.whereAsync (fun _ -> async2 { return false }) null


    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-filter or where has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.filter ((=) 12)
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)

        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.where ((=) 12)
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-filterAsync or whereAsync has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.filterAsync (fun x -> async2 { return x = 12 })
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)

        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.whereAsync (fun x -> async2 { return x = 12 })
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filter or where filters correctly`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.filter ((<=) 5) // greater than
            |> verifyDigitsAsString "EFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.where ((>) 5) // greater than
            |> verifyDigitsAsString "ABCD"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filterAsync or whereAsync filters correctly`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.filterAsync (fun x -> async2 { return x <= 5 })
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.whereAsync (fun x -> async2 { return x > 5 })
            |> verifyDigitsAsString "FGHIJ"

    }

module Immutable2 =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filter keeps all when predicate is always true`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.filter (fun _ -> true)
        |> verify1To10

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filterAsync keeps all when predicate is always true`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.filterAsync (fun _ -> async2 { return true })
        |> verify1To10

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filter returns empty when predicate is always false`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.filter (fun _ -> false)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-filterAsync returns empty when predicate is always false`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.filterAsync (fun _ -> async2 { return false })
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-filter evaluates each element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> AsyncSeq2.filter (fun x -> x % 2 = 0)
            |> ColdTask.toListAsync

        count |> should equal 5
        xs |> should equal [ 2; 4 ]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-filter filters correctly`` variant = task {
        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.filter ((<=) 5) // greater than or equal
            |> verifyDigitsAsString "EFGHIJ"

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.where ((>) 5) // less than
            |> verifyDigitsAsString "ABCD"
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-filterAsync filters correctly`` variant = task {
        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.filterAsync (fun x -> async2 { return x <= 5 })
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.whereAsync (fun x -> async2 { return x > 5 && x < 9 })
            |> verifyDigitsAsString "FGH"
    }
