module AsyncSeq2.Tests.DistinctUntilChanged

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.distinctUntilChanged / distinctUntilChangedWith / distinctUntilChangedWithAsync
//


module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.distinctUntilChanged null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinctUntilChanged has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith with null source raises`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.distinctUntilChangedWith (fun _ _ -> false) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinctUntilChangedWith has no effect on empty`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.distinctUntilChangedWith (fun _ _ -> false)
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync with null source raises`` () =
        assertNullArg
        <| fun () -> TaskCallbacks.distinctUntilChangedWithAsync (fun _ _ -> task { return false }) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync has no effect on empty`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> TaskCallbacks.distinctUntilChangedWithAsync (fun _ _ -> task { return false })
            |> ColdTask.toListAsync
            |> Task.map (List.isEmpty >> should be True)
    }

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged should return no consecutive duplicates`` () = task {
        let ts =
            [ 'A'; 'A'; 'B'; 'Z'; 'C'; 'C'; 'Z'; 'C'; 'D'; 'D'; 'D'; 'Z' ]
            |> AsyncSeq2.ofList

        let! xs = ts |> AsyncSeq2.distinctUntilChanged |> ColdTask.toListAsync

        xs
        |> List.map string
        |> String.concat ""
        |> should equal "ABZCZCDZ"
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged with single element returns singleton`` () = task {
        let! xs =
            asyncSeq2 { yield 42 }
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 42 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged with all identical elements returns one element`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 7; 7; 7; 7; 7 ] }
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 7 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged with all distinct elements returns all`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 1; 2; 3; 4; 5 ] }
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 1; 2; 3; 4; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged with alternating pairs`` () = task {
        // [A;A;B;B;A;A] -> [A;B;A]
        let! xs =
            asyncSeq2 { yield! [ 'A'; 'A'; 'B'; 'B'; 'A'; 'A' ] }
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 'A'; 'B'; 'A' ]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-distinctUntilChanged on immutable all-unique seq preserves all elements`` variant = task {
        // getSeqImmutable yields 1..10, all unique, so all are returned
        let! xs =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 1..10 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith with structural equality comparer behaves like distinctUntilChanged`` () = task {
        let ts =
            [ 'A'; 'A'; 'B'; 'Z'; 'C'; 'C'; 'Z'; 'C'; 'D'; 'D'; 'D'; 'Z' ]
            |> AsyncSeq2.ofList

        let! xs =
            ts
            |> AsyncSeq2.distinctUntilChangedWith (=)
            |> ColdTask.toListAsync

        xs
        |> List.map string
        |> String.concat ""
        |> should equal "ABZCZCDZ"
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith with always-true comparer returns only first element`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 1; 2; 3; 4; 5 ] }
            |> AsyncSeq2.distinctUntilChangedWith (fun _ _ -> true)
            |> ColdTask.toListAsync

        xs |> should equal [ 1 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith with always-false comparer returns all elements`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 1; 1; 2; 2; 3 ] }
            |> AsyncSeq2.distinctUntilChangedWith (fun _ _ -> false)
            |> ColdTask.toListAsync

        xs |> should equal [ 1; 1; 2; 2; 3 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith can use custom projection for equality`` () = task {
        // Treat values as equal if their absolute difference is <= 1
        let closeEnough a b = abs (a - b) <= 1

        let! xs =
            asyncSeq2 { yield! [ 10; 11; 9; 20; 21; 5 ] }
            |> AsyncSeq2.distinctUntilChangedWith closeEnough
            |> ColdTask.toListAsync

        // 10≈11 skip; 11≈9 skip (|11-9|=2? no, |11-9|=2>1, so keep 9); 9 vs 20 keep; 20≈21 skip; 21 vs 5 keep
        // Wait: |10-11|=1 skip 11; |10-9|=1 skip 9; 10 vs 20 keep 20; |20-21|=1 skip 21; 20 vs 5 keep 5
        xs |> should equal [ 10; 20; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith with single element returns singleton`` () = task {
        let! xs =
            asyncSeq2 { yield 99 }
            |> AsyncSeq2.distinctUntilChangedWith (fun _ _ -> true)
            |> ColdTask.toListAsync

        xs |> should equal [ 99 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith case-insensitive string comparison`` () = task {
        let! xs =
            asyncSeq2 { yield! [ "Hello"; "hello"; "HELLO"; "World"; "world" ] }
            |> AsyncSeq2.distinctUntilChangedWith (fun a b -> System.String.Compare(a, b, System.StringComparison.OrdinalIgnoreCase) = 0)
            |> ColdTask.toListAsync

        xs |> should equal [ "Hello"; "World" ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync with structural equality behaves like distinctUntilChanged`` () = task {
        let ts = [ 1; 1; 2; 3; 3; 4 ] |> AsyncSeq2.ofList

        let! xs =
            ts
            |> TaskCallbacks.distinctUntilChangedWithAsync (fun a b -> task { return a = b })
            |> ColdTask.toListAsync

        xs |> should equal [ 1; 2; 3; 4 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync with always-true async comparer returns only first element`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 10; 20; 30 ] }
            |> TaskCallbacks.distinctUntilChangedWithAsync (fun _ _ -> task { return true })
            |> ColdTask.toListAsync

        xs |> should equal [ 10 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync with always-false async comparer returns all elements`` () = task {
        let! xs =
            asyncSeq2 { yield! [ 5; 5; 5 ] }
            |> TaskCallbacks.distinctUntilChangedWithAsync (fun _ _ -> task { return false })
            |> ColdTask.toListAsync

        xs |> should equal [ 5; 5; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync can perform async work in comparer`` () = task {
        let mutable comparerCallCount = 0

        let asyncComparer a b = task {
            comparerCallCount <- comparerCallCount + 1
            return a = b
        }

        let! xs =
            asyncSeq2 { yield! [ 1; 1; 2; 2; 3 ] }
            |> TaskCallbacks.distinctUntilChangedWithAsync asyncComparer
            |> ColdTask.toListAsync

        xs |> should equal [ 1; 2; 3 ]
        // comparer called for each pair of consecutive elements (4 pairs for 5 elements)
        comparerCallCount |> should equal 4
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged consumes every element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..6 do
                count <- count + 1
                yield i % 3 // yields 1,2,0,1,2,0 — no consecutive duplicates
        }

        let! xs = ts |> AsyncSeq2.distinctUntilChanged |> ColdTask.toListAsync
        count |> should equal 6
        xs |> should equal [ 1; 2; 0; 1; 2; 0 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChanged skips duplicates without extra evaluation`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in [ 1; 1; 2; 2; 3 ] do
                count <- count + 1
                yield i
        }

        let! xs = ts |> AsyncSeq2.distinctUntilChanged |> ColdTask.toListAsync
        // All 5 source elements must still be consumed
        count |> should equal 5
        xs |> should equal [ 1; 2; 3 ]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-distinctUntilChanged on side-effect seq preserves all unique elements`` variant = task {
        // getSeqWithSideEffect yields 1..10 (all unique on first iteration)
        let! xs =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        xs |> should equal [ 1..10 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWith consumes every element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> AsyncSeq2.distinctUntilChangedWith (fun a b -> a = b)
            |> ColdTask.toListAsync

        count |> should equal 5
        xs |> should equal [ 1; 2; 3; 4; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctUntilChangedWithAsync consumes every element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> TaskCallbacks.distinctUntilChangedWithAsync (fun a b -> task { return a = b })
            |> ColdTask.toListAsync

        count |> should equal 5
        xs |> should equal [ 1; 2; 3; 4; 5 ]
    }
