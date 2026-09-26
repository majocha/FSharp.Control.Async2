module AsyncSeq2.Tests.FoldWhile

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.foldWhile
// ColdTask.foldWhileAsync
//
// Semantics match AsyncSeq2.takeWhile: the predicate is evaluated against (state, element)
// before that element is folded in. When the predicate returns false, iteration halts
// without folding that element, and no further elements are enumerated.
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.foldWhile (fun _ _ -> true) (fun _ item -> item + 1) 0 null

        assertNullArg
        <| fun () -> ColdTask.foldWhileAsync (fun _ _ -> Task.fromResult true) (fun _ item -> Task.fromResult (item + 1)) 0 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-foldWhile returns initial state when empty`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.foldWhile (fun _ _ -> true) (fun _ item -> item + 1) -1

        result |> should equal -1
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-foldWhileAsync returns initial state when empty`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> ColdTask.foldWhileAsync (fun _ _ -> Task.fromResult true) (fun _ item -> Task.fromResult (item + 1)) -1

        result |> should equal -1
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-foldWhile does not call predicate or folder when empty`` variant = task {
        let mutable predicateCalled = false
        let mutable folderCalled = false

        let! _ =
            Gen.getEmptyVariant variant
            |> ColdTask.foldWhile
                (fun _ _ ->
                    predicateCalled <- true
                    true)
                (fun state _ ->
                    folderCalled <- true
                    state)
                0

        predicateCalled |> should be False
        folderCalled |> should be False
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-foldWhileAsync does not call predicate or folder when empty`` variant = task {
        let mutable predicateCalled = false
        let mutable folderCalled = false

        let! _ =
            Gen.getEmptyVariant variant
            |> ColdTask.foldWhileAsync
                (fun _ _ -> task {
                    predicateCalled <- true
                    return true
                })
                (fun state _ -> task {
                    folderCalled <- true
                    return state
                })
                0

        predicateCalled |> should be False
        folderCalled |> should be False
    }

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-foldWhile with always-true predicate behaves like fold`` () = task {
        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhile (fun _ _ -> true) (fun acc item -> acc + item) 0

        result |> should equal 15
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhileAsync with always-true predicate behaves like foldAsync`` () = task {
        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhileAsync (fun _ _ -> Task.fromResult true) (fun acc item -> Task.fromResult (acc + item)) 0

        result |> should equal 15
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhile is left-associative like fold`` () = task {
        let! result =
            AsyncSeq2.ofList [ "b"; "c"; "d" ]
            |> ColdTask.foldWhile (fun _ _ -> true) (fun acc item -> acc + item) "a"

        result |> should equal "abcd"
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhileAsync is left-associative like foldAsync`` () = task {
        let! result =
            AsyncSeq2.ofList [ "b"; "c"; "d" ]
            |> ColdTask.foldWhileAsync (fun _ _ -> Task.fromResult true) (fun acc item -> Task.fromResult (acc + item)) "a"

        result |> should equal "abcd"
    }

module Halt =
    [<Fact>]
    let ``AsyncSeq2-foldWhile stops immediately when predicate is false on first element`` () = task {
        let mutable predicateCalls = 0
        let mutable folderCalls = 0

        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhile
                (fun _ _ ->
                    predicateCalls <- predicateCalls + 1
                    false)
                (fun _ item ->
                    folderCalls <- folderCalls + 1
                    item)
                0

        result |> should equal 0
        predicateCalls |> should equal 1
        folderCalls |> should equal 0
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhileAsync stops immediately when predicate is false on first element`` () = task {
        let mutable predicateCalls = 0
        let mutable folderCalls = 0

        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhileAsync
                (fun _ _ -> task {
                    predicateCalls <- predicateCalls + 1
                    return false
                })
                (fun _ item -> task {
                    folderCalls <- folderCalls + 1
                    return item
                })
                0

        result |> should equal 0
        predicateCalls |> should equal 1
        folderCalls |> should equal 0
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhile halts mid-sequence without folding the halting element`` () = task {
        // Sum while adding the next element would keep the total <= 5. Once adding
        // the element would overshoot, stop — that element is NOT folded in.
        let mutable predicateCalls = 0
        let mutable folderCalls = 0

        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhile
                (fun acc item ->
                    predicateCalls <- predicateCalls + 1
                    acc + item <= 5)
                (fun acc item ->
                    folderCalls <- folderCalls + 1
                    acc + item)
                0

        // 1 (ok, total 1), 2 (ok, total 3), 3 (would make 6 > 5, stop)
        result |> should equal 3
        predicateCalls |> should equal 3
        folderCalls |> should equal 2
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhileAsync halts mid-sequence without folding the halting element`` () = task {
        let mutable predicateCalls = 0
        let mutable folderCalls = 0

        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhileAsync
                (fun acc item -> task {
                    predicateCalls <- predicateCalls + 1
                    return acc + item <= 5
                })
                (fun acc item -> task {
                    folderCalls <- folderCalls + 1
                    return acc + item
                })
                0

        result |> should equal 3
        predicateCalls |> should equal 3
        folderCalls |> should equal 2
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhile does not enumerate past the halting element`` () = task {
        // Source has a side effect per pulled element; halt on the 3rd pull.
        let mutable pulled = 0

        let source = asyncSeq2 {
            for i in 1..5 do
                pulled <- pulled + 1
                yield i
        }

        let! _ =
            source
            |> ColdTask.foldWhile (fun _ item -> item < 3) (fun acc item -> acc + item) 0

        // Pull 1 (ok), pull 2 (ok), pull 3 (predicate false, stop) — must not pull 4.
        pulled |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhileAsync does not enumerate past the halting element`` () = task {
        let mutable pulled = 0

        let source = asyncSeq2 {
            for i in 1..5 do
                pulled <- pulled + 1
                yield i
        }

        let! _ =
            source
            |> ColdTask.foldWhileAsync (fun _ item -> Task.fromResult (item < 3)) (fun acc item -> Task.fromResult (acc + item)) 0

        pulled |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-foldWhile that never halts is equivalent to fold`` () = task {
        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.foldWhile (fun _ item -> item <= 10) (fun acc item -> acc + item) 0

        result |> should equal 15
    }
