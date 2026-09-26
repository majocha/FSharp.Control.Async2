module AsyncSeq2.Tests.MapFold

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.mapFold
// ColdTask.mapFoldAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.mapFold (fun _ item -> string item, 0) 0 null

        assertNullArg
        <| fun () -> ColdTask.mapFoldAsync (fun _ item -> Task.fromResult (string item, 0)) 0 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-mapFold on empty returns empty array with initial state`` variant = task {
        let! results, finalState =
            Gen.getEmptyVariant variant
            |> ColdTask.mapFold (fun state item -> item * 2, state + item) 0

        results |> should equal [||]
        finalState |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-mapFoldAsync on empty returns empty array with initial state`` variant = task {
        let! results, finalState =
            Gen.getEmptyVariant variant
            |> ColdTask.mapFoldAsync (fun state item -> task { return item * 2, state + item }) 0

        results |> should equal [||]
        finalState |> should equal 0
    }

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-mapFold maps elements while threading state`` () = task {
        // mapFold: map each element to its double, sum all originals as state
        let! results, finalState =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.mapFold (fun state item -> item * 2, state + item) 0

        results |> should equal [| 2; 4; 6; 8; 10 |]
        finalState |> should equal 15 // 1+2+3+4+5
    }

    [<Fact>]
    let ``AsyncSeq2-mapFoldAsync maps elements while threading state`` () = task {
        let! results, finalState =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> ColdTask.mapFoldAsync (fun state item -> task { return item * 2, state + item }) 0

        results |> should equal [| 2; 4; 6; 8; 10 |]
        finalState |> should equal 15
    }

    [<Fact>]
    let ``AsyncSeq2-mapFold returns array of same length as source`` () = task {
        let! results, _ =
            AsyncSeq2.ofList [ 'a'; 'b'; 'c' ]
            |> ColdTask.mapFold (fun idx c -> string c, idx + 1) 0

        results |> should equal [| "a"; "b"; "c" |]
    }

    [<Fact>]
    let ``AsyncSeq2-mapFold single element returns singleton array and updated state`` () = task {
        let! results, finalState =
            AsyncSeq2.singleton 42
            |> ColdTask.mapFold (fun state item -> item + 1, state + item) 10

        results |> should equal [| 43 |]
        finalState |> should equal 52
    }

    [<Fact>]
    let ``AsyncSeq2-mapFold state threads through in order`` () = task {
        // Build running index as state; mapped element is (index, item) pair
        let! results, finalState =
            AsyncSeq2.ofList [ 10; 20; 30 ]
            |> ColdTask.mapFold (fun idx item -> (idx, item), idx + 1) 0

        results |> should equal [| (0, 10); (1, 20); (2, 30) |]
        finalState |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-mapFoldAsync state threads through in order`` () = task {
        let! results, finalState =
            AsyncSeq2.ofList [ 10; 20; 30 ]
            |> ColdTask.mapFoldAsync (fun idx item -> task { return (idx, item), idx + 1 }) 0

        results |> should equal [| (0, 10); (1, 20); (2, 30) |]
        finalState |> should equal 3
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-mapFold accumulates correctly across variants`` variant = task {
        // Input 1..10; mapped = item*item; state = running sum
        let! results, finalState =
            Gen.getSeqImmutable variant
            |> ColdTask.mapFold (fun acc item -> item * item, acc + item) 0

        results
        |> should equal [| 1; 4; 9; 16; 25; 36; 49; 64; 81; 100 |]

        finalState |> should equal 55 // 1+2+...+10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-mapFoldAsync accumulates correctly across variants`` variant = task {
        let! results, finalState =
            Gen.getSeqImmutable variant
            |> ColdTask.mapFoldAsync (fun acc item -> task { return item * item, acc + item }) 0

        results
        |> should equal [| 1; 4; 9; 16; 25; 36; 49; 64; 81; 100 |]

        finalState |> should equal 55
    }

    [<Fact>]
    let ``AsyncSeq2-mapFold result matches equivalent List.mapFold`` () = task {
        let items = [ 1; 2; 3; 4; 5 ]

        let listResults, listState = List.mapFold (fun state item -> item + state, state + item) 0 items

        let! taskResults, taskState =
            AsyncSeq2.ofList items
            |> ColdTask.mapFold (fun state item -> item + state, state + item) 0

        taskResults |> should equal (Array.ofList listResults)
        taskState |> should equal listState
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapFold second iteration sees next batch of side-effect values`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first, firstState =
            ts
            |> ColdTask.mapFold (fun acc item -> item * 2, acc + item) 0

        first
        |> should equal [| 2; 4; 6; 8; 10; 12; 14; 16; 18; 20 |]

        firstState |> should equal 55

        // side-effect sequences yield next 10 items (11..20) on second consumption
        let! second, secondState =
            ts
            |> ColdTask.mapFold (fun acc item -> item * 2, acc + item) 0

        second
        |> should equal [| 22; 24; 26; 28; 30; 32; 34; 36; 38; 40 |]

        secondState |> should equal 155 // 11+12+...+20
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapFoldAsync second iteration sees next batch of side-effect values`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first, firstState =
            ts
            |> ColdTask.mapFoldAsync (fun acc item -> task { return item * 2, acc + item }) 0

        first
        |> should equal [| 2; 4; 6; 8; 10; 12; 14; 16; 18; 20 |]

        firstState |> should equal 55

        let! second, secondState =
            ts
            |> ColdTask.mapFoldAsync (fun acc item -> task { return item * 2, acc + item }) 0

        second
        |> should equal [| 22; 24; 26; 28; 30; 32; 34; 36; 38; 40 |]

        secondState |> should equal 155
    }
