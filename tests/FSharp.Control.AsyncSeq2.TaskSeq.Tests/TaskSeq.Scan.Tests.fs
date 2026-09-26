module AsyncSeq2.Tests.Scan

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.scan
// AsyncSeq2.scanAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.scan (fun _ _ -> 42) 0 null

        assertNullArg
        <| fun () -> AsyncSeq2.scanAsync (fun _ _ -> async2 { return 42 }) 0 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-scan on empty returns singleton initial state`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> AsyncSeq2.scan (fun acc _ -> acc + 1) 0
            |> ColdTask.toListAsync

        result |> should equal [ 0 ]
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-scanAsync on empty returns singleton initial state`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> AsyncSeq2.scanAsync (fun acc _ -> async2 { return acc + 1 }) 0
            |> ColdTask.toListAsync

        result |> should equal [ 0 ]
    }

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-scan yields initial state then each intermediate state`` () = task {
        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> AsyncSeq2.scan (fun acc item -> acc + item) 0
            |> ColdTask.toListAsync

        // N=5 elements → N+1=6 output elements
        result |> should equal [ 0; 1; 3; 6; 10; 15 ]
    }

    [<Fact>]
    let ``AsyncSeq2-scanAsync yields initial state then each intermediate state`` () = task {
        let! result =
            AsyncSeq2.ofList [ 1; 2; 3; 4; 5 ]
            |> AsyncSeq2.scanAsync (fun acc item -> async2 { return acc + item }) 0
            |> ColdTask.toListAsync

        result |> should equal [ 0; 1; 3; 6; 10; 15 ]
    }

    [<Fact>]
    let ``AsyncSeq2-scan output length is input length plus one`` () = task {
        let input = AsyncSeq2.ofList [ 'a'; 'b'; 'c' ]

        let! result =
            input
            |> AsyncSeq2.scan (fun acc c -> acc + string c) ""
            |> ColdTask.toListAsync

        result |> should equal [ ""; "a"; "ab"; "abc" ]
    }

    [<Fact>]
    let ``AsyncSeq2-scanAsync output length is input length plus one`` () = task {
        let input = AsyncSeq2.ofList [ 'a'; 'b'; 'c' ]

        let! result =
            input
            |> AsyncSeq2.scanAsync (fun acc c -> async2 { return acc + string c }) ""
            |> ColdTask.toListAsync

        result |> should equal [ ""; "a"; "ab"; "abc" ]
    }

    [<Fact>]
    let ``AsyncSeq2-scan with single element returns two-element result`` () = task {
        let! result =
            AsyncSeq2.singleton 42
            |> AsyncSeq2.scan (fun acc item -> acc + item) 10
            |> ColdTask.toListAsync

        result |> should equal [ 10; 52 ]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-scan accumulates correctly across variants`` variant = task {
        // Input is 1..10; cumulative sums: 0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55
        let! result =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.scan (fun acc item -> acc + item) 0
            |> ColdTask.toListAsync

        result
        |> should equal [ 0; 1; 3; 6; 10; 15; 21; 28; 36; 45; 55 ]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-scanAsync accumulates correctly across variants`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.scanAsync (fun acc item -> async2 { return acc + item }) 0
            |> ColdTask.toListAsync

        result
        |> should equal [ 0; 1; 3; 6; 10; 15; 21; 28; 36; 45; 55 ]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-scan second iteration accumulates from fresh start`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            ts
            |> AsyncSeq2.scan (fun acc item -> acc + item) 0
            |> ColdTask.toListAsync

        first
        |> should equal [ 0; 1; 3; 6; 10; 15; 21; 28; 36; 45; 55 ]

        let! second =
            ts
            |> AsyncSeq2.scan (fun acc item -> acc + item) 0
            |> ColdTask.toListAsync

        // side-effect sequences yield next 10 items (11..20)
        second
        |> should equal [ 0; 11; 23; 36; 50; 65; 81; 98; 116; 135; 155 ]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-scanAsync second iteration accumulates from fresh start`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            ts
            |> AsyncSeq2.scanAsync (fun acc item -> async2 { return acc + item }) 0
            |> ColdTask.toListAsync

        first
        |> should equal [ 0; 1; 3; 6; 10; 15; 21; 28; 36; 45; 55 ]

        let! second =
            ts
            |> AsyncSeq2.scanAsync (fun acc item -> async2 { return acc + item }) 0
            |> ColdTask.toListAsync

        second
        |> should equal [ 0; 11; 23; 36; 50; 65; 81; 98; 116; 135; 155 ]
    }
