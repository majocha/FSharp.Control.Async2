module AsyncSeq2.Tests.Distinct

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.distinct
// AsyncSeq2.distinctBy
// AsyncSeq2.distinctByAsync
//


module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-distinct with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.distinct null

    [<Fact>]
    let ``AsyncSeq2-distinctBy with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.distinctBy id null

    [<Fact>]
    let ``AsyncSeq2-distinctByAsync with null source raises`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.distinctByAsync (fun x -> async2 { return x }) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinct on empty returns empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.distinct
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinctBy on empty returns empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.distinctBy id
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-distinctByAsync on empty returns empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.distinctByAsync (fun x -> async2 { return x })
        |> verifyEmpty


module Functionality =
    [<Fact>]
    let ``AsyncSeq2-distinct removes duplicate ints`` () = task {
        let! result =
            asyncSeq2 { yield! [ 1; 2; 2; 3; 1; 4; 3; 5 ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 1; 2; 3; 4; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct removes duplicate strings`` () = task {
        let! result =
            asyncSeq2 { yield! [ "a"; "b"; "b"; "a"; "c" ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ "a"; "b"; "c" ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct with all identical elements returns singleton`` () = task {
        let! result =
            asyncSeq2 { yield! [ 7; 7; 7; 7; 7 ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 7 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct with all distinct elements returns all`` () = task {
        let! result =
            asyncSeq2 { yield! [ 1..5 ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 1; 2; 3; 4; 5 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct on singleton returns singleton`` () = task {
        let! result =
            asyncSeq2 { yield 42 }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 42 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct keeps first occurrence, not last`` () = task {
        // sequence [3;1;2;1;3] - first occurrences are at indices 0,1,2 for values 3,1,2
        let! result =
            asyncSeq2 { yield! [ 3; 1; 2; 1; 3 ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 3; 1; 2 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinct is different from distinctUntilChanged`` () = task {
        // [1;2;1] - distinct gives [1;2], distinctUntilChanged gives [1;2;1]
        let! distinct =
            asyncSeq2 { yield! [ 1; 2; 1 ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        let! distinctUntilChanged =
            asyncSeq2 { yield! [ 1; 2; 1 ] }
            |> AsyncSeq2.distinctUntilChanged
            |> ColdTask.toListAsync

        distinct |> should equal [ 1; 2 ]
        distinctUntilChanged |> should equal [ 1; 2; 1 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctBy removes elements with duplicate projected keys`` () = task {
        let! result =
            asyncSeq2 { yield! [ 1; 2; 3; 4; 5; 6 ] }
            |> AsyncSeq2.distinctBy (fun x -> x % 3)
            |> ColdTask.toListAsync

        // keys: 1%3=1, 2%3=2, 3%3=0, 4%3=1(dup), 5%3=2(dup), 6%3=0(dup)
        result |> should equal [ 1; 2; 3 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctBy with string length as key`` () = task {
        let! result =
            asyncSeq2 { yield! [ "a"; "bb"; "c"; "dd"; "eee" ] }
            |> AsyncSeq2.distinctBy String.length
            |> ColdTask.toListAsync

        // lengths: 1, 2, 1(dup), 2(dup), 3
        result |> should equal [ "a"; "bb"; "eee" ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctBy with identity projection equals distinct`` () = task {
        let input = [ 1; 2; 2; 3; 1; 4 ]

        let! byId =
            asyncSeq2 { yield! input }
            |> AsyncSeq2.distinctBy id
            |> ColdTask.toListAsync

        let! plain =
            asyncSeq2 { yield! input }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        byId |> should equal plain
    }

    [<Fact>]
    let ``AsyncSeq2-distinctBy keeps first element with a given key`` () = task {
        let! result =
            asyncSeq2 { yield! [ (1, "a"); (2, "b"); (1, "c") ] }
            |> AsyncSeq2.distinctBy fst
            |> ColdTask.toListAsync

        result |> should equal [ (1, "a"); (2, "b") ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctByAsync removes elements with duplicate projected keys`` () = task {
        let! result =
            asyncSeq2 { yield! [ 1; 2; 3; 4; 5; 6 ] }
            |> AsyncSeq2.distinctByAsync (fun x -> async2 { return x % 3 })
            |> ColdTask.toListAsync

        result |> should equal [ 1; 2; 3 ]
    }

    [<Fact>]
    let ``AsyncSeq2-distinctByAsync behaves identically to distinctBy`` () = task {
        let input = [ 1; 2; 2; 3; 1; 4 ]
        let projection x = x % 2

        let! bySync =
            asyncSeq2 { yield! input }
            |> AsyncSeq2.distinctBy projection
            |> ColdTask.toListAsync

        let! byAsync =
            asyncSeq2 { yield! input }
            |> AsyncSeq2.distinctByAsync (fun x -> async2 { return projection x })
            |> ColdTask.toListAsync

        bySync |> should equal byAsync
    }

    [<Fact>]
    let ``AsyncSeq2-distinct with chars`` () = task {
        let! result =
            asyncSeq2 { yield! [ 'A'; 'A'; 'B'; 'Z'; 'C'; 'C'; 'Z'; 'C'; 'D'; 'D'; 'D'; 'Z' ] }
            |> AsyncSeq2.distinct
            |> ColdTask.toListAsync

        result |> should equal [ 'A'; 'B'; 'Z'; 'C'; 'D' ]
    }


module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-distinct evaluates elements lazily`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                sideEffects <- sideEffects + 1
                yield i
        }

        let distinct = ts |> AsyncSeq2.distinct

        // no evaluation yet
        sideEffects |> should equal 0

        let! _ = distinct |> ColdTask.toListAsync

        // only evaluated when consumed
        sideEffects |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-distinctBy evaluates projection lazily`` () = task {
        let mutable projections = 0

        let! result =
            asyncSeq2 { yield! [ 1; 2; 3; 1; 2 ] }
            |> AsyncSeq2.distinctBy (fun x ->
                projections <- projections + 1
                x)
            |> ColdTask.toListAsync

        result |> should equal [ 1; 2; 3 ]
        // projection called once per element (5 elements)
        projections |> should equal 5
    }
