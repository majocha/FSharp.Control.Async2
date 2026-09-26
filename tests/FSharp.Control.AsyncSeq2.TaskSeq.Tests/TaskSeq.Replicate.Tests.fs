module AsyncSeq2.Tests.Replicate

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.replicate
//

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-replicate with count 0 gives empty sequence`` () = AsyncSeq2.replicate 0 42 |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-replicate with negative count gives an error`` () =
        fun () ->
            AsyncSeq2.replicate -1 42
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            AsyncSeq2.replicate Int32.MinValue "hello"
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

module Immutable =
    [<Fact>]
    let ``AsyncSeq2-replicate produces the correct count and value`` () = task {
        let! arr = AsyncSeq2.replicate 5 99 |> ColdTask.toArrayAsync
        arr |> should haveLength 5
        arr |> should equal [| 99; 99; 99; 99; 99 |]
    }

    [<Fact>]
    let ``AsyncSeq2-replicate with count 1 produces a singleton`` () = task {
        let! arr = AsyncSeq2.replicate 1 "x" |> ColdTask.toArrayAsync
        arr |> should haveLength 1
        arr[0] |> should equal "x"
    }

    [<Fact>]
    let ``AsyncSeq2-replicate with large count`` () = task {
        let count = 10_000
        let! arr = AsyncSeq2.replicate count 7 |> ColdTask.toArrayAsync
        arr |> should haveLength count
        arr |> Array.forall ((=) 7) |> should be True
    }

    [<Fact>]
    let ``AsyncSeq2-replicate works with null as value`` () = task {
        let! arr = AsyncSeq2.replicate 3 null |> ColdTask.toArrayAsync
        arr |> should haveLength 3
        arr |> Array.forall (fun x -> x = null) |> should be True
    }

    [<Fact>]
    let ``AsyncSeq2-replicate can be consumed multiple times`` () = task {
        let ts = AsyncSeq2.replicate 4 "a"
        let! arr1 = ts |> ColdTask.toArrayAsync
        let! arr2 = ts |> ColdTask.toArrayAsync
        arr1 |> should equal arr2
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-replicate with a mutable value captures the value, not a reference`` () = task {
        let mutable x = 1
        let ts = AsyncSeq2.replicate 3 x
        x <- 999
        let! arr = ts |> ColdTask.toArrayAsync
        // replicate captures the value at call time (value type)
        arr |> should equal [| 1; 1; 1 |]
    }
