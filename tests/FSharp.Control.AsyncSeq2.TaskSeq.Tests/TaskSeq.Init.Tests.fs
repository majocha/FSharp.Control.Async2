module AsyncSeq2.Tests.Init

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.init
// AsyncSeq2.initInfinite
// TaskCallbacks.initAsync
// TaskCallbacks.initInfiniteAsync
//

/// Asserts that a sequence contains the char values 'A'..'J'.

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-init can generate an empty sequence`` () = AsyncSeq2.init 0 (fun x -> x) |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-initAsync can generate an empty sequence`` () =
        TaskCallbacks.initAsync 0 (fun x -> Task.fromResult x)
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-init with a negative count gives an error`` () =
        fun () ->
            AsyncSeq2.init -1 (fun x -> Task.fromResult x)
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            AsyncSeq2.init Int32.MinValue (fun x -> Task.fromResult x)
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-initAsync with a negative count gives an error`` () =
        fun () ->
            TaskCallbacks.initAsync Int32.MinValue (fun x -> Task.fromResult x)
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<ArgumentException>

module Immutable =
    [<Fact>]
    let ``AsyncSeq2-init singleton`` () =
        AsyncSeq2.init 1 id
        |> ColdTask.head
        |> Task.map (should equal 0)

    [<Fact>]
    let ``AsyncSeq2-initAsync singleton`` () =
        TaskCallbacks.initAsync 1 (id >> Task.fromResult)
        |> ColdTask.head
        |> Task.map (should equal 0)

    [<Fact>]
    let ``AsyncSeq2-init some values`` () =
        AsyncSeq2.init 42 (fun x -> x / 2)
        |> AsyncSeq2.length |> Async2.StartAsTask
        |> Task.map (should equal 42)

    [<Fact>]
    let ``AsyncSeq2-initAsync some values`` () =
        AsyncSeq2.init 42 (fun x -> Task.fromResult (x / 2))
        |> AsyncSeq2.length |> Async2.StartAsTask
        |> Task.map (should equal 42)

    [<Fact>]
    let ``AsyncSeq2-initInfinite`` () =
        AsyncSeq2.initInfinite (fun x -> x / 2)
        |> ColdTask.item 1_000_001
        |> Task.map (should equal 500_000)

    [<Fact>]
    let ``AsyncSeq2-initInfiniteAsync`` () =
        TaskCallbacks.initInfiniteAsync (fun x -> Task.fromResult (x / 2))
        |> ColdTask.item 1_000_001
        |> Task.map (should equal 500_000)

module SideEffects =
    let inc (i: int byref) =
        i <- i + 1
        i

    [<Fact>]
    let ``AsyncSeq2-init singleton with side effects`` () = task {
        let mutable x = 0

        let ts = AsyncSeq2.init 1 (fun _ -> inc &x)

        do! ColdTask.head ts |> Task.map (should equal 1)
        do! ColdTask.head ts |> Task.map (should equal 2)
        do! ColdTask.head ts |> Task.map (should equal 3) // state mutates
    }

    [<Fact>]
    let ``AsyncSeq2-init singleton with side effects -- Current`` () = task {
        let mutable x = 0

        let ts = AsyncSeq2.init 1 (fun _ -> inc &x)

        let enumerator = ts.GetAsyncEnumerator()
        let! _ = enumerator.MoveNextAsync()
        do enumerator.Current |> should equal 1
        do enumerator.Current |> should equal 1
        do enumerator.Current |> should equal 1 // current state does not mutate
    }

    [<Fact>]
    let ``AsyncSeq2-initAsync singleton with side effects`` () = task {
        let mutable x = 0

        let ts = TaskCallbacks.initAsync 1 (fun _ -> Task.fromResult (inc &x))

        do! ColdTask.head ts |> Task.map (should equal 1)
        do! ColdTask.head ts |> Task.map (should equal 2)
        do! ColdTask.head ts |> Task.map (should equal 3) // state mutates
    }

    [<Fact>]
    let ``AsyncSeq2-initAsync singleton with side effects -- Current`` () = task {
        let mutable x = 0

        let ts = TaskCallbacks.initAsync 1 (fun _ -> Task.fromResult (inc &x))

        let enumerator = ts.GetAsyncEnumerator()
        let! _ = enumerator.MoveNextAsync()
        do enumerator.Current |> should equal 1
        do enumerator.Current |> should equal 1
        do enumerator.Current |> should equal 1 // current state does not mutate
    }
