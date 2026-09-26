module AsyncSeq2.Tests.Unfold

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.unfold
// AsyncSeq2.unfoldAsync
//

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-unfold generator returning None immediately yields empty sequence`` () = task {
        let! result = AsyncSeq2.unfold (fun _ -> None) 0 |> ColdTask.toArrayAsync

        result |> should be Empty
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync generator returning None immediately yields empty sequence`` () = task {
        let! result =
            AsyncSeq2.unfoldAsync (fun _ -> async2 { return None }) 0
            |> ColdTask.toArrayAsync

        result |> should be Empty
    }

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-unfold generates a finite sequence`` () = task {
        // unfold 0..9
        let! result =
            AsyncSeq2.unfold (fun n -> if n < 10 then Some(n, n + 1) else None) 0
            |> ColdTask.toArrayAsync

        result |> should equal [| 0..9 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync generates a finite sequence`` () = task {
        let! result =
            AsyncSeq2.unfoldAsync (fun n -> async2 { return if n < 10 then Some(n, n + 1) else None }) 0
            |> ColdTask.toArrayAsync

        result |> should equal [| 0..9 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfold generates a singleton sequence`` () = task {
        let! result =
            AsyncSeq2.unfold (fun s -> if s = 0 then Some(42, 1) else None) 0
            |> ColdTask.toArrayAsync

        result |> should equal [| 42 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync generates a singleton sequence`` () = task {
        let! result =
            AsyncSeq2.unfoldAsync (fun s -> async2 { return if s = 0 then Some(42, 1) else None }) 0
            |> ColdTask.toArrayAsync

        result |> should equal [| 42 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfold uses state correctly to thread accumulator`` () = task {
        // Fibonacci: state = (a, b), yield a, new state = (b, a+b)
        let! fibs =
            AsyncSeq2.unfold (fun (a, b) -> if a > 100 then None else Some(a, (b, a + b))) (1, 1)
            |> ColdTask.toArrayAsync

        fibs
        |> should equal [| 1; 1; 2; 3; 5; 8; 13; 21; 34; 55; 89 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync uses state correctly to thread accumulator`` () = task {
        let! fibs =
            AsyncSeq2.unfoldAsync (fun (a, b) -> async2 { return if a > 100 then None else Some(a, (b, a + b)) }) (1, 1)
            |> ColdTask.toArrayAsync

        fibs
        |> should equal [| 1; 1; 2; 3; 5; 8; 13; 21; 34; 55; 89 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfold can be truncated to limit infinite-like sequences`` () = task {
        // counters counting from 1 upward, take first 100
        let! result =
            AsyncSeq2.unfold (fun n -> Some(n, n + 1)) 1
            |> AsyncSeq2.take 100
            |> ColdTask.toArrayAsync

        result |> should equal [| 1..100 |]
        result |> Array.length |> should equal 100
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync can be truncated to limit infinite-like sequences`` () = task {
        let! result =
            AsyncSeq2.unfoldAsync (fun n -> async2 { return Some(n, n + 1) }) 1
            |> AsyncSeq2.take 100
            |> ColdTask.toArrayAsync

        result |> should equal [| 1..100 |]
        result |> Array.length |> should equal 100
    }

    [<Fact>]
    let ``AsyncSeq2-unfold generates string sequences from state`` () = task {
        // build "A", "B", ..., "Z"
        let! letters =
            AsyncSeq2.unfold (fun c -> if c > int 'Z' then None else Some(string (char c), c + 1)) (int 'A')
            |> ColdTask.toArrayAsync

        letters
        |> should equal [| for c in 'A' .. 'Z' -> string c |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfold calls generator exactly once per element plus one final None call`` () = task {
        let mutable callCount = 0

        let! result =
            AsyncSeq2.unfold
                (fun n ->
                    callCount <- callCount + 1

                    if n < 5 then Some(n, n + 1) else None)
                0
            |> ColdTask.toArrayAsync

        result |> should equal [| 0..4 |]
        callCount |> should equal 6 // 5 Some + 1 None
    }

    [<Fact>]
    let ``AsyncSeq2-unfold re-iterating restarts from initial state`` () = task {
        let ts = AsyncSeq2.unfold (fun n -> if n < 5 then Some(n, n + 1) else None) 0

        let! first = ts |> ColdTask.toArrayAsync
        let! second = ts |> ColdTask.toArrayAsync

        first |> should equal second
        first |> should equal [| 0..4 |]
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync re-iterating restarts from initial state`` () = task {
        let ts = AsyncSeq2.unfoldAsync (fun n -> async2 { return if n < 5 then Some(n, n + 1) else None }) 0

        let! first = ts |> ColdTask.toArrayAsync
        let! second = ts |> ColdTask.toArrayAsync

        first |> should equal second
        first |> should equal [| 0..4 |]
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-unfold generator side-effects accumulate across re-iterations`` () = task {
        // The generator closes over mutable external state. Each re-iteration starts fresh from
        // the initial seed (0), but the external counter keeps climbing — demonstrating that
        // the IAsyncEnumerable itself is stateless but the captured state is shared.
        let mutable totalCalls = 0

        let ts =
            AsyncSeq2.unfold
                (fun n ->
                    totalCalls <- totalCalls + 1
                    if n < 3 then Some(n, n + 1) else None)
                0

        let! first = ts |> ColdTask.toArrayAsync
        first |> should equal [| 0; 1; 2 |]
        totalCalls |> should equal 4 // 3 Some + 1 None

        let! second = ts |> ColdTask.toArrayAsync
        second |> should equal [| 0; 1; 2 |]
        totalCalls |> should equal 8 // called 4 more times for the second iteration
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync generator side-effects accumulate across re-iterations`` () = task {
        let mutable totalCalls = 0

        let ts =
            AsyncSeq2.unfoldAsync
                (fun n -> async2 {
                    totalCalls <- totalCalls + 1
                    return if n < 3 then Some(n, n + 1) else None
                })
                0

        let! first = ts |> ColdTask.toArrayAsync
        first |> should equal [| 0; 1; 2 |]
        totalCalls |> should equal 4

        let! second = ts |> ColdTask.toArrayAsync
        second |> should equal [| 0; 1; 2 |]
        totalCalls |> should equal 8
    }

    [<Fact>]
    let ``AsyncSeq2-unfold with take stops generator calls at the limit`` () = task {
        let mutable callCount = 0

        // Infinite generator: always returns Some
        let ts =
            AsyncSeq2.unfold
                (fun n ->
                    callCount <- callCount + 1
                    Some(n, n + 1))
                0

        let! result = ts |> AsyncSeq2.take 5 |> ColdTask.toArrayAsync
        result |> should equal [| 0; 1; 2; 3; 4 |]

        // take 5 pulls exactly 5 elements; with an always-Some generator no
        // extra sentinel call is needed, so callCount should be exactly 5.
        callCount |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync with take stops generator calls at the limit`` () = task {
        let mutable callCount = 0

        let ts =
            AsyncSeq2.unfoldAsync
                (fun n -> async2 {
                    callCount <- callCount + 1
                    return Some(n, n + 1)
                })
                0

        let! result = ts |> AsyncSeq2.take 5 |> ColdTask.toArrayAsync
        result |> should equal [| 0; 1; 2; 3; 4 |]
        callCount |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-unfold propagates exception thrown inside the generator`` () =
        let ts =
            AsyncSeq2.unfold
                (fun n ->
                    if n = 3 then
                        failwith "generator-boom"

                    Some(n, n + 1))
                0

        fun () -> ts |> consumeTaskSeq
        |> should throwAsyncExact typeof<System.Exception>

    [<Fact>]
    let ``AsyncSeq2-unfoldAsync propagates exception thrown inside the async generator`` () =
        let ts =
            AsyncSeq2.unfoldAsync
                (fun n -> async2 {
                    if n = 3 then
                        failwith "async-generator-boom"

                    return Some(n, n + 1)
                })
                0

        fun () -> ts |> consumeTaskSeq
        |> should throwAsyncExact typeof<System.Exception>
