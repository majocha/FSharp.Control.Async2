module AsyncSeq2.Tests.ReplicateInfinite

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.replicateInfinite
// AsyncSeq2.replicateInfiniteAsync
// AsyncSeq2.replicateUntilNoneAsync
//

module ReplicateInfinite =
    [<Fact>]
    let ``AsyncSeq2-replicateInfinite yields value indefinitely`` () = task {
        let! arr =
            AsyncSeq2.replicateInfinite 7
            |> AsyncSeq2.take 5
            |> ColdTask.toArrayAsync

        arr |> should equal [| 7; 7; 7; 7; 7 |]
    }

    [<Fact>]
    let ``AsyncSeq2-replicateInfinite with take 0 gives empty`` () = AsyncSeq2.replicateInfinite 1 |> AsyncSeq2.take 0 |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-replicateInfinite can be consumed multiple times`` () = task {
        let ts = AsyncSeq2.replicateInfinite "x"
        let! arr1 = ts |> AsyncSeq2.take 3 |> ColdTask.toArrayAsync
        let! arr2 = ts |> AsyncSeq2.take 3 |> ColdTask.toArrayAsync
        arr1 |> should equal [| "x"; "x"; "x" |]
        arr2 |> should equal arr1
    }

    [<Fact>]
    let ``AsyncSeq2-replicateInfinite with large take`` () = task {
        let count = 10_000

        let! arr =
            AsyncSeq2.replicateInfinite 42
            |> AsyncSeq2.take count
            |> ColdTask.toArrayAsync

        arr |> should haveLength count
        arr |> Array.forall ((=) 42) |> should be True
    }

    [<Fact>]
    let ``AsyncSeq2-replicateInfinite value captured at call site`` () = task {
        let mutable x = 1
        let ts = AsyncSeq2.replicateInfinite x
        x <- 999
        let! arr = ts |> AsyncSeq2.take 3 |> ColdTask.toArrayAsync
        // value type is captured at call time
        arr |> should equal [| 1; 1; 1 |]
    }


module ReplicateInfiniteAsync =
    [<Fact>]
    let ``AsyncSeq2-replicateInfiniteAsync yields computed value indefinitely`` () = task {
        let mutable n = 0

        let comp () = async2 {
            n <- n + 1
            return n
        }

        let! arr =
            AsyncSeq2.replicateInfiniteAsync comp
            |> AsyncSeq2.take 4
            |> ColdTask.toArrayAsync

        arr |> should equal [| 1; 2; 3; 4 |]
    }

    [<Fact>]
    let ``AsyncSeq2-replicateInfiniteAsync with take 0 gives empty`` () =
        let comp () = async2 { return 99 }

        AsyncSeq2.replicateInfiniteAsync comp
        |> AsyncSeq2.take 0
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-replicateInfiniteAsync constant computation`` () = task {
        let comp () = async2 { return "hello" }

        let! arr =
            AsyncSeq2.replicateInfiniteAsync comp
            |> AsyncSeq2.take 3
            |> ColdTask.toArrayAsync

        arr |> should equal [| "hello"; "hello"; "hello" |]
    }


module ReplicateUntilNoneAsync =
    [<Fact>]
    let ``AsyncSeq2-replicateUntilNoneAsync stops on None`` () = task {
        let mutable n = 0

        let comp () = async2 {
            n <- n + 1

            if n <= 3 then return Some n else return None
        }

        let! arr = AsyncSeq2.replicateUntilNoneAsync comp |> ColdTask.toArrayAsync
        arr |> should equal [| 1; 2; 3 |]
    }

    [<Fact>]
    let ``AsyncSeq2-replicateUntilNoneAsync returns empty when first call is None`` () = task {
        let comp () = async2 { return None }
        let ts = AsyncSeq2.replicateUntilNoneAsync comp
        let! arr = ts |> ColdTask.toArrayAsync
        arr |> should haveLength 0
    }

    [<Fact>]
    let ``AsyncSeq2-replicateUntilNoneAsync yields single element`` () = task {
        let mutable called = false

        let comp () = async2 {
            if not called then
                called <- true
                return Some 42
            else
                return None
        }

        let! arr = AsyncSeq2.replicateUntilNoneAsync comp |> ColdTask.toArrayAsync
        arr |> should equal [| 42 |]
    }

    [<Fact>]
    let ``AsyncSeq2-replicateUntilNoneAsync with counter`` () = task {
        let count = 100
        let mutable i = 0

        let comp () = async2 {
            if i < count then
                i <- i + 1
                return Some i
            else
                return None
        }

        let! arr = AsyncSeq2.replicateUntilNoneAsync comp |> ColdTask.toArrayAsync
        arr |> should haveLength count
        arr[0] |> should equal 1
        arr[count - 1] |> should equal count
    }
