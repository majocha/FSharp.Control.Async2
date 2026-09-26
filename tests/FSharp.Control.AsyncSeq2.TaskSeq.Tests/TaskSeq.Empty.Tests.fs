module AsyncSeq2.Tests.Empty

open System.Threading.Tasks
open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation


[<Fact>]
let ``AsyncSeq2-empty returns an empty sequence`` () = task {
    let! sq = (AsyncSeq2.empty<string> ()) |> ColdTask.toListAsync
    Seq.isEmpty sq |> should be True
    Seq.length sq |> should equal 0
}

[<Fact>]
let ``AsyncSeq2-empty returns an empty sequence - variant`` () = task {
    let! isEmpty = (AsyncSeq2.empty<string> ()) |> AsyncSeq2.isEmpty |> Async2.StartAsTask
    isEmpty |> should be True
}

[<Fact>]
let ``AsyncSeq2-empty in a asyncSeq2 context`` () = task {
    let! sq =
        asyncSeq2 { yield! (AsyncSeq2.empty<string> ()) }
        |> ColdTask.toArrayAsync

    Array.isEmpty sq |> should be True
}

[<Fact>]
let ``AsyncSeq2-empty of unit in a asyncSeq2 context`` () = task {
    let! sq =
        asyncSeq2 { yield! (AsyncSeq2.empty<unit> ()) }
        |> ColdTask.toArrayAsync

    Array.isEmpty sq |> should be True
}

[<Fact>]
let ``AsyncSeq2-empty of more complex type in a asyncSeq2 context`` () = task {
    let! sq =
        asyncSeq2 { yield! (AsyncSeq2.empty<Result<Task<string>, int>> ()) } // not a TaskResult, but a ResultTask lol
        |> ColdTask.toArrayAsync

    Array.isEmpty sq |> should be True
}

[<Fact>]
let ``AsyncSeq2-empty multiple times in a asyncSeq2 context`` () = task {
    let! sq =
        asyncSeq2 {
            yield! (AsyncSeq2.empty<string> ())
            yield! (AsyncSeq2.empty<string> ())
            yield! (AsyncSeq2.empty<string> ())
            yield! (AsyncSeq2.empty<string> ())
            yield! (AsyncSeq2.empty<string> ())
        }
        |> ColdTask.toArrayAsync

    Array.isEmpty sq |> should be True
}

[<Fact>]
let ``AsyncSeq2-empty multiple times with side effects`` () = task {
    let mutable x = 0

    let sq = asyncSeq2 {
        yield! (AsyncSeq2.empty<string> ())
        yield! (AsyncSeq2.empty<string> ())
        x <- x + 1
        yield! (AsyncSeq2.empty<string> ())
        x <- x + 1
        yield! (AsyncSeq2.empty<string> ())
        x <- x + 1
        yield! (AsyncSeq2.empty<string> ())
        x <- x + 1
        x <- x + 1
    }

    // executing side effects once
    (AsyncSeq2.toArraySync >> Array.isEmpty) sq |> should be True
    x |> should equal 5

    // twice
    (AsyncSeq2.toArraySync >> Array.isEmpty) sq |> should be True
    x |> should equal 10
}
