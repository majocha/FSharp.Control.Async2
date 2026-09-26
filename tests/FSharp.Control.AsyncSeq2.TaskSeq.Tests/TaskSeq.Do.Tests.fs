module AsyncSeq2.Tests.Do

open System.Threading.Tasks

open FsUnit
open Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<Fact>]
let ``CE asyncSeq2: use 'do'`` () =
    let mutable value = 0

    asyncSeq2 { do value <- value + 1 } |> verifyEmpty

[<Fact>]
let ``CE asyncSeq2: use 'do!' with a task<unit>`` () =
    let mutable value = 0

    asyncSeq2 { do! task { do value <- value + 1 } }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with a ValueTask<unit>`` () =
    let mutable value = 0

    asyncSeq2 { do! ValueTask.ofTask (task { do value <- value + 1 }) }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with a non-generic ValueTask`` () =
    let mutable value = 0

    asyncSeq2 { do! ValueTask(task { do value <- value + 1 }) }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with a non-generic task`` () =
    let mutable value = 0

    asyncSeq2 { do! task { do value <- value + 1 } |> Task.ignore }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with a task-delay`` () =
    let mutable value = 0

    asyncSeq2 {
        do value <- value + 1
        do! Task.Delay 50
        do value <- value + 1
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 2)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with Async2`` () =
    let mutable value = 0

    asyncSeq2 {
        do value <- value + 1
        do! Async2.Sleep 50
        do value <- value + 1
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 2)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with Async2 - mutables`` () =
    let mutable value = 0

    asyncSeq2 {
        do! async2 { value <- value + 1 }
        do! Async2.Sleep 50
        do! async2 { value <- value + 1 }
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 2)

[<Fact>]
let ``CE asyncSeq2: use 'do!' with all kinds of overloads at once`` () =
    let mutable value = 0

    // this test should be expanded in case any new overload is added
    // that is supported by `do!`, to ensure the high/low priority
    // overloads still work properly
    asyncSeq2 {
        do! task { do value <- value + 1 } |> Task.ignore
        do! ValueTask <| task { do value <- value + 1 }
        do! ValueTask.ofTask (task { do value <- value + 1 })
        do! ValueTask<_>(()) // unit ValueTask that completes immediately
        do! Task.fromResult (()) // unit Task that completes immediately
        do! Task.Delay 0
        do! Async2.Sleep 0
        do! async2 { value <- value + 1 } // eq 4
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 4)
