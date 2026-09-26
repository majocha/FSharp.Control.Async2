module AsyncSeq2.Tests.Let

open System.Threading.Tasks

open FsUnit
open Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<Fact>]
let ``CE asyncSeq2: use 'let'`` () =
    let mutable value = 0

    asyncSeq2 {
        let value1 = value + 1
        let value2 = value1 + 1
        yield value2
    }
    |> ColdTask.exactlyOne
    |> Task.map (should equal 2)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a task<unit>`` () =
    let mutable value = 0

    asyncSeq2 {
        let! unit' = task { do value <- value + 1 }
        do unit'
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a task<string>`` () =
    asyncSeq2 {
        let! test = task { return "test" }
        yield test
    }
    |> ColdTask.exactlyOne
    |> Task.map (should equal "test")

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a ValueTask<unit>`` () =
    let mutable value = 0

    asyncSeq2 {
        let! unit' = ValueTask.ofTask (task { do value <- value + 1 })
        do unit'
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a ValueTask<string>`` () =
    asyncSeq2 {
        let! test = ValueTask.ofTask (task { return "test" })
        yield test
    }
    |> ColdTask.exactlyOne
    |> Task.map (should equal "test")

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a non-generic ValueTask`` () =
    let mutable value = 0

    asyncSeq2 {
        let! unit' = ValueTask(task { do value <- value + 1 })
        do unit'
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with a non-generic task`` () =
    let mutable value = 0

    asyncSeq2 {
        let! unit' = (task { do value <- value + 1 }) |> Task.ignore
        do unit'
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 1)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with Async`` () =
    let mutable value = 0

    asyncSeq2 {
        do value <- value + 1
        let! _ = Async.Sleep 50
        do value <- value + 1
    }
    |> verifyEmpty
    |> Task.map (fun _ -> value |> should equal 2)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with Async - mutables`` () =
    let mutable value = 0

    asyncSeq2 {
        do! async { value <- value + 1 }
        do value |> should equal 1
        let! x = async { return value + 1 }
        do x |> should equal 2
        do! Async.Sleep 50
        do! async { value <- value + 1 }
        do value |> should equal 2
        let! ret = async { return value + 1 }
        do value |> should equal 2
        do ret |> should equal 3
        yield x + ret // eq 5
    }
    |> ColdTask.exactlyOne
    |> Task.map (should equal 5)

[<Fact>]
let ``CE asyncSeq2: use 'let!' with all kinds of overloads at once`` () =
    let mutable value = 0

    // this test should be expanded in case any new overload is added
    // that is supported by `let!`, to ensure the high/low priority
    // overloads still work properly
    asyncSeq2 {
        let! a = task { // eq 1
            do! Task.Delay 10
            do value <- value + 1
            return value
        }

        let! b = // eq 2
            task {
                do! Task.Delay 50
                do value <- value + 1
                return value
            }
            |> ValueTask<int>

        let! c = ValueTask<_>(4) // ValueTask that completes immediately
        let! _ = Task.Factory.StartNew(fun () -> value <- value + 1) // non-generic Task with side effect
        let! d = Task.fromResult 99 // normal Task that completes immediately
        let! _ = Async.Sleep 0 // unit Async

        let! e = async {
            do! Async.Sleep 40
            do value <- value + 1 // eq 4 now
            return value
        }

        yield! [ a; b; c; d; e ]
    }
    |> ColdTask.toListAsync
    |> Task.map (should equal [ 1; 2; 4; 99; 4 ])
