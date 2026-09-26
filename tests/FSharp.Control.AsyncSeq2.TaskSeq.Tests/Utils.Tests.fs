module AsyncSeq2.Tests.Utils

open System.Threading.Tasks
open System.Threading
open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<Fact>]
let ``runtimeTask starts an Async2 computation`` () = task {
    let mutable calls = 0
    let computation = async2 {
        let! token = Async2.CancellationToken
        token |> should equal CancellationToken.None
        calls <- calls + 1
        return 42
    }
    calls |> should equal 0
    let! result = runtimeTask { return! computation }
    result |> should equal 42
    calls |> should equal 1
}


module TaskBind =
    [<Fact>]
    let ``Task.bind awaits the task and passes the value to the binder`` () = task {
        let result =
            task { return 21 }
            |> Task.bind (fun n -> task { return n * 2 })

        let! v = result
        v |> should equal 42
    }

    [<Fact>]
    let ``Task.bind chains correctly`` () = task {
        let result =
            task { return 1 }
            |> Task.bind (fun n -> task { return n + 10 })
            |> Task.bind (fun n -> task { return n + 100 })

        let! v = result
        v |> should equal 111
    }


module TaskMap =
    [<Fact>]
    let ``Task.map transforms the result`` () = task {
        let result = task { return 21 } |> Task.map (fun n -> n * 2)

        let! v = result
        v |> should equal 42
    }
