module AsyncSeq2.Tests.Delay

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.delay
//

let validateSequence ts =
    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal "12345678910")

module EmptySeq =
    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-delay with empty sequences`` variant =
        fun () -> Gen.getEmptyVariant variant
        |> AsyncSeq2.delay
        |> verifyEmpty

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-delay`` variant =
        fun () -> Gen.getSeqImmutable variant
        |> AsyncSeq2.delay
        |> validateSequence

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-delay executes side effects`` () = task {
        let mutable i = 0

        let ts =
            fun () -> asyncSeq2 {
                yield! [ 1..10 ]
                i <- i + 1
            }
            |> AsyncSeq2.delay

        do! ts |> validateSequence
        i |> should equal 1
        let! len = AsyncSeq2.length ts |> Async2.StartAsTask
        i |> should equal 2 // re-eval of the sequence executes side effect again
        len |> should equal 10
    }
