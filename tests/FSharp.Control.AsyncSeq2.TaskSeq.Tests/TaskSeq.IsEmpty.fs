module AsyncSeq2.Tests.IsEmpty

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.isEmpty
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () = assertNullArg <| fun () -> ColdTask.head null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-isEmpty returns true for empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be True)

module Immutable =
    [<Fact>]
    let ``AsyncSeq2-isEmpty returns false for singleton`` () =
        asyncSeq2 { yield 42 }
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)

    [<Fact>]
    let ``AsyncSeq2-isEmpty returns false for delayed singleton sequence`` () =
        Gen.sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 3
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-isEmpty returns false for non-empty`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-isEmpty prove that it won't execute side effects after the first item`` () =
        let mutable i = 0

        asyncSeq2 {
            i <- i + 1
            yield 42
            i <- i + 1
        }
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)
        |> Task.map (fun () -> i |> should equal 1)

    [<Fact>]
    let ``AsyncSeq2-isEmpty prove that it does execute side effects if empty`` () =
        let mutable i = 0

        asyncSeq2 {
            i <- i + 1
            i <- i + 1
        }
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be True)
        |> Task.map (fun () -> i |> should equal 2)

    [<Fact>]
    let ``AsyncSeq2-isEmpty executes side effects each time`` () =
        let mutable i = 0

        asyncSeq2 {
            i <- i + 1
            i <- i + 1
        }
        |>> (AsyncSeq2.isEmpty >> Async2.RunSynchronouslyImmediate)
        |>> (AsyncSeq2.isEmpty >> Async2.RunSynchronouslyImmediate)
        |>> (AsyncSeq2.isEmpty >> Async2.RunSynchronouslyImmediate)
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask // 4th time: 8
        |> Task.map (should be True)
        |> Task.map (fun () -> i |> should equal 8)

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-isEmpty returns false for non-empty`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)
