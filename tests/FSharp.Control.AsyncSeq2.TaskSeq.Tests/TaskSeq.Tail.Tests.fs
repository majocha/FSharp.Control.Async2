module AsyncSeq2.Tests.Tail

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.tail
// ColdTask.tryTail
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.tail null
        assertNullArg <| fun () -> ColdTask.tryTail null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tail throws`` variant = task {
        fun () -> Gen.getEmptyVariant variant |> ColdTask.tail |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryTail returns None`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryTail
        nothing |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-tail executes side effect`` () = task {
        let mutable x = 0

        fun () -> asyncSeq2 { do x <- x + 1 } |> ColdTask.tail |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        // side effect must have run!
        x |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-tryTail executes side effect`` () = task {
        let mutable x = 0

        let! nothing = asyncSeq2 { do x <- x + 1 } |> ColdTask.tryTail
        nothing |> should be None'

        // side effect must have run!
        x |> should equal 1
    }


module Immutable =
    let verifyTail tail =
        tail
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 2..10 |])

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tail gets the tail items`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! tail = ColdTask.tail ts
        do! verifyTail tail

        let! tail = ColdTask.tail ts //immutable, so re-iteration does not change outcome
        do! verifyTail tail
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryTail gets the tail item`` variant = task {
        let ts = Gen.getSeqImmutable variant

        match! ColdTask.tryTail ts with
        | Some tail -> do! verifyTail tail
        | x -> do x |> should not' (be None')

    }

    [<Fact>]
    let ``AsyncSeq2-tail return empty from a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        let! tail = ColdTask.tail ts
        do! verifyEmpty tail
    }

    [<Fact>]
    let ``AsyncSeq2-tryTail gets the only item in a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        match! ColdTask.tryTail ts with
        | Some tail -> do! verifyEmpty tail
        | x -> do x |> should not' (be None')
    }


module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-tail does not execute side effect after the first item in singleton`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1 // <--- we should never get here
        }

        let! _ = one |> ColdTask.tail
        let! _ = one |> ColdTask.tail // side effect, re-iterating!

        x |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-tryTail does not execute execute side effect after first item in singleton`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1 // <--- we should never get here
        }

        let! _ = one |> ColdTask.tryTail
        let! _ = one |> ColdTask.tryTail

        // side effect, reiterating causes it to execute again!
        x |> should equal 42

    }

    [<Fact>]
    let ``AsyncSeq2-tail executes side effect partially`` () = task {
        let mutable x = 42

        let ts = asyncSeq2 {
            x <- x + 1 // <--- executed on tail, but not materializing rest
            yield 1
            x <- x + 1 // <--- not executed on tail, but on materializing rest
            yield 2
            x <- x + 1 // <--- id
        }

        let! tail1 = ts |> ColdTask.tail
        x |> should equal 43 // test side effect runs 1x

        let! tail2 = ts |> ColdTask.tail
        x |> should equal 44 // test side effect ran again only 1x

        let! len = AsyncSeq2.length tail1 |> Async2.StartAsTask
        x |> should equal 46 // now 2nd & 3rd side effect runs, but not the first
        len |> should equal 1

        let! len = AsyncSeq2.length tail2 |> Async2.StartAsTask
        x |> should equal 48 // now again 2nd & 3rd side effect runs, but not the first
        len |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-tryTail executes side effect partially`` () = task {
        let mutable x = 42

        let ts = asyncSeq2 {
            x <- x + 1 // <--- executed on tail, but not materializing rest
            yield 1
            x <- x + 1 // <--- not executed on tail, but on materializing rest
            yield 2
            x <- x + 1 // <--- id
        }

        let! tail1 = ts |> ColdTask.tryTail
        x |> should equal 43 // test side effect runs 1x

        let! tail2 = ts |> ColdTask.tryTail
        x |> should equal 44 // test side effect ran again only 1x

        let! len = AsyncSeq2.length tail1.Value |> Async2.StartAsTask
        x |> should equal 46 // now 2nd side effect runs, but not the first
        len |> should equal 1

        let! len = AsyncSeq2.length tail2.Value |> Async2.StartAsTask
        x |> should equal 48 // now again 2nd side effect runs, but not the first
        len |> should equal 1
    }
