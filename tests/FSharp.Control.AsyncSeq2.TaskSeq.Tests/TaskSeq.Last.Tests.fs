module AsyncSeq2.Tests.Last

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.last
// ColdTask.tryLast
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.last null
        assertNullArg <| fun () -> ColdTask.tryLast null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-last throws`` variant = task {
        fun () -> Gen.getEmptyVariant variant |> ColdTask.last |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryLast returns None`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryLast
        nothing |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-last executes side effect`` () = task {
        let mutable x = 0

        fun () -> asyncSeq2 { do x <- x + 1 } |> ColdTask.last |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        // side effect must have run!
        x |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-tryLast executes side effect`` () = task {
        let mutable x = 0

        let! nothing = asyncSeq2 { do x <- x + 1 } |> ColdTask.tryLast
        nothing |> should be None'

        // side effect must have run!
        x |> should equal 1
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-last gets the last item`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! last = ColdTask.last ts
        last |> should equal 10

        let! last = ColdTask.last ts //immutable, so re-iteration does not change outcome
        last |> should equal 10
    }

    [<Fact>]
    let ``AsyncSeq2-last gets the only item in a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        let! last = ColdTask.last ts
        last |> should equal 42

        let! last = ColdTask.last ts // doing it twice is fine
        last |> should equal 42
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryLast gets the last item`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! last = ColdTask.tryLast ts
        last |> should equal (Some 10)

        let! last = ColdTask.tryLast ts //immutable, so re-iteration does not change outcome
        last |> should equal (Some 10)
    }

    [<Fact>]
    let ``AsyncSeq2-tryLast gets the only item in a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        let! last = ColdTask.tryLast ts
        last |> should equal (Some 42)

        let! last = ColdTask.tryLast ts // doing it twice is fine
        last |> should equal (Some 42)
    }


module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-last executes side effect after first item`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1
        }

        let! fortyTwo = one |> ColdTask.last
        let! fortyThree = one |> ColdTask.last // side effect, re-iterating!

        fortyTwo |> should equal 42
        fortyThree |> should equal 43
    }

    [<Fact>]
    let ``AsyncSeq2-tryLast executes side effect after first item`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1
        }

        let! fortyTwo = one |> ColdTask.tryLast
        fortyTwo |> should equal (Some 42)

        // side effect, reiterating causes it to execute again!
        let! fortyThree = one |> ColdTask.tryLast
        fortyThree |> should equal (Some 43)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-last gets the last item`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! ten = ColdTask.last ts
        ten |> should equal 10

        // side effect, reiterating causes it to execute again!
        let! twenty = ColdTask.last ts
        twenty |> should equal 20
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryLast gets the last item`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! ten = ColdTask.tryLast ts
        ten |> should equal (Some 10)

        // side effect, reiterating causes it to execute again!
        let! twenty = ColdTask.tryLast ts
        twenty |> should equal (Some 20)
    }
