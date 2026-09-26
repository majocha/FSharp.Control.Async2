module AsyncSeq2.Tests.Head

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.head
// ColdTask.tryHead
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.head null
        assertNullArg <| fun () -> ColdTask.tryHead null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-head throws`` variant = task {
        fun () -> Gen.getEmptyVariant variant |> ColdTask.head |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryHead returns None`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryHead
        nothing |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-head throws, but side effect is executed`` () = task {
        let mutable x = 0

        fun () -> asyncSeq2 { do x <- x + 1 } |> ColdTask.head |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        // side effect must have run!
        x |> should equal 1
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-head gets the head item of longer sequence`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! head = ColdTask.head ts
        head |> should equal 1

        let! head = ColdTask.head ts //immutable, so re-iteration does not change outcome
        head |> should equal 1
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryHead gets the head item of longer sequence`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! head = ColdTask.tryHead ts
        head |> should equal (Some 1)

        let! head = ColdTask.tryHead ts //immutable, so re-iteration does not change outcome
        head |> should equal (Some 1)
    }

    [<Fact>]
    let ``AsyncSeq2-head gets the only item in a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        let! head = ColdTask.head ts
        head |> should equal 42

        let! head = ColdTask.head ts // doing it twice is fine
        head |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-tryHead gets the only item in a singleton sequence`` () = task {
        let ts = asyncSeq2 { yield 42 }

        let! head = ColdTask.tryHead ts
        head |> should equal (Some 42)

        let! head = ColdTask.tryHead ts // doing it twice is fine
        head |> should equal (Some 42)
    }


module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-head __special-case__ prove it does not read beyond first yield`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1 // we never get here
        }

        let! fortyTwo = one |> ColdTask.head
        let! stillFortyTwo = one |> ColdTask.head // the statement after 'yield' will never be reached

        fortyTwo |> should equal 42
        stillFortyTwo |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-tryHead __special-case__ prove it does not read beyond first yield`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            yield x
            x <- x + 1 // we never get here
        }

        let! fortyTwo = one |> ColdTask.tryHead
        fortyTwo |> should equal (Some 42)

        // the statement after 'yield' will never be reached, the mutable will not be updated
        let! stillFortyTwo = one |> ColdTask.tryHead
        stillFortyTwo |> should equal (Some 42)

    }

    [<Fact>]
    let ``AsyncSeq2-head __special-case__ prove early side effect is executed`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            x <- x + 1
            x <- x + 1
            yield 42
            x <- x + 200 // we won't get here!
        }

        let! fortyTwo = one |> ColdTask.head
        fortyTwo |> should equal 42
        x |> should equal 44
        let! fortyTwo = one |> ColdTask.head
        fortyTwo |> should equal 42
        x |> should equal 46
    }

    [<Fact>]
    let ``AsyncSeq2-tryHead __special-case__ prove early side effect is executed`` () = task {
        let mutable x = 42

        let one = asyncSeq2 {
            x <- x + 1
            x <- x + 1
            yield 42
            x <- x + 200 // we won't get here!
        }

        let! fortyTwo = one |> ColdTask.tryHead
        fortyTwo |> should equal (Some 42)
        x |> should equal 44
        let! fortyTwo = one |> ColdTask.tryHead
        fortyTwo |> should equal (Some 42)
        x |> should equal 46

    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-head gets the head item in a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! ten = ColdTask.head ts
        ten |> should equal 1

        // side effect, reiterating causes it to execute again!
        let! twenty = ColdTask.head ts
        twenty |> should not' (equal 1) // different test data changes first item counter differently
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryHead gets the head item in a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! ten = ColdTask.tryHead ts
        ten |> should equal (Some 1)

        // side effect, reiterating causes it to execute again!
        let! twenty = ColdTask.tryHead ts
        twenty |> should not' (equal (Some 1)) // different test data changes first item counter differently
    }
