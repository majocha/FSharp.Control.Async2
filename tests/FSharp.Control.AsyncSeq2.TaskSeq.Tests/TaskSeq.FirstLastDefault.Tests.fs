module AsyncSeq2.Tests.FirstLastDefault

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.firstOrDefault
// ColdTask.lastOrDefault
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.firstOrDefault 0 null
        assertNullArg <| fun () -> ColdTask.lastOrDefault 0 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-firstOrDefault returns default for empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> ColdTask.firstOrDefault 42
        result |> should equal 42
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-lastOrDefault returns default for empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> ColdTask.lastOrDefault 99
        result |> should equal 99
    }

    [<Fact>]
    let ``AsyncSeq2-firstOrDefault returns default with reference type`` () = task {
        let! result = (AsyncSeq2.empty<string> ()) |> ColdTask.firstOrDefault "hello"
        result |> should equal "hello"
    }

    [<Fact>]
    let ``AsyncSeq2-lastOrDefault returns default with reference type`` () = task {
        let! result = (AsyncSeq2.empty<string> ()) |> ColdTask.lastOrDefault "world"
        result |> should equal "world"
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-firstOrDefault returns first element`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! result = ColdTask.firstOrDefault 0 ts
        result |> should equal 1
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lastOrDefault returns last element`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! result = ColdTask.lastOrDefault 0 ts
        result |> should equal 10
    }

    [<Fact>]
    let ``AsyncSeq2-firstOrDefault does not use default when non-empty`` () = task {
        let! result =
            asyncSeq2 {
                yield 5
                yield 6
            }
            |> ColdTask.firstOrDefault -1

        result |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-lastOrDefault does not use default when non-empty`` () = task {
        let! result =
            asyncSeq2 {
                yield 5
                yield 6
            }
            |> ColdTask.lastOrDefault -1

        result |> should equal 6
    }

    [<Fact>]
    let ``AsyncSeq2-firstOrDefault with singleton`` () = task {
        let! result = AsyncSeq2.singleton 42 |> ColdTask.firstOrDefault 0
        result |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-lastOrDefault with singleton`` () = task {
        let! result = AsyncSeq2.singleton 42 |> ColdTask.lastOrDefault 0
        result |> should equal 42
    }


module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-firstOrDefault __special-case__ prove it does not read beyond first yield`` () = task {
        let mutable x = 42

        let ts = asyncSeq2 {
            yield x
            x <- x + 1 // we never get here
        }

        let! fortyTwo = ts |> ColdTask.firstOrDefault 0
        let! stillFortyTwo = ts |> ColdTask.firstOrDefault 0 // the statement after 'yield' will never be reached

        fortyTwo |> should equal 42
        stillFortyTwo |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-firstOrDefault __special-case__ prove early side effect is executed`` () = task {
        let mutable x = 42

        let ts = asyncSeq2 {
            x <- x + 1
            x <- x + 1
            yield 42
            x <- x + 200 // we won't get here!
        }

        let! result = ts |> ColdTask.firstOrDefault 0
        result |> should equal 42
        x |> should equal 44

        let! result = ts |> ColdTask.firstOrDefault 0
        result |> should equal 42
        x |> should equal 46
    }

    [<Fact>]
    let ``AsyncSeq2-lastOrDefault __special-case__ prove it reads the entire sequence`` () = task {
        let mutable x = 42

        let ts = asyncSeq2 {
            yield x
            x <- x + 1 // will be executed
            yield x
            x <- x + 1 // will be executed
        }

        let! result = ts |> ColdTask.lastOrDefault -1
        result |> should equal 43
        x |> should equal 44
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-firstOrDefault returns first item in a side-effect sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first = ts |> ColdTask.firstOrDefault 0
        first |> should equal 1

        // side effect: re-enumerating changes the first item
        let! secondFirst = ts |> ColdTask.firstOrDefault 0
        secondFirst |> should not' (equal 1)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lastOrDefault returns last item and exhausts the sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! last = ts |> ColdTask.lastOrDefault 0
        last |> should equal 10

        // side effect: re-enumerating continues from mutated state
        let! secondLast = ts |> ColdTask.lastOrDefault 0
        secondLast |> should equal 20
    }
