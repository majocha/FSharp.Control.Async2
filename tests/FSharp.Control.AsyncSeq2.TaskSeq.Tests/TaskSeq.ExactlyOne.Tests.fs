module AsyncSeq2.Tests.ExactlyOne

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.exactlyOne
// ColdTask.tryExactlyOne
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.exactlyOne null
        assertNullArg <| fun () -> ColdTask.tryExactlyOne null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-exactlyOne throws`` variant = task {
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryExactlyOne returns None`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryExactlyOne
        nothing |> should be None'
    }

module Other =
    [<Fact>]
    let ``AsyncSeq2-exactlyOne throws for a sequence of length = two`` () = task {
        fun () ->
            asyncSeq2 {
                yield 1
                yield 2
            }
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-exactlyOne throws for a sequence of length = two - variant`` () = task {
        fun () ->
            Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 2
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne returns None for sequence of length = two`` () =
        asyncSeq2 {
            yield 1
            yield 2
        }
        |> ColdTask.tryExactlyOne
        |> Task.map (should be None')

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne returns None for sequence of length = two - variant`` () =
        Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 2
        |> ColdTask.tryExactlyOne
        |> Task.map (should be None')

    [<Fact>]
    let ``AsyncSeq2-exactlyOne throws with a larger sequence`` () = task {
        fun () ->
            Gen.sideEffectTaskSeqMicro 50L<µs> 300L<µs> 200
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne returns None with a larger sequence`` () = task {
        let! nothing =
            Gen.sideEffectTaskSeqMicro 50L<µs> 300L<µs> 20
            |> ColdTask.tryExactlyOne

        nothing |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-exactlyOne gets the only item in a singleton sequence`` () = task {
        let! exactlyOne = asyncSeq2 { yield 10 } |> ColdTask.exactlyOne
        exactlyOne |> should equal 10
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne gets the only item in a singleton sequence`` () = task {
        let! exactlyOne = asyncSeq2 { yield 10 } |> ColdTask.tryExactlyOne
        exactlyOne |> should be Some'
        exactlyOne |> should equal (Some 10)
    }

    [<Fact>]
    let ``AsyncSeq2-exactlyOne gets the only item in a singleton sequence - variant`` () = task {
        let! exactlyOne =
            Gen.sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 1
            |> ColdTask.exactlyOne

        exactlyOne |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne gets the only item in a singleton sequence - variant`` () = task {
        let! exactlyOne =
            Gen.sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 1
            |> ColdTask.tryExactlyOne

        exactlyOne |> should be Some'
        exactlyOne |> should equal (Some 1)
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exactlyOne throws`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryExactlyOne returns None`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! head1 = ColdTask.tryExactlyOne ts
        let! head2 = ColdTask.tryExactlyOne ts
        head1 |> should be None'
        head2 |> should be None'
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-exactlyOne prove we don't iterate further than necessary`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1 // to test we're "exactly one", we need to read until 2nd item
            yield 2
            i <- i + 1 // we never get here
        }

        fun () -> ts |> ColdTask.exactlyOne |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        i |> should equal 2 // last side effect is not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne prove we don't iterate further than necessary`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1 // to test we're "exactly one", we need to read until 2nd item
            yield 2
            i <- i + 1 // we never get here
        }

        do! ts |> ColdTask.tryExactlyOne |> Task.map (should be None')
        i |> should equal 2 // last side effect is not executed
    }

    [<Fact>]
    let ``AsyncSeq2-exactlyOne prove we execute side-effects in empty seq`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        fun () -> ts |> ColdTask.exactlyOne |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        i |> should equal 3 // last side effect is ALSO executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne prove we execute side-effects in empty seq`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.tryExactlyOne |> Task.map (should be None')
        i |> should equal 3 // last side effect is ALSO executed
    }

    [<Fact>]
    let ``AsyncSeq2-exactlyOne prove we execute side-effects in singleton seq`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.exactlyOne |> Task.map (should equal 42)
        i |> should equal 3 // last side effect is ALSO executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryExactlyOne prove we execute side-effects in singleton seq`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do!
            ts
            |> ColdTask.tryExactlyOne
            |> Task.map (should equal (Some 42))

        i |> should equal 3 // last side effect is ALSO executed
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-exactlyOne throws`` variant =
        fun () ->
            Gen.getSeqWithSideEffect variant
            |> ColdTask.exactlyOne
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-exactlyOne throws, but sequence remains accessible`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! nothing = task {
            try
                return! ColdTask.exactlyOne ts
            with ex ->
                ex |> should be ofExactType<ArgumentException>
                return -42
        }

        nothing |> should equal -42

        // Test that side-effect has executed. Different sequence variants
        // increase the counter differently but they're never 1
        let! head1 = ColdTask.head ts
        head1 |> should not' (equal 1)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryExactlyOne returns None`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! head1 = ColdTask.tryExactlyOne ts
        let! head2 = ColdTask.tryExactlyOne ts
        head1 |> should be None'
        head2 |> should be None'

        // Test that side-effect has executed. Different sequence variants
        // increase the counter differently but they're never 1
        let! head3 = ColdTask.head ts
        head3 |> should not' (equal 1)
    }
