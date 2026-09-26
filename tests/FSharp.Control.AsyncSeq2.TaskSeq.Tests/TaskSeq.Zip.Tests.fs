module AsyncSeq2.Tests.Zip

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.zip
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.zip null (AsyncSeq2.empty ())
        assertNullArg <| fun () -> AsyncSeq2.zip (AsyncSeq2.empty ()) null
        assertNullArg <| fun () -> AsyncSeq2.zip null null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip can zip empty sequences v1`` variant =
        AsyncSeq2.zip (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip can zip empty sequences v2`` variant =
        AsyncSeq2.zip (AsyncSeq2.empty<int> ()) (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip can zip empty sequences v3`` variant =
        AsyncSeq2.zip (Gen.getEmptyVariant variant) (AsyncSeq2.empty<int> ())
        |> verifyEmpty


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zip zips in correct order`` variant = task {
        let one = Gen.getSeqImmutable variant
        let two = Gen.getSeqImmutable variant
        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined
        |> Array.forall (fun (x, y) -> x = y)
        |> should be True

        combined |> should be (haveLength 10)

        combined
        |> should equal (Array.init 10 (fun x -> x + 1, x + 1))
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zip zips in correct order for differently delayed sequences`` variant = task {
        let one = Gen.getSeqImmutable variant
        let two = asyncSeq2 { yield! [ 1..10 ] }
        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined
        |> Array.forall (fun (x, y) -> x = y)
        |> should be True

        combined |> should be (haveLength 10)

        combined
        |> should equal (Array.init 10 (fun x -> x + 1, x + 1))
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zip zips can deal with side effects in sequences`` variant = task {
        let one = Gen.getSeqWithSideEffect variant
        let two = Gen.getSeqWithSideEffect variant
        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined
        |> Array.forall (fun (x, y) -> x = y)
        |> should be True

        combined |> should be (haveLength 10)

        combined
        |> should equal (Array.init 10 (fun x -> x + 1, x + 1))
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zip zips combine a side-effect-free, and a side-effect-full sequence`` variant = task {
        let one = Gen.getSeqWithSideEffect variant
        let two = asyncSeq2 { yield! [ 1..10 ] }
        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined
        |> Array.forall (fun (x, y) -> x = y)
        |> should be True

        combined |> should be (haveLength 10)

        combined
        |> should equal (Array.init 10 (fun x -> x + 1, x + 1))
    }

module Performance =
    [<Theory; InlineData 100; InlineData 1_000; InlineData 10_000; InlineData 100_000>]
    let ``AsyncSeq2-zip zips large sequences just fine`` length = task {
        let one = Gen.sideEffectTaskSeqMicro 10L<µs> 50L<µs> length
        let two = Gen.sideEffectTaskSeq_Sequential length
        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined
        |> Array.forall (fun (x, y) -> x = y)
        |> should be True

        combined |> should be (haveLength length)
        combined |> Array.last |> should equal (length, length)
    }

module UnequalLength =
    [<Fact>]
    let ``AsyncSeq2-zip stops at shorter first sequence`` () = task {
        // documented: "when one sequence is exhausted any remaining elements in the other sequence are ignored"
        let short = asyncSeq2 { yield! [ 1..5 ] }
        let long = asyncSeq2 { yield! [ 1..10 ] }
        let! combined = AsyncSeq2.zip short long |> ColdTask.toArrayAsync
        combined |> should be (haveLength 5)

        combined
        |> should equal (Array.init 5 (fun i -> i + 1, i + 1))
    }

    [<Fact>]
    let ``AsyncSeq2-zip stops at shorter second sequence`` () = task {
        // documented: "when one sequence is exhausted any remaining elements in the other sequence are ignored"
        let long = asyncSeq2 { yield! [ 1..10 ] }
        let short = asyncSeq2 { yield! [ 1..3 ] }
        let! combined = AsyncSeq2.zip long short |> ColdTask.toArrayAsync
        combined |> should be (haveLength 3)

        combined
        |> should equal (Array.init 3 (fun i -> i + 1, i + 1))
    }

    [<Fact>]
    let ``AsyncSeq2-zip with first sequence empty returns empty`` () =
        // documented: remaining elements in the longer sequence are ignored
        let empty = asyncSeq2 { yield! ([]: int list) }
        let nonEmpty = asyncSeq2 { yield! [ 1..10 ] }
        AsyncSeq2.zip empty nonEmpty |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-zip with second sequence empty returns empty`` () =
        // documented: remaining elements in the longer sequence are ignored
        let nonEmpty = asyncSeq2 { yield! [ 1..10 ] }
        let empty = asyncSeq2 { yield! ([]: int list) }
        AsyncSeq2.zip nonEmpty empty |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-zip with singleton first and longer second returns singleton`` () = task {
        let one = asyncSeq2 { yield 42 }
        let many = asyncSeq2 { yield! [ 1..10 ] }
        let! combined = AsyncSeq2.zip one many |> ColdTask.toArrayAsync
        combined |> should equal [| (42, 1) |]
    }

    [<Fact>]
    let ``AsyncSeq2-zip with longer first and singleton second returns singleton`` () = task {
        let many = asyncSeq2 { yield! [ 1..10 ] }
        let one = asyncSeq2 { yield 99 }
        let! combined = AsyncSeq2.zip many one |> ColdTask.toArrayAsync
        combined |> should equal [| (1, 99) |]
    }

module Other =
    [<Fact>]
    let ``AsyncSeq2-zip zips different types`` () = task {
        let one = asyncSeq2 {
            yield "one"
            yield "two"
        }

        let two = asyncSeq2 {
            yield 42L
            yield 43L
        }

        let combined = AsyncSeq2.zip one two
        let! combined = ColdTask.toArrayAsync combined

        combined |> should equal [| ("one", 42L); ("two", 43L) |]
    }

//
// AsyncSeq2.zip3
//

module EmptySeqZip3 =
    [<Fact>]
    let ``Null source is invalid for zip3`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.zip3 null (AsyncSeq2.empty ()) (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.zip3 (AsyncSeq2.empty ()) null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.zip3 (AsyncSeq2.empty ()) (AsyncSeq2.empty ()) null

        assertNullArg <| fun () -> AsyncSeq2.zip3 null null null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip3 can zip empty sequences`` variant =
        AsyncSeq2.zip3 (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip3 stops at first exhausted sequence`` variant =
        // second and third are non-empty, first is empty → result is empty
        AsyncSeq2.zip3 (Gen.getEmptyVariant variant) (asyncSeq2 { yield 1 }) (asyncSeq2 { yield 2 })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip3 stops when second sequence is empty`` variant =
        AsyncSeq2.zip3 (asyncSeq2 { yield 1 }) (Gen.getEmptyVariant variant) (asyncSeq2 { yield 2 })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zip3 stops when third sequence is empty`` variant =
        AsyncSeq2.zip3 (asyncSeq2 { yield 1 }) (asyncSeq2 { yield 2 }) (Gen.getEmptyVariant variant)
        |> verifyEmpty

module ImmutableZip3 =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zip3 zips in correct order`` variant = task {
        let one = Gen.getSeqImmutable variant
        let two = Gen.getSeqImmutable variant
        let three = Gen.getSeqImmutable variant
        let! combined = AsyncSeq2.zip3 one two three |> ColdTask.toArrayAsync

        combined |> should haveLength 10

        combined
        |> should equal (Array.init 10 (fun x -> x + 1, x + 1, x + 1))
    }

    [<Fact>]
    let ``AsyncSeq2-zip3 produces correct triples with mixed types`` () = task {
        let one = asyncSeq2 {
            yield "a"
            yield "b"
        }

        let two = asyncSeq2 {
            yield 1
            yield 2
        }

        let three = asyncSeq2 {
            yield true
            yield false
        }

        let! combined = AsyncSeq2.zip3 one two three |> ColdTask.toArrayAsync

        combined
        |> should equal [| ("a", 1, true); ("b", 2, false) |]
    }

    [<Fact>]
    let ``AsyncSeq2-zip3 truncates to shortest sequence`` () = task {
        let one = asyncSeq2 { yield! [ 1..10 ] }
        let two = asyncSeq2 { yield! [ 1..5 ] }
        let three = asyncSeq2 { yield! [ 1..3 ] }
        let! combined = AsyncSeq2.zip3 one two three |> ColdTask.toArrayAsync

        combined |> should haveLength 3

        combined
        |> should equal [| (1, 1, 1); (2, 2, 2); (3, 3, 3) |]
    }

    [<Fact>]
    let ``AsyncSeq2-zip3 works with a single-element sequences`` () = task {
        let! combined =
            AsyncSeq2.zip3 (AsyncSeq2.singleton 1) (AsyncSeq2.singleton "x") (AsyncSeq2.singleton true)
            |> ColdTask.toArrayAsync

        combined |> should equal [| (1, "x", true) |]
    }

module SideEffectsZip3 =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zip3 can deal with side effects in sequences`` variant = task {
        let one = Gen.getSeqWithSideEffect variant
        let two = Gen.getSeqWithSideEffect variant
        let three = Gen.getSeqWithSideEffect variant
        let! combined = AsyncSeq2.zip3 one two three |> ColdTask.toArrayAsync

        combined
        |> Array.forall (fun (x, y, z) -> x = y && y = z)
        |> should be True

        combined |> should haveLength 10
    }
