module AsyncSeq2.Tests.RemoveAt

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation


//
// AsyncSeq2.removeAt
// AsyncSeq2.removeManyAt
//

exception SideEffectPastEnd of string

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.removeAt 0 null

        assertNullArg <| fun () -> AsyncSeq2.removeManyAt 0 1 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-removeAt(0) on empty input raises`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.removeAt 0
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-removeManyAt(0) on empty input raises`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.removeManyAt 0 0
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-removeAt(-1) on empty input should throw ArgumentException without consuming`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.removeAt -1
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> Gen.getEmptyVariant variant |> AsyncSeq2.removeAt -1 |> ignore // task is not awaited

        |> should throw typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-removeManyAt(-1) on empty input should throw ArgumentException without consuming`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.removeManyAt -1 0
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.removeManyAt -1 0
            |> ignore

        |> should throw typeof<ArgumentException>

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeAt can remove last item`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 9
            |> verifyDigitsAsString "ABCDEFGHI"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeAt removes the item at indexed positions`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 0
            |> verifyDigitsAsString "BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 1
            |> verifyDigitsAsString "ACDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 5
            |> verifyDigitsAsString "ABCDEGHIJ"

    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeAt can be repeated in a chain`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 0
            |> AsyncSeq2.removeAt 0
            |> AsyncSeq2.removeAt 0
            |> AsyncSeq2.removeAt 0
            |> AsyncSeq2.removeAt 0
            |> verifyDigitsAsString "FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 9
            |> AsyncSeq2.removeAt 8
            |> AsyncSeq2.removeAt 7
            |> AsyncSeq2.removeAt 6
            |> AsyncSeq2.removeAt 5 // sequence gets shorter, pick last
            |> verifyDigitsAsString "ABCDE"
    }

    [<Fact>]
    let ``AsyncSeq2-removeAt can be applied to an infinite task sequence`` () =
        AsyncSeq2.initInfinite id
        |> AsyncSeq2.removeAt 10_000
        |> ColdTask.item 10_000
        |> Task.map (should equal 10_001)


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeAt throws when there are not enough elements`` variant =
        fun () ->
            AsyncSeq2.singleton 1
            // remove after 1
            |> AsyncSeq2.removeAt 2
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 10
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeAt 10_000_000
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt can remove last item`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 9 1
            |> verifyDigitsAsString "ABCDEFGHI"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt can remove multiple items`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 1 5
            |> verifyDigitsAsString "AGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt can with a large count past the end of the sequence is fine`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 2 20_000 // try to remove too many is fine, like Seq.removeManyAt (regardless the docs at time of writing)
            |> verifyDigitsAsString "AB"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt does not remove any item when count is zero`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 9 0
            |> verifyDigitsAsString "ABCDEFGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt does not remove any item when count is negative`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 1 -99
            |> verifyDigitsAsString "ABCDEFGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt removes items at indexed positions`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 0 5
            |> verifyDigitsAsString "FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 1 3
            |> verifyDigitsAsString "AEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 5 5
            |> verifyDigitsAsString "ABCDE"

    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt can be repeated in a chain`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 0 1
            |> AsyncSeq2.removeManyAt 0 2
            |> AsyncSeq2.removeManyAt 0 3
            |> verifyDigitsAsString "GHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 9 1 // pick last, result ABCDEFGHIJ
            |> AsyncSeq2.removeManyAt 6 1 // then from 6th pos, result ABCDEFHI
            |> AsyncSeq2.removeManyAt 3 2 // from 3rd pos take 2, result ABCFHI
            |> AsyncSeq2.removeManyAt 0 2 // from start, take 2, result CFHI
            |> verifyDigitsAsString "CFHI"
    }

    [<Fact>]
    let ``AsyncSeq2-removeManyAt can be applied to an infinite task sequence`` () =
        AsyncSeq2.initInfinite id
        |> AsyncSeq2.removeManyAt 10_000 5_000
        |> ColdTask.item 12_000
        |> Task.map (should equal 17_000)


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-removeManyAt throws when there are not enough elements for index`` variant =
        // NOTE: only raises if INDEX is out of bounds, not when COUNT is out of bounds!!!

        fun () ->
            AsyncSeq2.singleton 1
            // remove after 1
            |> AsyncSeq2.removeManyAt 2 0 // regardless of count, it should raise
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 10 5
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.removeManyAt 10_000_000 -5 // even with neg. count
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>



module SideEffects =

    // NOTES:
    //
    // no tests, it is not possible to create a meaningful side-effect test, as any consuming after
    // removing an item would logically require the side effect to be executed the normal way

    // PoC test
    [<Fact>]
    let ``Seq-removeAt (poc-proof) will execute side effect before index`` () =
        // NOTE: this test is for documentation purposes only, to show this behavior that is tested in this module
        // this shows that Seq.removeAt executes more side effects than necessary.

        let mutable x = 42

        let items = seq {
            yield x
            x <- x + 1 // we are proving this gets executed with removeAt(0), BUT this is the result of Seq.item
            yield x * 2
        }

        items
        |> Seq.removeAt 0
        |> Seq.item 0 // consume anything (this is why there's nothing to test with Seq.removeAt, as this is always true after removing an item)
        |> ignore

        x |> should equal 43 // one time side effect executed. QED
