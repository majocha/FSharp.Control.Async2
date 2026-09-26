module AsyncSeq2.Tests.UpdateAt

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation


//
// AsyncSeq2.updateAt
//

exception SideEffectPastEnd of string

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () = assertNullArg <| fun () -> AsyncSeq2.updateAt 0 99 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-updateAt(0) on empty input should throw ArgumentException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.updateAt 0 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-updateAt(-1) should throw ArgumentException on any input`` () =
        fun () ->
            (AsyncSeq2.empty<int> ())
            |> AsyncSeq2.updateAt -1 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            AsyncSeq2.init 10 id
            |> AsyncSeq2.updateAt -1 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-updateAt(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.updateAt -1 42
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        // test without awaiting the async
        |> should throw typeof<ArgumentException>

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-updateAt can update at end of sequence`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 9 99
            |> verifyDigitsAsString "ABCDEFGHI£"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-updateAt past end of sequence throws ArgumentException`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 10 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-updateAt updates item immediately after the indexed position`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 0 99
            |> verifyDigitsAsString "£BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 1 99
            |> verifyDigitsAsString "A£CDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 5 99
            |> verifyDigitsAsString "ABCDE£GHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 9 99
            |> verifyDigitsAsString "ABCDEFGHI£"
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-updateAt can be repeated in a chain`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 0 99
            |> AsyncSeq2.updateAt 1 99
            |> AsyncSeq2.updateAt 2 99
            |> AsyncSeq2.updateAt 3 99
            |> AsyncSeq2.updateAt 4 99
            |> verifyDigitsAsString "£££££FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 9 99
            |> AsyncSeq2.updateAt 8 99
            |> AsyncSeq2.updateAt 6 99
            |> AsyncSeq2.updateAt 4 99
            |> AsyncSeq2.updateAt 2 99
            |> verifyDigitsAsString "AB£D£F£H££"
    }


    [<Fact>]
    let ``AsyncSeq2-updateAt can be applied to an infinite task sequence`` () =
        AsyncSeq2.initInfinite id
        |> AsyncSeq2.updateAt 1_000_000 12345
        |> ColdTask.item 1_000_000
        |> Task.map (should equal 12345)


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-updateAt throws when there are not enough elements`` variant =
        fun () ->
            AsyncSeq2.singleton 1
            // update after 1
            |> AsyncSeq2.updateAt 2 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 10 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.updateAt 10_000_000 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>


module SideEffects =

    // PoC test
    [<Fact>]
    let ``Seq-updateAt (poc-proof) will NOT execute side effect just after index`` () =
        // NOTE: this test is for documentation purposes only, to show this behavior that is tested in this module
        // this shows that Seq.updateAt executes no extra side effects.

        let mutable x = 42

        let items = seq {
            yield x
            x <- x + 1 // we are proving this gets executed with updateAt(0)
            yield x * 2
        }

        items
        |> Seq.updateAt 0 99
        |> Seq.item 0 // put enumerator to updated item
        |> ignore

        x |> should equal 42 // one time side effect executed. QED

    [<Fact>]
    let ``AsyncSeq2-updateAt(0) will execute side effects at start of sequence`` () =
        // NOTE: while not strictly necessary, this mirrors behavior of Seq.updateAt

        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            x <- x + 1 // this is executed even with updateAt(0)
            yield x
            yield x * 2
        }

        items
        |> AsyncSeq2.updateAt 0 99
        |> ColdTask.item 0 // consume only the first item
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // the mutable was updated

    [<Fact>]
    let ``AsyncSeq2-updateAt will NOT execute last side effect when inserting past end`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            yield x * 2
            yield x * 4
            x <- x + 1 // this is executed when inserting past last item
        }

        items
        |> AsyncSeq2.updateAt 2 99
        |> ColdTask.item 2
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 42) // as with 'seq', see first test in this block, we prove NO SIDE EFFECTS


    [<Fact>]
    let ``AsyncSeq2-updateAt will NOT execute side effect just before index`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            x <- x + 1 // this is executed, even though we insert after the first item
            yield x * 2
            yield x * 4
        }

        items
        |> AsyncSeq2.updateAt 0 99
        |> ColdTask.item 0
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 42) // as with 'seq', see first test in this block, we prove NO SIDE EFFECTS

    [<Fact>]
    let ``AsyncSeq2-updateAt exception at update index is NOT thrown`` () =
        asyncSeq2 {
            yield 1
            yield! [ 2; 3 ]
            do SideEffectPastEnd "at the end" |> raise // this is NOT raised
            yield 4
        }
        |> AsyncSeq2.updateAt 2 99
        |> ColdTask.item 2
        |> Task.map (should equal 99)

    [<Fact>]
    let ``AsyncSeq2-updateAt prove that an exception from the asyncSeq2 is thrown instead of exception from function`` () =
        let items = asyncSeq2 {
            yield 42
            yield! [ 1; 2 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD get here before ArgumentException is raised
        }

        fun () -> items |> AsyncSeq2.updateAt 4 99 |> consumeTaskSeq // this would raise ArgumentException normally, but not now
        |> should throwAsyncExact typeof<SideEffectPastEnd>
