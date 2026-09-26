module AsyncSeq2.Tests.InsertAt

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation


//
// AsyncSeq2.insertAt
// AsyncSeq2.insertManyAt
//

exception SideEffectPastEnd of string

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.insertAt 0 99 null

        assertNullArg
        <| fun () -> AsyncSeq2.insertManyAt 0 (AsyncSeq2.empty ()) null

    [<Fact>]
    let ``Null values argument is invalid for insertManyAt`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.insertManyAt 0 null (AsyncSeq2.ofList [ 1 ])

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-insertAt(0) on empty input returns singleton`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.insertAt 0 42
        |> verifySingleton 42

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-insertAt(1) on empty input should throw ArgumentException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.insertAt 1 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-insertAt(-1) should throw ArgumentException on any input`` () =
        fun () ->
            (AsyncSeq2.empty<int> ())
            |> AsyncSeq2.insertAt -1 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            AsyncSeq2.init 10 id
            |> AsyncSeq2.insertAt -1 42
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-insertAt(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.insertAt -1 42
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        // test without awaiting the async
        |> should throw typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-insertManyAt(0) on empty input returns singleton`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.insertManyAt 0 (AsyncSeq2.ofArray [| 42; 43; 44 |])
        |> verifyDigitsAsString "jkl"

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-insertManyAt(1) on empty input should throw InvalidOperation`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.insertManyAt 1 (AsyncSeq2.ofArray [| 42; 43; 44 |])
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-insertManyAt(-1) should throw ArgumentException on any input`` () =
        fun () ->
            (AsyncSeq2.empty<int> ())
            |> AsyncSeq2.insertManyAt -1 (AsyncSeq2.ofArray [| 42; 43; 44 |])
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            AsyncSeq2.init 10 id
            |> AsyncSeq2.insertManyAt -1 (AsyncSeq2.ofArray [| 42; 43; 44 |])
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-insertManyAt(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.insertManyAt -1 (AsyncSeq2.ofArray [| 42; 43; 44 |])
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        // test without awaiting the async
        |> should throw typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-insertManyAt() with empty sequenc as source`` () =
        (AsyncSeq2.empty<int> ())
        |> AsyncSeq2.insertManyAt 0 (AsyncSeq2.empty ())
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-insertManyAt() with empty sequence as source applies to non-empty sequence`` () =
        AsyncSeq2.init 10 id
        |> AsyncSeq2.insertManyAt 2 (AsyncSeq2.empty ())
        |> verify0To9

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertAt can insert after end of sequence`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 10 99
            |> verifyDigitsAsString "ABCDEFGHIJ£"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertAt inserts item immediately after the indexed position`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 0 99
            |> verifyDigitsAsString "£ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 1 99
            |> verifyDigitsAsString "A£BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 5 99
            |> verifyDigitsAsString "ABCDE£FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 10 99
            |> verifyDigitsAsString "ABCDEFGHIJ£"
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertAt can be repeated in a chain`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 0 99
            |> AsyncSeq2.insertAt 0 99
            |> AsyncSeq2.insertAt 0 99
            |> AsyncSeq2.insertAt 0 99
            |> AsyncSeq2.insertAt 0 99
            |> verifyDigitsAsString "£££££ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 10 99
            |> AsyncSeq2.insertAt 11 99
            |> AsyncSeq2.insertAt 12 99
            |> AsyncSeq2.insertAt 13 99
            |> AsyncSeq2.insertAt 14 99
            |> verifyDigitsAsString "ABCDEFGHIJ£££££"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertAt applies to a position in the new sequence`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 0 99
            |> AsyncSeq2.insertAt 2 99
            |> AsyncSeq2.insertAt 4 99
            |> AsyncSeq2.insertAt 6 99
            |> AsyncSeq2.insertAt 8 99
            |> AsyncSeq2.insertAt 10 99
            |> AsyncSeq2.insertAt 12 99
            |> AsyncSeq2.insertAt 14 99
            |> AsyncSeq2.insertAt 16 99
            |> AsyncSeq2.insertAt 18 99
            |> AsyncSeq2.insertAt 20 99
            |> verifyDigitsAsString "£A£B£C£D£E£F£G£H£I£J£"
    }

    [<Fact>]
    let ``AsyncSeq2-insertAt can be applied to an infinite task sequence`` () =
        AsyncSeq2.initInfinite id
        |> AsyncSeq2.insertAt 100 12345
        |> ColdTask.item 100
        |> Task.map (should equal 12345)


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertAt throws when there are not enough elements`` variant =
        fun () ->
            AsyncSeq2.singleton 1
            // insert after 1
            |> AsyncSeq2.insertAt 2 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 11 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertAt 10_000_000 99
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertManyAt can insert after end of sequence`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 10 (AsyncSeq2.ofArray [| 99; 100; 101 |])
            |> verifyDigitsAsString "ABCDEFGHIJ£¤¥"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertManyAt inserts item immediately after the indexed position`` variant = task {
        let values = AsyncSeq2.ofArray [| 99; 100; 101 |]

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 0 values
            |> verifyDigitsAsString "£¤¥ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 1 values
            |> verifyDigitsAsString "A£¤¥BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 5 values
            |> verifyDigitsAsString "ABCDE£¤¥FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 10 values
            |> verifyDigitsAsString "ABCDEFGHIJ£¤¥"
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertManyAt can be repeated in a chain`` variant = task {
        let values = AsyncSeq2.ofArray [| 99; 100; 101 |]

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 0 values
            |> AsyncSeq2.insertManyAt 0 values
            |> AsyncSeq2.insertManyAt 0 values
            |> AsyncSeq2.insertManyAt 0 values
            |> AsyncSeq2.insertManyAt 0 values
            |> verifyDigitsAsString "£¤¥£¤¥£¤¥£¤¥£¤¥ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 10 values
            |> AsyncSeq2.insertManyAt 11 values
            |> AsyncSeq2.insertManyAt 12 values
            |> AsyncSeq2.insertManyAt 13 values
            |> AsyncSeq2.insertManyAt 14 values
            |> verifyDigitsAsString "ABCDEFGHIJ£££££¤¥¤¥¤¥¤¥¤¥"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertManyAt applies to a position in the new sequence`` variant = task {
        let values = AsyncSeq2.ofArray [| 99; 100; 101 |]

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 0 values
            |> AsyncSeq2.insertManyAt 4 values
            |> AsyncSeq2.insertManyAt 8 values
            |> AsyncSeq2.insertManyAt 12 values
            |> AsyncSeq2.insertManyAt 16 values
            |> AsyncSeq2.insertManyAt 20 values
            |> AsyncSeq2.insertManyAt 24 values
            |> AsyncSeq2.insertManyAt 28 values
            |> AsyncSeq2.insertManyAt 32 values
            |> AsyncSeq2.insertManyAt 36 values
            |> AsyncSeq2.insertManyAt 40 values
            |> verifyDigitsAsString "£¤¥A£¤¥B£¤¥C£¤¥D£¤¥E£¤¥F£¤¥G£¤¥H£¤¥I£¤¥J£¤¥"
    }

    [<Fact>]
    let ``AsyncSeq2-insertManyAt (infinite) can be applied to an infinite task sequence`` () =
        AsyncSeq2.initInfinite id
        |> AsyncSeq2.insertManyAt 100 (AsyncSeq2.init 10 id)
        |> ColdTask.item 109
        |> Task.map (should equal 9)



    [<Fact>]
    let ``AsyncSeq2-insertManyAt (infinite) with infinite task sequence as argument`` () =
        AsyncSeq2.init 100 id
        |> AsyncSeq2.insertManyAt 100 (AsyncSeq2.initInfinite id)
        |> ColdTask.item 1999
        |> Task.map (should equal 1899) // the inserted infinite sequence started at 100, with value 0.

    [<Fact>]
    let ``AsyncSeq2-insertManyAt (infinite) with source and values both as infinite task sequence`` () = task {

        // using two infinite task sequences
        let ts =
            AsyncSeq2.initInfinite id
            |> AsyncSeq2.insertManyAt 1000 (AsyncSeq2.initInfinite id)

        // the inserted infinite sequence started at 1000, with value 0.
        do! ts |> ColdTask.item 999 |> Task.map (should equal 999)
        do! ts |> ColdTask.item 1000 |> Task.map (should equal 0)
        do! ts |> ColdTask.item 2000 |> Task.map (should equal 1000)
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-insertManyAt throws when there are not enough elements`` variant =
        let values = AsyncSeq2.ofArray [| 99; 100; 101 |]

        fun () ->
            AsyncSeq2.singleton 1
            // insert after 1
            |> AsyncSeq2.insertManyAt 2 values
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 11 values
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.insertManyAt 10_000_000 values
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>



module SideEffects =

    // PoC test
    [<Fact>]
    let ``Seq-insertAt (poc-proof) will execute side effect before index`` () =
        // NOTE: this test is for documentation purposes only, to show this behavior that is tested in this module
        // this shows that Seq.insertAt executes more side effects than necessary.

        let mutable x = 42

        let items = seq {
            x <- x + 1 // we are proving this gets executed with insertAt(0)
            yield x
            yield x * 2
        }

        items
        |> Seq.insertAt 0 99
        |> Seq.item 0 // put enumerator to inserted item
        |> ignore

        x |> should equal 43 // one time side effect executed. QED

    [<Fact>]
    let ``AsyncSeq2-insertAt(0) will execute side effects at start of sequence`` () =
        // NOTE: while not strictly necessary, this mirrors behavior of Seq.insertAt

        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            x <- x + 1 // this is executed even with insertAt(0)
            yield x
            yield x * 2
        }

        items
        |> AsyncSeq2.insertAt 0 99
        |> ColdTask.item 0 // consume only the first item
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // the mutable was updated

    [<Fact>]
    let ``AsyncSeq2-insertAt will execute last side effect when inserting past end`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            yield x * 2
            yield x * 4
            x <- x + 1 // this is executed when inserting past last item
        }

        items
        |> AsyncSeq2.insertAt 3 99
        |> ColdTask.item 3
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // as with 'seq', see first test in this block, we execute the side effect at index


    [<Fact>]
    let ``AsyncSeq2-insertAt will execute side effect just before index`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            x <- x + 1 // this is executed, even though we insert after the first item
            yield x * 2
            yield x * 4
        }

        items
        |> AsyncSeq2.insertAt 1 99
        |> ColdTask.item 1
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // as with 'seq', see first test in this block, we execute the side effect at index

    [<Fact>]
    let ``AsyncSeq2-insertAt exception at insertion index is thrown`` () =
        fun () ->
            asyncSeq2 {
                yield 1
                yield! [ 2; 3 ]
                do SideEffectPastEnd "at the end" |> raise // this is raised
                yield 4
            }
            |> AsyncSeq2.insertAt 3 99
            |> ColdTask.item 3
            |> Task.ignore

        |> should throwAsyncExact typeof<SideEffectPastEnd>

    [<Fact>]
    let ``AsyncSeq2-insertAt prove that an exception from the asyncSeq2 is thrown instead of exception from function`` () =
        let items = asyncSeq2 {
            yield 42
            yield! [ 1; 2 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD get here before ArgumentException is raised
        }

        fun () -> items |> AsyncSeq2.insertAt 4 99 |> consumeTaskSeq // this would raise ArgumentException normally, but not now
        |> should throwAsyncExact typeof<SideEffectPastEnd>

    [<Fact>]
    let ``AsyncSeq2-insertManyAt(0) will execute side effects at start of sequence`` () =
        // NOTE: while not strictly necessary, this mirrors behavior of Seq.insertManyAt

        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            x <- x + 1 // this is executed even with insertManyAt(0)
            yield x
            yield x * 2
        }

        items
        |> AsyncSeq2.insertManyAt 0 (asyncSeq2 { yield! [ 99; 100 ] })
        |> ColdTask.item 0 // consume only the first item
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // the mutable was updated

    [<Fact>]
    let ``AsyncSeq2-insertManyAt will execute last side effect when inserting past end`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            yield x * 2
            yield x * 4
            x <- x + 1 // this is executed when inserting past last item
        }

        items
        |> AsyncSeq2.insertManyAt 3 (asyncSeq2 { yield! [ 99; 100 ] })
        |> ColdTask.item 3
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // as with 'seq', see first test in this block, we execute the side effect at index


    [<Fact>]
    let ``AsyncSeq2-insertManyAt will execute side effect just before index`` () =
        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            x <- x + 1 // this is executed, even though we insert after the first item
            yield x * 2
            yield x * 4
        }

        items
        |> AsyncSeq2.insertManyAt 1 (asyncSeq2 { yield! [ 99; 100 ] })
        |> ColdTask.item 1
        |> Task.map (should equal 99)
        |> Task.map (fun () -> x |> should equal 43) // as with 'seq', see first test in this block, we execute the side effect at index

    [<Fact>]
    let ``AsyncSeq2-insertManyAt exception at insertion index is thrown`` () =
        fun () ->
            asyncSeq2 {
                yield 1
                yield! [ 2; 3 ]
                do SideEffectPastEnd "at the end" |> raise // this is raised
                yield 4
            }
            |> AsyncSeq2.insertManyAt 3 (asyncSeq2 { yield! [ 99; 100 ] })
            |> ColdTask.item 3
            |> Task.ignore

        |> should throwAsyncExact typeof<SideEffectPastEnd>

    [<Fact>]
    let ``AsyncSeq2-insertManyAt prove that an exception from the asyncSeq2 is thrown instead of exception from function`` () =
        let items = asyncSeq2 {
            yield 42
            yield! [ 1; 2 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD get here before ArgumentException is raised
        }

        fun () ->
            items
            |> AsyncSeq2.insertManyAt 4 (asyncSeq2 { yield! [ 99; 100 ] })
            |> consumeTaskSeq // this would raise ArgumentException normally, but not now

        |> should throwAsyncExact typeof<SideEffectPastEnd>
