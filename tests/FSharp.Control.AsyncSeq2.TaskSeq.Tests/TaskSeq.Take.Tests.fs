module AsyncSeq2.Tests.Take

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.take
// AsyncSeq2.truncate
//

exception SideEffectPastEnd of string

module EmptySeq =
    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-take(0) has no effect on empty input`` variant =
        // no `task` block needed
        Gen.getEmptyVariant variant |> AsyncSeq2.take 0 |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-take(1) on empty input should throw InvalidOperation`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.take 1
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-take(-1) should throw ArgumentException on any input`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.take -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> AsyncSeq2.init 10 id |> AsyncSeq2.take -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-take(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.take -1
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        |> should throw typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-truncate(0) has no effect on empty input`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.truncate 0
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-truncate(99) does not throw on empty input`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.truncate 99
        |> verifyEmpty


    [<Fact>]
    let ``AsyncSeq2-truncate(-1) should throw ArgumentException on any input`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.truncate -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> AsyncSeq2.init 10 id |> AsyncSeq2.truncate -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-truncate(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.truncate -1
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        |> should throw typeof<ArgumentException>

module Immutable =

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-take returns exactly 'count' items`` variant = task {

        do! Gen.getSeqImmutable variant |> AsyncSeq2.take 0 |> verifyEmpty

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.take 1
            |> verifyDigitsAsString "A"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.take 5
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.take 10
            |> verifyDigitsAsString "ABCDEFGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-take throws when there are not enough elements`` variant =
        fun () -> AsyncSeq2.init 1 id |> AsyncSeq2.take 2 |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.take 11
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.take 10_000_000
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-truncate returns at least 'count' items`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 0
            |> verifyEmpty

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 1
            |> verifyDigitsAsString "A"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 5
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 10
            |> verifyDigitsAsString "ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 11
            |> verifyDigitsAsString "ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.truncate 10_000_000
            |> verifyDigitsAsString "ABCDEFGHIJ"
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-take gets enough items`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.take 5
        |> verifyDigitsAsString "ABCDE"

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-truncate gets enough items`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.truncate 5
        |> verifyDigitsAsString "ABCDE"

    [<Fact>]
    let ``AsyncSeq2-take prove it does not read beyond the last yield`` () = task {
        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            yield x
            yield x * 2
            x <- x + 1 // we are proving we never get here
        }

        let expected = [| 42; 84 |]

        let! first = items |> AsyncSeq2.take 2 |> ColdTask.toArrayAsync
        let! repeat = items |> AsyncSeq2.take 2 |> ColdTask.toArrayAsync

        first |> should equal expected
        repeat |> should equal expected // if we read too far, this is now [|43, 86|]
        x |> should equal 42 // expect: side-effect at end of asyncSeq2 not executed
    }

    [<Fact>]
    let ``AsyncSeq2-take prove that an exception that is not consumed, is not raised`` () =
        let items = asyncSeq2 {
            yield 1
            yield! [ 2; 3 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD NOT get here
        }

        items |> AsyncSeq2.take 3 |> verifyDigitsAsString "ABC"


    [<Fact>]
    let ``AsyncSeq2-take prove that an exception from the asyncSeq2 is thrown instead of exception from function`` () =
        let items = asyncSeq2 {
            yield 42
            yield! [ 1; 2 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD get here before ArgumentException is raised
        }

        fun () -> items |> AsyncSeq2.take 4 |> consumeTaskSeq // this would raise ArgumentException normally
        |> should throwAsyncExact typeof<SideEffectPastEnd>


    [<Fact>]
    let ``AsyncSeq2-truncate prove it does not read beyond the last yield`` () = task {
        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            yield x
            yield x * 2
            x <- x + 1 // we are proving we never get here
        }

        let expected = [| 42; 84 |]

        let! first = items |> AsyncSeq2.truncate 2 |> ColdTask.toArrayAsync
        let! repeat = items |> AsyncSeq2.truncate 2 |> ColdTask.toArrayAsync

        first |> should equal expected
        repeat |> should equal expected // if we read too far, this is now [|43, 86|]
        x |> should equal 42 // expect: side-effect at end of asyncSeq2 not executed
    }
