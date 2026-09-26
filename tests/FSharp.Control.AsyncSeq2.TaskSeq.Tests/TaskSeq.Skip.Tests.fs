module AsyncSeq2.Tests.Skip

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.skip
// AsyncSeq2.drop
//

exception SideEffectPastEnd of string

module EmptySeq =
    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-skip(0) has no effect on empty input`` variant =
        // no `task` block needed
        Gen.getEmptyVariant variant |> AsyncSeq2.skip 0 |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-skip(1) on empty input should throw InvalidOperation`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.skip 1
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-skip(-1) should throw ArgumentException on any input`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.skip -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> AsyncSeq2.init 10 id |> AsyncSeq2.skip -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-skip(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.skip -1
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        |> should throw typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-drop(0) has no effect on empty input`` variant = Gen.getEmptyVariant variant |> AsyncSeq2.drop 0 |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-drop(99) does not throw on empty input`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.drop 99
        |> verifyEmpty


    [<Fact>]
    let ``AsyncSeq2-drop(-1) should throw ArgumentException on any input`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.drop -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> AsyncSeq2.init 10 id |> AsyncSeq2.drop -1 |> consumeTaskSeq
        |> should throwAsyncExact typeof<ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-drop(-1) should throw ArgumentException before awaiting`` () =
        fun () ->
            asyncSeq2 {
                do! longDelay ()

                if false then
                    yield 0 // type inference
            }
            |> AsyncSeq2.drop -1
            |> ignore // throws even without running the async. Bad coding, don't ignore a task!

        |> should throw typeof<ArgumentException>

module Immutable =

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skip skips over exactly 'count' items`` variant = task {

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 0
            |> verifyDigitsAsString "ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 1
            |> verifyDigitsAsString "BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 5
            |> verifyDigitsAsString "FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 10
            |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skip throws when there are not enough elements`` variant =
        fun () -> AsyncSeq2.init 1 id |> AsyncSeq2.skip 2 |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 11
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skip 10_000_000
            |> consumeTaskSeq

        |> should throwAsyncExact typeof<ArgumentException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-drop skips over at least 'count' items`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 0
            |> verifyDigitsAsString "ABCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 1
            |> verifyDigitsAsString "BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 5
            |> verifyDigitsAsString "FGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 10
            |> verifyEmpty

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 11 // no exception
            |> verifyEmpty

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.drop 10_000_000 // no exception
            |> verifyEmpty
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-skip skips over enough items`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.skip 5
        |> verifyDigitsAsString "FGHIJ"

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-drop skips over enough items`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.drop 5
        |> verifyDigitsAsString "FGHIJ"

    [<Fact>]
    let ``AsyncSeq2-skip prove we do not skip side effects`` () = task {
        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            yield x
            yield x * 2
            x <- x + 1 // we are proving we never get here
        }

        let! first = items |> AsyncSeq2.skip 2 |> ColdTask.toArrayAsync
        let! repeat = items |> AsyncSeq2.skip 2 |> ColdTask.toArrayAsync

        first |> should equal Array.empty<int>
        repeat |> should equal Array.empty<int>
        x |> should equal 44 // expect: side-effect is executed twice by now
    }

    [<Fact>]
    let ``AsyncSeq2-skip prove that an exception from the asyncSeq2 is thrown instead of exception from function`` () =
        let items = asyncSeq2 {
            yield 42
            yield! [ 1; 2 ]
            do SideEffectPastEnd "at the end" |> raise // we SHOULD get here before ArgumentException is raised
        }

        fun () -> items |> AsyncSeq2.skip 4 |> consumeTaskSeq // this would raise ArgumentException normally
        |> should throwAsyncExact typeof<SideEffectPastEnd>


    [<Fact>]
    let ``AsyncSeq2-drop prove we do not skip side effects at the end`` () = task {
        let mutable x = 42 // for this test, the potential mutation should not actually occur

        let items = asyncSeq2 {
            yield x
            yield x * 2
            x <- x + 1 // we are proving we never get here
        }

        let! first = items |> AsyncSeq2.drop 2 |> ColdTask.toArrayAsync
        let! repeat = items |> AsyncSeq2.drop 2 |> ColdTask.toArrayAsync

        first |> should equal Array.empty<int>
        repeat |> should equal Array.empty<int>
        x |> should equal 44 // expect: side-effect at end is executed twice by now
    }
