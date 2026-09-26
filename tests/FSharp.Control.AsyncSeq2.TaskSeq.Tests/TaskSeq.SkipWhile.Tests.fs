module AsyncSeq2.Tests.skipWhile

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.skipWhile
// TaskCallbacks.skipWhileAsync
// AsyncSeq2.skipWhileInclusive
// TaskCallbacks.skipWhileInclusiveAsync
//

exception SideEffectPastEnd of string


module EmptySeq =

    // AsyncSeq2-skipWhile+A stands for:
    // skipWhile + skipWhileAsync etc.

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-skipWhile+A has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.skipWhile ((=) 12)
            |> verifyEmpty

        do!
            Gen.getEmptyVariant variant
            |> TaskCallbacks.skipWhileAsync ((=) 12 >> Task.fromResult)
            |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-skipWhileInclusive+A has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.skipWhileInclusive ((=) 12)
            |> verifyEmpty

        do!
            Gen.getEmptyVariant variant
            |> TaskCallbacks.skipWhileInclusiveAsync ((=) 12 >> Task.fromResult)
            |> verifyEmpty
    }

module Immutable =

    // AsyncSeq2-skipWhile+A stands for:
    // skipWhile + skipWhileAsync etc.

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skipWhile+A filters correctly`` variant = task {
        // truth table for f(x) = x < 5
        // 1 2 3 4 5 6 7 8 9 10
        // T T T T F F F F F F (stops at first F)
        // x x x x _ _ _ _ _ _ (skips exclusive)
        // A B C D E F G H I J

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skipWhile (fun x -> x < 5)
            |> verifyDigitsAsString "EFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> TaskCallbacks.skipWhileAsync (fun x -> task { return x < 5 })
            |> verifyDigitsAsString "EFGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skipWhile+A does not skip first item when false`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skipWhile ((=) 0)
            |> verifyDigitsAsString "ABCDEFGHIJ" // all 10 remain!

        do!
            Gen.getSeqImmutable variant
            |> TaskCallbacks.skipWhileAsync ((=) 0 >> Task.fromResult)
            |> verifyDigitsAsString "ABCDEFGHIJ" // all 10 remain!
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skipWhileInclusive+A filters correctly`` variant = task {
        // truth table for f(x) = x < 5
        // 1 2 3 4 5 6 7 8 9 10
        // T T T T F F F F F F (stops at first F)
        // x x x x x _ _ _ _ _ (skips inclusively)
        // A B C D E F G H I J

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skipWhileInclusive (fun x -> x < 5)
            |> verifyDigitsAsString "FGHIJ" // last 4

        do!
            Gen.getSeqImmutable variant
            |> TaskCallbacks.skipWhileInclusiveAsync (fun x -> task { return x < 5 })
            |> verifyDigitsAsString "FGHIJ"
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skipWhileInclusive+A returns the empty sequence if always true`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skipWhileInclusive (fun x -> x > -1) // always true
            |> verifyEmpty

        do!
            Gen.getSeqImmutable variant
            |> TaskCallbacks.skipWhileInclusiveAsync (fun x -> Task.fromResult (x > -1))
            |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-skipWhileInclusive+A always skips at least the first item`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.skipWhileInclusive ((=) 0)
            |> verifyDigitsAsString "BCDEFGHIJ"

        do!
            Gen.getSeqImmutable variant
            |> TaskCallbacks.skipWhileInclusiveAsync ((=) 0 >> Task.fromResult)
            |> verifyDigitsAsString "BCDEFGHIJ"
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-skipWhile+A filters correctly`` variant = task {
        // truth table for f(x) = x < 6
        // 1 2 3 4 5 6 7 8 9 10
        // T T T T T F F F F F (stops at first F)
        // x x x x x _ _ _ _ _ (skips exclusively)
        // A B C D E F G H I J

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.skipWhile (fun x -> x < 6)
            |> verifyDigitsAsString "FGHIJ"

        do!
            Gen.getSeqWithSideEffect variant
            |> TaskCallbacks.skipWhileAsync (fun x -> task { return x < 6 })
            |> verifyDigitsAsString "FGHIJ"
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-skipWhileInclusive+A filters correctly`` variant = task {
        // truth table for f(x) = x < 6
        // 1 2 3 4 5 6 7 8 9 10
        // T T T T T F F F F F (stops at first F)
        // x x x x x x _ _ _ _ (skips inclusively)
        // A B C D E F G H I J

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.skipWhileInclusive (fun x -> x < 6)
            |> verifyDigitsAsString "GHIJ"

        do!
            Gen.getSeqWithSideEffect variant
            |> TaskCallbacks.skipWhileInclusiveAsync (fun x -> task { return x < 6 })
            |> verifyDigitsAsString "GHIJ"
    }

    [<Fact>]
    let ``AsyncSeq2-skipWhile and variants prove it reads the entire input stream`` () =

        let mutable x = 42

        let items = asyncSeq2 {
            yield x
            yield x * 2
            x <- x + 1 // we are proving we ALWAYS get here
        }

        // this needs to be lifted from the task or it raises the infamous
        // warning FS3511 on CI: This state machine is not statically compilable
        let testSkipper skipper expected = task {
            let! first = items |> skipper |> ColdTask.toArrayAsync
            return first |> should equal expected
        }

        task {
            do! testSkipper (AsyncSeq2.skipWhile ((=) 42)) [| 84 |]
            x |> should equal 43

            do! testSkipper (AsyncSeq2.skipWhileInclusive ((=) 43)) [||]
            x |> should equal 44

            do! testSkipper (TaskCallbacks.skipWhileAsync (fun x -> Task.fromResult (x = 44))) [| 88 |]
            x |> should equal 45

            do! testSkipper (TaskCallbacks.skipWhileInclusiveAsync (fun x -> Task.fromResult (x = 45))) [||]
            x |> should equal 46
        }

    [<Fact>]
    let ``AsyncSeq2-skipWhile and variants prove side effects are properly executed`` () =
        let mutable x = 41

        let items = asyncSeq2 {
            x <- x + 1
            yield x
            x <- x + 2
            yield x * 2
            x <- x + 200 // as previously proven, we should ALWAYS trigger this
        }

        // this needs to be lifted from the task or it raises the infamous
        // warning FS3511 on CI: This state machine is not statically compilable
        let testSkipper skipper expected = task {
            let! first = items |> skipper |> ColdTask.toArrayAsync
            return first |> should equal expected
        }

        task {
            do! testSkipper (AsyncSeq2.skipWhile ((=) 42)) [| 88 |]
            x |> should equal 244

            do! testSkipper (AsyncSeq2.skipWhileInclusive ((=) 245)) [||]
            x |> should equal 447

            do! testSkipper (TaskCallbacks.skipWhileAsync (fun x -> Task.fromResult (x = 448))) [| 900 |]
            x |> should equal 650

            do! testSkipper (TaskCallbacks.skipWhileInclusiveAsync (fun x -> Task.fromResult (x = 651))) [||]
            x |> should equal 853
        }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-skipWhile consumes the prefix of a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            AsyncSeq2.skipWhile (fun x -> x < 5) ts
            |> ColdTask.toArrayAsync

        let expected = [| 5..10 |]
        first |> should equal expected

        // side effect, reiterating causes it to resume from where we left it (minus the failing item)
        // which means the original sequence has now changed due to the side effect
        let! repeat =
            AsyncSeq2.skipWhile (fun x -> x < 5) ts
            |> ColdTask.toArrayAsync

        repeat |> should not' (equal expected)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-skipWhileInclusiveAsync consumes the prefix for a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            TaskCallbacks.skipWhileInclusiveAsync (fun x -> task { return x < 5 }) ts
            |> ColdTask.toArrayAsync

        let expected = [| 6..10 |]
        first |> should equal expected

        // side effect, reiterating causes it to resume from where we left it (minus the failing item)
        // which means the original sequence has now changed due to the side effect
        let! repeat =
            TaskCallbacks.skipWhileInclusiveAsync (fun x -> task { return x < 5 }) ts
            |> ColdTask.toArrayAsync

        repeat |> should not' (equal expected)
    }

module Other =
    [<Fact>]
    let ``AsyncSeq2-skipWhileXXX should include all items after predicate fails`` () = task {
        do!
            [ 1; 2; 2; 3; 3; 2; 1 ]
            |> AsyncSeq2.ofSeq
            |> AsyncSeq2.skipWhile (fun x -> x <= 2)
            |> verifyDigitsAsString "CCBA"

        do!
            [ 1; 2; 2; 3; 3; 2; 1 ]
            |> AsyncSeq2.ofSeq
            |> AsyncSeq2.skipWhileInclusive (fun x -> x <= 2)
            |> verifyDigitsAsString "CBA"

        do!
            [ 1; 2; 2; 3; 3; 2; 1 ]
            |> AsyncSeq2.ofSeq
            |> TaskCallbacks.skipWhileAsync (fun x -> Task.fromResult (x <= 2))
            |> verifyDigitsAsString "CCBA"

        do!
            [ 1; 2; 2; 3; 3; 2; 1 ]
            |> AsyncSeq2.ofSeq
            |> TaskCallbacks.skipWhileInclusiveAsync (fun x -> Task.fromResult (x <= 2))
            |> verifyDigitsAsString "CBA"
    }

    [<Fact>]
    let ``AsyncSeq2-skipWhileXXX stops consuming after predicate fails`` () =
        let testSkipper skipper =
            fun () ->
                seq {
                    yield! [ 1; 2; 2; 3; 3 ]
                    yield SideEffectPastEnd "Too far" |> raise
                }
                |> AsyncSeq2.ofSeq
                |> skipper
                |> consumeTaskSeq
            |> should throwAsyncExact typeof<SideEffectPastEnd>

        testSkipper (AsyncSeq2.skipWhile (fun x -> x <= 2))
        testSkipper (AsyncSeq2.skipWhileInclusive (fun x -> x <= 2))
        testSkipper (TaskCallbacks.skipWhileAsync (fun x -> Task.fromResult (x <= 2)))
        testSkipper (TaskCallbacks.skipWhileInclusiveAsync (fun x -> Task.fromResult (x <= 2)))
