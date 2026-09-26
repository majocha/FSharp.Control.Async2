module AsyncSeq2.Tests.TakeWhile

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.takeWhile
// AsyncSeq2.takeWhileAsync
// AsyncSeq2.takeWhileInclusive
// AsyncSeq2.takeWhileInclusiveAsync
//

[<AutoOpen>]
module With =
    /// The only real difference in semantics between the base and the *Inclusive variant lies in whether the final item is returned.
    /// NOTE the semantics are very clear on only propagating a single failing item in the inclusive case.
    let getFunction inclusive isAsync =
        match inclusive, isAsync with
        | false, false -> AsyncSeq2.takeWhile
        | false, true -> fun pred -> AsyncSeq2.takeWhileAsync (fun value -> async2 { return pred value })
        | true, false -> AsyncSeq2.takeWhileInclusive
        | true, true -> fun pred -> AsyncSeq2.takeWhileInclusiveAsync (fun value -> async2 { return pred value })

    /// This is the base condition as one would expect in actual code
    let inline cond x = x <> 6

    /// For each of the tests below, we add a guard that will trigger if the predicate is passed items known to be beyond the
    /// first failing item in the known sequence (which is 1..10)
    let inline condWithGuard x =
        let res = cond x

        if x > 6 then
            failwith "Test sequence should not be enumerated beyond the first item failing the predicate"

        res

module EmptySeq =

    // AsyncSeq2-takeWhile+A stands for:
    // takeWhile + takeWhileAsync etc.

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-takeWhile+A has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.takeWhile ((=) 12)
            |> verifyEmpty

        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.takeWhileAsync (fun x -> async2 { return x = 12 })
            |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-takeWhileInclusive+A has no effect`` variant = task {
        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.takeWhileInclusive ((=) 12)
            |> verifyEmpty

        do!
            Gen.getEmptyVariant variant
            |> AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return x = 12 })
            |> verifyEmpty
    }

module Immutable =

    // AsyncSeq2-takeWhile+A stands for:
    // takeWhile + takeWhileAsync etc.

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-takeWhile+A filters correctly`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhile condWithGuard
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileAsync (fun x -> async2 { return condWithGuard x })
            |> verifyDigitsAsString "ABCDE"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-takeWhile+A does not pick first item when false`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhile ((=) 0)
            |> verifyDigitsAsString ""

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileAsync (fun x -> async2 { return x = 0 })
            |> verifyDigitsAsString ""
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-takeWhileInclusive+A filters correctly`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileInclusive condWithGuard
            |> verifyDigitsAsString "ABCDEF"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return condWithGuard x })
            |> verifyDigitsAsString "ABCDEF"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-takeWhileInclusive+A always pick at least the first item`` variant = task {
        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileInclusive ((=) 0)
            |> verifyDigitsAsString "A"

        do!
            Gen.getSeqImmutable variant
            |> AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return x = 0 })
            |> verifyDigitsAsString "A"
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-takeWhile+A filters correctly`` variant = task {
        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.takeWhile condWithGuard
            |> verifyDigitsAsString "ABCDE"

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.takeWhileAsync (fun x -> async2 { return condWithGuard x })
            |> verifyDigitsAsString "ABCDE"
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-takeWhileInclusive+A filters correctly`` variant = task {
        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.takeWhileInclusive condWithGuard
            |> verifyDigitsAsString "ABCDEF"

        do!
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return condWithGuard x })
            |> verifyDigitsAsString "ABCDEF"
    }

    [<Theory>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    [<InlineData(true, true)>]
    let ``AsyncSeq2-takeWhile and variants prove it does not read beyond the failing yield`` (inclusive, isAsync) = task {
        let mutable x = 42 // for this test, the potential mutation should not actually occur
        let functionToTest = getFunction inclusive isAsync ((=) 42)

        let items = asyncSeq2 {
            yield x // Always passes the test; always returned
            yield x * 2 // the failing item (which will also be yielded in the result when using *Inclusive)
            x <- x + 1 // we are proving we never get here
        }

        let expected = if inclusive then [| 42; 84 |] else [| 42 |]

        let! first = items |> functionToTest |> ColdTask.toArrayAsync
        let! repeat = items |> functionToTest |> ColdTask.toArrayAsync

        first |> should equal expected
        repeat |> should equal expected
        x |> should equal 42
    }

    [<Theory>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    [<InlineData(true, true)>]
    let ``AsyncSeq2-takeWhile and variants prove side effects are executed`` (inclusive, isAsync) = task {
        let mutable x = 41
        let functionToTest = getFunction inclusive isAsync ((>) 50)

        let items = asyncSeq2 {
            x <- x + 1
            yield x
            x <- x + 2
            yield x * 2
            x <- x + 200 // as previously proven, we should not trigger this
        }

        let expectedFirst = if inclusive then [| 42; 44 * 2 |] else [| 42 |]
        let expectedRepeat = if inclusive then [| 45; 47 * 2 |] else [| 45 |]

        x |> should equal 41
        let! first = items |> functionToTest |> ColdTask.toArrayAsync
        x |> should equal 44
        let! repeat = items |> functionToTest |> ColdTask.toArrayAsync
        x |> should equal 47

        first |> should equal expectedFirst
        repeat |> should equal expectedRepeat
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-takeWhile consumes the prefix of a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            AsyncSeq2.takeWhile (fun x -> x < 5) ts
            |> ColdTask.toArrayAsync

        let expected = [| 1..4 |]
        first |> should equal expected

        // side effect, reiterating causes it to resume from where we left it (minus the failing item)
        // which means the original sequence has now changed due to the side effect
        let! repeat =
            AsyncSeq2.takeWhile (fun x -> x < 5) ts
            |> ColdTask.toArrayAsync

        repeat |> should not' (equal expected)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-takeWhileInclusiveAsync consumes the prefix for a longer sequence, with mutation`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! first =
            AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return x < 5 }) ts
            |> ColdTask.toArrayAsync

        let expected = [| 1..5 |]
        first |> should equal expected

        // side effect, reiterating causes it to resume from where we left it (minus the failing item)
        // which means the original sequence has now changed due to the side effect
        let! repeat =
            AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return x < 5 }) ts
            |> ColdTask.toArrayAsync

        repeat |> should not' (equal expected)
    }

module Other =
    [<Theory>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    [<InlineData(true, true)>]
    let ``AsyncSeq2-takeWhile and variants excludes all items after predicate fails`` (inclusive, isAsync) =
        let functionToTest = With.getFunction inclusive isAsync

        [ 1; 2; 2; 3; 3; 2; 1 ]
        |> AsyncSeq2.ofSeq
        |> functionToTest (fun x -> x <= 2)
        |> verifyDigitsAsString (if inclusive then "ABBC" else "ABB")

    [<Theory>]
    [<InlineData(false, false)>]
    [<InlineData(false, true)>]
    [<InlineData(true, false)>]
    [<InlineData(true, true)>]
    let ``AsyncSeq2-takeWhile and variants stops consuming after predicate fails`` (inclusive, isAsync) =
        let functionToTest = With.getFunction inclusive isAsync

        seq {
            yield! [ 1; 2; 2; 3; 3 ]
            yield failwith "Too far"
        }
        |> AsyncSeq2.ofSeq
        |> functionToTest (fun x -> x <= 2)
        |> verifyDigitsAsString (if inclusive then "ABBC" else "ABB")
