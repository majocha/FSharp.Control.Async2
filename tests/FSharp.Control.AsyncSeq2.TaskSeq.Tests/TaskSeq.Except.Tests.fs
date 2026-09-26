module AsyncSeq2.Tests.Except

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.except
// AsyncSeq2.exceptOfSeq
//


module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.except null (AsyncSeq2.empty ())
        assertNullArg <| fun () -> AsyncSeq2.except (AsyncSeq2.empty ()) null
        assertNullArg <| fun () -> AsyncSeq2.except null null

        assertNullArg
        <| fun () -> AsyncSeq2.exceptOfSeq null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.exceptOfSeq Seq.empty null

        assertNullArg <| fun () -> AsyncSeq2.exceptOfSeq null null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-except`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.except (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-exceptOfSeq`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.exceptOfSeq Seq.empty
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-except v2`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.except (AsyncSeq2.empty ())
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-except v3`` variant =
        (AsyncSeq2.empty ())
        |> AsyncSeq2.except (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-except no side effect in exclude seq if source seq is empty`` variant =
        let mutable i = 0

        // The `exclude` argument of AsyncSeq2.except is only iterated after the first item
        // from the input. With empty input, this is not evaluated
        let exclude = asyncSeq2 {
            i <- i + 1 // we test that we never get here
            yield 12
        }

        Gen.getEmptyVariant variant
        |> AsyncSeq2.except exclude
        |> verifyEmpty
        |> Task.map (fun () -> i |> should equal 0) // exclude seq is only enumerated after first item in source

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-except removes duplicates`` variant =
        AsyncSeq2.ofList [ 1; 1; 2; 3; 4; 12; 12; 12; 13; 13; 13; 13; 13; 99 ]
        |> AsyncSeq2.except (Gen.getSeqImmutable variant)
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 12; 13; 99 |])

    [<Fact>]
    let ``AsyncSeq2-except removes duplicates with empty itemsToExcept`` () =
        AsyncSeq2.ofList [ 1; 1; 2; 3; 4; 12; 12; 12; 13; 13; 13; 13; 13; 99 ]
        |> AsyncSeq2.except (AsyncSeq2.empty ())
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 1; 2; 3; 4; 12; 13; 99 |])

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-except removes everything`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.except (Gen.getSeqImmutable variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-except removes everything with duplicates`` variant =
        asyncSeq2 {
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
        }
        |> AsyncSeq2.except (Gen.getSeqImmutable variant)
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-exceptOfSeq removes duplicates`` () =
        AsyncSeq2.ofList [ 1; 1; 2; 3; 4; 12; 12; 12; 13; 13; 13; 13; 13; 99 ]
        |> AsyncSeq2.exceptOfSeq [ 1..10 ]
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 12; 13; 99 |])

    [<Fact>]
    let ``AsyncSeq2-exceptOfSeq removes duplicates with empty itemsToExcept`` () =
        AsyncSeq2.ofList [ 1; 1; 2; 3; 4; 12; 12; 12; 13; 13; 13; 13; 13; 99 ]
        |> AsyncSeq2.exceptOfSeq Seq.empty
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 1; 2; 3; 4; 12; 13; 99 |])

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exceptOfSeq removes everything`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.exceptOfSeq [ 1..10 ]
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exceptOfSeq removes everything with duplicates`` variant =
        asyncSeq2 {
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
            yield! Gen.getSeqImmutable variant
        }
        |> AsyncSeq2.exceptOfSeq [ 1..10 ]
        |> verifyEmpty

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-except removes duplicates`` variant =
        AsyncSeq2.ofList [ 1; 1; 2; 3; 4; 12; 12; 12; 13; 13; 13; 13; 13; 99 ]
        |> AsyncSeq2.except (Gen.getSeqWithSideEffect variant)
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 12; 13; 99 |])

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-except removes everything`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.except (Gen.getSeqWithSideEffect variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-except removes everything with duplicates`` variant =
        asyncSeq2 {
            yield! Gen.getSeqWithSideEffect variant
            yield! Gen.getSeqWithSideEffect variant
            yield! Gen.getSeqWithSideEffect variant
            yield! Gen.getSeqWithSideEffect variant
        }
        |> AsyncSeq2.except (Gen.getSeqWithSideEffect variant)
        |> verifyEmpty
