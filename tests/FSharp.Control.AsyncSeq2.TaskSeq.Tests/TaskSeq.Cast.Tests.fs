module AsyncSeq2.Tests.Cast

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.box
// AsyncSeq2.unbox
// AsyncSeq2.cast
//

/// Asserts that a sequence contains the char values 'A'..'J'.
let validateSequence ts =
    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal "12345678910")

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.box null
        assertNullArg <| fun () -> AsyncSeq2.unbox null
        assertNullArg <| fun () -> AsyncSeq2.cast null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-box empty`` variant = Gen.getEmptyVariant variant |> AsyncSeq2.box |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-unbox empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.unbox<int>
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-cast empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<int>
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-unbox empty to invalid type should not fail`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.unbox<Guid> // cannot cast to int, but for empty sequences, the exception won't be thrown
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-cast empty to invalid type should not fail`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<string> // cannot cast to int, but for empty sequences, the exception won't be thrown
        |> verifyEmpty

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-box`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.box
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-unbox`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.box
        |> AsyncSeq2.unbox<int>
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-cast`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<int>
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-unbox invalid type should throw`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.box
            |> AsyncSeq2.unbox<uint> // cannot unbox from int to uint, even though types have the same size
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<InvalidCastException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-cast invalid type should throw`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.box
            |> AsyncSeq2.cast<string>
            |> ColdTask.toArrayAsync
            |> Task.ignore

        |> should throwAsyncExact typeof<InvalidCastException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-unbox invalid type should NOT throw before sequence is iterated`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.box
            |> AsyncSeq2.unbox<uint> // no iteration done
            |> ignore

        |> should not' (throw typeof<Exception>)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-cast invalid type should NOT throw before sequence is iterated`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> AsyncSeq2.box
            |> AsyncSeq2.cast<string> // no iteration done
            |> ignore

        |> should not' (throw typeof<Exception>)

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-box prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield 42
            i <- i + 1
        }

        // point of this test: just calling 'box' won't execute anything of the sequence!
        let boxed = ts |> AsyncSeq2.box |> AsyncSeq2.box |> AsyncSeq2.box

        // no side effect until iterated
        i |> should equal 0

        boxed
        |> ColdTask.last
        |> Task.map (should equal 42)
        |> Task.map (fun () -> i = 9)

    [<Fact>]
    let ``AsyncSeq2-unbox prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield box 42
            i <- i + 1
        }

        // point of this test: just calling 'unbox' won't execute anything of the sequence!
        let unboxed = ts |> AsyncSeq2.unbox

        // no side effect until iterated
        i |> should equal 0

        unboxed
        |> ColdTask.last
        |> Task.map (should equal 42)
        |> Task.map (fun () -> i = 3)

    [<Fact>]
    let ``AsyncSeq2-cast prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield box 42
            i <- i + 1
        }

        // point of this test: just calling 'cast' won't execute anything of the sequence!
        let cast = ts |> AsyncSeq2.cast<int>
        i |> should equal 0 // no side effect until iterated

        cast
        |> ColdTask.last
        |> Task.map (should equal 42)
        |> Task.map (fun () -> i = 3)
