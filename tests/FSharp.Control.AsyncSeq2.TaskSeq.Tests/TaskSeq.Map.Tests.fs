module AsyncSeq2.Tests.Map

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

module private PortMap =
    let mapAsync (mapping: 'T -> System.Threading.Tasks.Task<'U>) (source: AsyncSeq2<'T>) =
        AsyncSeq2.mapAsync (fun item -> Async2.Await(mapping item)) source

//
// AsyncSeq2.map
// AsyncSeq2.mapi
// PortMap.mapAsync
// TaskCallbacks.mapiAsync
//

/// Asserts that a sequence contains the char values 'A'..'J'.
let validateSequence ts =
    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal "ABCDEFGHIJ")

/// Validates for "ABCDEFGHIJ" char sequence, or any amount of char-value higher
let validateSequenceWithOffset offset ts =
    let expected =
        [ 'A' .. 'J' ]
        |> List.map (int >> (+) offset >> char >> string)
        |> String.concat ""

    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal expected)

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.map (fun _ -> ()) null
        assertNullArg <| fun () -> AsyncSeq2.mapi (fun _ _ -> ()) null

        assertNullArg
        <| fun () -> PortMap.mapAsync (fun _ -> Task.fromResult ()) null

        assertNullArg
        <| fun () -> TaskCallbacks.mapiAsync (fun _ _ -> Task.fromResult ()) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-map empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.map (fun item -> char (item + 64))
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-mapi empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.mapi (fun i _ -> char (i + 65))
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-mapAsync empty`` variant =
        Gen.getEmptyVariant variant
        |> PortMap.mapAsync (fun item -> task { return char (item + 64) })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-mapiAsync empty`` variant =
        Gen.getEmptyVariant variant
        |> TaskCallbacks.mapiAsync (fun i _ -> task { return char (i + 65) })
        |> verifyEmpty


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-map maps in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.map (fun item -> char (item + 64))
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-mapi maps in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.mapi (fun i _ -> char (i + 65))
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-mapAsync maps in correct order`` variant =
        Gen.getSeqImmutable variant
        |> PortMap.mapAsync (fun item -> task { return char (item + 64) })
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-mapiAsync maps in correct order`` variant =
        Gen.getSeqImmutable variant
        |> TaskCallbacks.mapiAsync (fun i _ -> task { return char (i + 65) })
        |> validateSequence

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-map prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield 42
            i <- i + 1
        }

        // point of this test: just calling 'map' won't execute anything of the sequence!
        let _ =
            ts
            |> AsyncSeq2.map (fun x -> x + 10)
            |> AsyncSeq2.map (fun x -> x + 10)
            |> AsyncSeq2.map (fun x -> x + 10)

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-mapi prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield 42
            i <- i + 1
        }

        // point of this test: just calling 'map' won't execute anything of the sequence!
        let _ =
            ts
            |> AsyncSeq2.mapi (fun x _ -> x + 10)
            |> AsyncSeq2.mapi (fun x _ -> x + 10)
            |> AsyncSeq2.mapi (fun x _ -> x + 10)

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-mapAsync prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield 42
            i <- i + 1
        }

        // point of this test: just calling 'map' won't execute anything of the sequence!
        let _ =
            ts
            |> PortMap.mapAsync (fun x -> task { return x + 10 })
            |> PortMap.mapAsync (fun x -> task { return x + 10 })
            |> PortMap.mapAsync (fun x -> task { return x + 10 })

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-mapiAsync prove that it has no effect until executed`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1 // we should not get here
            i <- i + 1
            yield 42
            i <- i + 1
        }

        // point of this test: just calling 'map' won't execute anything of the sequence!
        let _ =
            ts
            |> TaskCallbacks.mapiAsync (fun x _ -> task { return x + 10 })
            |> TaskCallbacks.mapiAsync (fun x _ -> task { return x + 10 })
            |> TaskCallbacks.mapiAsync (fun x _ -> task { return x + 10 })

        // multiple maps have no effect unless executed
        i |> should equal 0


    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-map can access mutables that are mutated in correct order`` variant =
        let mutable sum = 0

        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.map (fun _ ->
            sum <- sum + 1
            char (sum + 64))
        |> validateSequence
        |> Task.map (fun () -> sum |> should equal 10)

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapi can access mutables which are mutated in correct order`` variant =
        let mutable sum = 0

        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.mapi (fun i _ ->
            sum <- i + 1
            char (sum + 64))
        |> validateSequence
        |> Task.map (fun () -> sum |> should equal 10)

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapAsync can map the same sequence multiple times`` variant = task {
        let doMap = PortMap.mapAsync (fun item -> task { return char (item + 64) })
        let ts = Gen.getSeqWithSideEffect variant

        // each time we do GetAsyncEnumerator(), and go through the whole sequence,
        // the whole sequence gets re-evaluated, causing our +1 side-effect to run again.
        do! doMap ts |> validateSequence
        do! doMap ts |> validateSequenceWithOffset 10 // the mutable is 10 higher
        do! doMap ts |> validateSequenceWithOffset 20 // again
        do! doMap ts |> validateSequenceWithOffset 30 // again
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapAsync can access mutables which are mutated in correct order`` variant =
        let mutable sum = 0

        Gen.getSeqWithSideEffect variant
        |> PortMap.mapAsync (fun _ -> task {
            sum <- sum + 1
            return char (sum + 64)
        })
        |> validateSequence
        |> Task.map (fun () -> sum |> should equal 10)

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-mapiAsync can access mutables which are mutated in correct order`` variant =
        let mutable data = '0'

        Gen.getSeqWithSideEffect variant
        |> TaskCallbacks.mapiAsync (fun i _ -> task {
            data <- char (i + 65)
            return data
        })
        |> validateSequence
        |> Task.map (fun () -> data |> should equal (char 74))
