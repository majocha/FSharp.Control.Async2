module AsyncSeq2.Tests.Append

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.append
// AsyncSeq2.appendSeq
// AsyncSeq2.prependSeq
//

let validateSequence ts =
    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal "1234567891012345678910")


module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> (AsyncSeq2.empty ()) |> AsyncSeq2.append null

        assertNullArg
        <| fun () -> null |> AsyncSeq2.append ((AsyncSeq2.empty ()))

        assertNullArg <| fun () -> null |> AsyncSeq2.append null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-append both args empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.append (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-appendSeq both args empty`` variant =
        Seq.empty
        |> AsyncSeq2.appendSeq (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-prependSeq both args empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.prependSeq Seq.empty
        |> verifyEmpty

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-append`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.append (Gen.getSeqImmutable variant)
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-appendSeq with a list`` variant =
        [ 1..10 ]
        |> AsyncSeq2.appendSeq (Gen.getSeqImmutable variant)
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-appendSeq with an array`` variant =
        [| 1..10 |]
        |> AsyncSeq2.appendSeq (Gen.getSeqImmutable variant)
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-prependSeq with a list`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.prependSeq [ 1..10 ]
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-prependSeq with an array`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.prependSeq [| 1..10 |]
        |> validateSequence

module SideEffects =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-append consumes whole sequence once incl after-effects`` variant =
        let mutable i = 0

        asyncSeq2 {
            i <- i + 1
            yield! [ 1..10 ]
            i <- i + 1
        }
        |> AsyncSeq2.append (Gen.getSeqImmutable variant)
        |> validateSequence
        |> Task.map (fun () -> i |> should equal 2)

    [<Fact>]
    let ``AsyncSeq2-appendSeq consumes whole sequence once incl after-effects`` () =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield! [ 1..10 ]
            i <- i + 1
        }

        [| 1..10 |]
        |> AsyncSeq2.appendSeq ts
        |> validateSequence
        |> Task.map (fun () -> i |> should equal 2)

    [<Fact>]
    let ``AsyncSeq2-prependSeq consumes whole sequence once incl after-effects`` () =
        let mutable i = 0

        asyncSeq2 {
            i <- i + 1
            yield! [ 1..10 ]
            i <- i + 1
        }
        |> AsyncSeq2.prependSeq [ 1..10 ]
        |> validateSequence
        |> Task.map (fun () -> i |> should equal 2)
