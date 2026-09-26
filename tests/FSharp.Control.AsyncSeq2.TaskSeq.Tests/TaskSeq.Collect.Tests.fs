module AsyncSeq2.Tests.Collect

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.collect
// AsyncSeq2.collectAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.collect (fun _ -> (AsyncSeq2.empty ())) null

        assertNullArg
        <| fun () -> AsyncSeq2.collectAsync (fun _ -> async2 { return AsyncSeq2.empty () }) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collect collecting emptiness`` variant =
        Gen.sideEffectTaskSeq 10
        |> AsyncSeq2.collect (fun _ -> Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collect collecting emptiness v2`` variant =
        Gen.sideEffectTaskSeq variant
        |> AsyncSeq2.collect (fun _ -> Gen.getEmptyVariant EmptyVariant.YieldBang)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collect collecting emptiness from emptiness`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collect (fun _ -> Gen.getEmptyVariant variant)
        |> verifyEmpty


    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collect collecting non-empty sequences on an empty sequence`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collect (fun _ -> asyncSeq2 { yield 10 })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectAsync collecting emptiness`` variant =
        Gen.sideEffectTaskSeq 10
        |> AsyncSeq2.collectAsync (fun _ -> async2 { return Gen.getEmptyVariant variant })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectAsync collecting emptiness v2`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectAsync (fun _ -> async2 { return Gen.getEmptyVariant EmptyVariant.DelayDoBang })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectAsync collecting emptiness from emptiness`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectAsync (fun _ -> async2 { return Gen.getEmptyVariant variant })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectAsync collecting non-empty sequences on an empty sequence`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectAsync (fun _ -> async2 { return asyncSeq2 { yield 10 } })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-collectSeq collecting emptiness`` variant =
        Gen.getSeqWithSideEffect variant
        |> AsyncSeq2.collectSeq (fun _ -> Seq.empty<int>)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectSeq collecting emptiness from emptiness`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectSeq (fun _ -> Seq.empty<int>)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectSeq collecting non-empty sequences on an empty sequence`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectSeq (fun _ -> seq { yield 10 })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectSeqAsync collecting emptiness`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return Array.empty<int> })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectSeqAsync collecting emptiness from emptiness`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return Array.empty<int> })
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-collectSeqAsync collecting non-empty sequences on an empty sequence`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return [ 10 ] })
        |> verifyEmpty

module Immutable =

    let validateSequence ts =
        ts
        |> ColdTask.toListAsync
        |> Task.map (List.map string)
        |> Task.map (String.concat "")
        |> Task.map (should equal "ABBCCDDEEFFGGHHIIJJK")

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collect operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collect (fun item -> asyncSeq2 {
            yield char (item + 64)
            yield char (item + 65)
        })
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectAsync operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectAsync (fun item -> async2 {
            return asyncSeq2 {
                yield char (item + 64)
                yield char (item + 65)
            }
        })
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectSeq operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectSeq (fun item -> seq {
            yield char (item + 64)
            yield char (item + 65)
        })
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectSeq with arrays operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectSeq (fun item -> [| char (item + 64); char (item + 65) |])
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectSeqAsync operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectSeqAsync (fun item -> async2 {
            return seq {
                yield char (item + 64)
                yield char (item + 65)
            }
        })
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-collectSeqAsync with arrays operates in correct order`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collectSeqAsync (fun item -> async2 { return [| char (item + 64); char (item + 65) |] })
        |> validateSequence

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-collect prove that it has no effect until executed`` () =
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
            |> AsyncSeq2.collect (fun _ -> asyncSeq2 { yield 10 })
            |> AsyncSeq2.collect (fun _ -> asyncSeq2 { yield 10 })
            |> AsyncSeq2.collect (fun _ -> asyncSeq2 { yield 10 })

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-collectAsync prove that it has no effect until executed`` () =
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
            |> AsyncSeq2.collectAsync (fun _ -> async2 { return asyncSeq2 { yield 10 } })
            |> AsyncSeq2.collectAsync (fun _ -> async2 { return asyncSeq2 { yield 10 } })
            |> AsyncSeq2.collectAsync (fun _ -> async2 { return asyncSeq2 { yield 10 } })

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-collectSeq prove that it has no effect until executed`` () =
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
            |> AsyncSeq2.collectSeq (fun _ -> seq { yield 10 })
            |> AsyncSeq2.collectSeq (fun _ -> seq { yield 10 })
            |> AsyncSeq2.collectSeq (fun _ -> seq { yield 10 })

        // multiple maps have no effect unless executed
        i |> should equal 0

    [<Fact>]
    let ``AsyncSeq2-collectSeqAsync prove that it has no effect until executed`` () =
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
            |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return seq { yield 10 } })
            |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return seq { yield 10 } })
            |> AsyncSeq2.collectSeqAsync (fun _ -> async2 { return seq { yield 10 } })

        // multiple maps have no effect unless executed
        i |> should equal 0
