module AsyncSeq2.Tests.Length

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.length
// ColdTask.lengthOrMax
// ColdTask.lengthBy
// ColdTask.lengthByAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> AsyncSeq2.length null
        assertNullArg <| fun () -> ColdTask.lengthOrMax 10 null

        assertNullArg
        <| fun () -> ColdTask.lengthBy (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.lengthByAsync (fun _ -> Task.fromResult false) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-length returns zero on empty sequences`` variant = task {
        let! len = Gen.getEmptyVariant variant |> AsyncSeq2.length |> Async2.StartAsTask
        len |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-lengthBy returns zero on empty sequences`` variant = task {
        let! len =
            Gen.getEmptyVariant variant
            |> ColdTask.lengthBy (fun _ -> true)

        len |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-lengthByAsync returns zero on empty sequences`` variant = task {
        let! len =
            Gen.getEmptyVariant variant
            |> ColdTask.lengthByAsync (Task.apply (fun _ -> true))

        len |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-lengthOrMax on empty sequence returns 0 regardless of max`` variant = task {
        let! len = Gen.getEmptyVariant variant |> ColdTask.lengthOrMax 100
        len |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-lengthOrMax on empty sequence with max=0 returns 0`` variant = task {
        let! len = Gen.getEmptyVariant variant |> ColdTask.lengthOrMax 0
        len |> should equal 0
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-length returns proper length`` variant = task {
        let ts = Gen.getSeqImmutable variant
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 10)
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 10) // twice is fine
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthBy returns proper length`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let run () = ColdTask.lengthBy (fun _ -> true) ts
        do! run () |> Task.map (should equal 10)
        do! run () |> Task.map (should equal 10) // twice is fine
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthByAsync returns proper length`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let run () = ColdTask.lengthByAsync (Task.apply (fun _ -> true)) ts
        do! run () |> Task.map (should equal 10)
        do! run () |> Task.map (should equal 10) // twice is fine
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthBy returns proper length when filtering`` variant = task {
        let run f = Gen.getSeqImmutable variant |> ColdTask.lengthBy f
        do! run (fun x -> x % 3 = 0) |> Task.map (should equal 3) // [3; 6; 9]
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 4) // [1; 4; 7; 10]
        do! run (fun x -> x % 3 = 2) |> Task.map (should equal 3) // [2; 5; 8]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthByAsync returns proper length when filtering`` variant = task {
        let run f =
            Gen.getSeqImmutable variant
            |> ColdTask.lengthByAsync (Task.apply f)

        do! run (fun x -> x % 3 = 0) |> Task.map (should equal 3) // [3; 6; 9]
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 4) // [1; 4; 7; 10]
        do! run (fun x -> x % 3 = 2) |> Task.map (should equal 3) // [2; 5; 8]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax returns actual length when sequence is shorter than max`` variant = task {
        // source has 10 items; max=100 → actual length 10 is returned
        let! len = Gen.getSeqImmutable variant |> ColdTask.lengthOrMax 100
        len |> should equal 10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax returns max when sequence is longer than max`` variant = task {
        // source has 10 items; max=5 → capped at 5
        let! len = Gen.getSeqImmutable variant |> ColdTask.lengthOrMax 5
        len |> should equal 5
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax returns max when sequence is exactly max`` variant = task {
        // source has 10 items; max=10 → returns 10
        let! len = Gen.getSeqImmutable variant |> ColdTask.lengthOrMax 10
        len |> should equal 10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax with max=1 returns 1 for any non-empty sequence`` variant = task {
        let! len = Gen.getSeqImmutable variant |> ColdTask.lengthOrMax 1
        len |> should equal 1
    }

    [<Fact>]
    let ``AsyncSeq2-lengthOrMax with max=0 always returns 0 regardless of source`` () = task {
        // max=0: the while loop condition (i < max) is false from the start → 0 returned
        // NOTE: the implementation still calls MoveNextAsync once before the loop
        let! len = AsyncSeq2.ofList [ 1..100 ] |> ColdTask.lengthOrMax 0
        len |> should equal 0
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-length prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> AsyncSeq2.length |> Async2.StartAsTask |> Task.ignore
        do! ts |> AsyncSeq2.length |> Async2.StartAsTask |> Task.ignore
        do! ts |> AsyncSeq2.length |> Async2.StartAsTask |> Task.ignore
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-lengthBy prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.lengthBy (fun _ -> true) |> Task.ignore
        do! ts |> ColdTask.lengthBy (fun _ -> true) |> Task.ignore
        do! ts |> ColdTask.lengthBy (fun _ -> true) |> Task.ignore
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-lengthByAsync prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        let lenBy =
            ColdTask.lengthByAsync (fun _ -> task { return true })
            >> Task.ignore

        do! lenBy ts
        do! lenBy ts
        do! lenBy ts

        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-length with sequence that changes length`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 10)
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 20) // mutable state dangers!!
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 30) // id
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 40) // id
        do! AsyncSeq2.length ts |> Async2.StartAsTask |> Task.map (should equal 50) // id
    }

    [<Fact>]
    let ``AsyncSeq2-lengthBy with sequence that changes length`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        do! ColdTask.lengthBy ((<) 10) ts |> Task.map (should equal 0)
        do! ColdTask.lengthBy ((<) 20) ts |> Task.map (should equal 0) // mutable state dangers!!
        do! ColdTask.lengthBy ((<) 30) ts |> Task.map (should equal 0) // id
        do! ColdTask.lengthBy ((<) 10) ts |> Task.map (should equal 30) // id
        do! ColdTask.lengthBy ((<) 10) ts |> Task.map (should equal 40) // id
    }

    [<Fact>]
    let ``AsyncSeq2-lengthByAsync with sequence that changes length`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        let notBefore x = ColdTask.lengthByAsync (Task.apply ((<) x)) ts
        do! notBefore 10 |> Task.map (should equal 0)
        do! notBefore 20 |> Task.map (should equal 0) // mutable state dangers!!
        do! notBefore 30 |> Task.map (should equal 0) // id
        do! notBefore 10 |> Task.map (should equal 30) // id
        do! notBefore 10 |> Task.map (should equal 40) // id
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-length returns proper length`` variant = task {
        let! len = Gen.getSeqWithSideEffect variant |> AsyncSeq2.length |> Async2.StartAsTask
        len |> should equal 10
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthBy returns proper length`` variant = task {
        let! len =
            Gen.getSeqWithSideEffect variant
            |> ColdTask.lengthBy (fun _ -> true)

        len |> should equal 10
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthByAsync returns proper length`` variant = task {
        let! len =
            Gen.getSeqWithSideEffect variant
            |> ColdTask.lengthByAsync (Task.apply (fun _ -> true))

        len |> should equal 10
    }


    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthBy returns proper length when filtering`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let run f = ts |> ColdTask.lengthBy f

        do! run (fun x -> x % 3 = 0) |> Task.map (should equal 3) // [3; 6; 9]
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 3) // [13; 16; 19]  // because of side-effect run again!
        do! run (fun x -> x % 3 = 2) |> Task.map (should equal 3) // [23; 26; 29]  // id
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 4) // [31; 34; 37; 40]  // id
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthByAsync returns proper length when filtering - side-effect`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let run f = ts |> ColdTask.lengthByAsync (Task.apply f)

        do! run (fun x -> x % 3 = 0) |> Task.map (should equal 3) // [3; 6; 9]
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 3) // [13; 16; 19]  // because of side-effect run again!
        do! run (fun x -> x % 3 = 2) |> Task.map (should equal 3) // [23; 26; 29]  // id
        do! run (fun x -> x % 3 = 1) |> Task.map (should equal 4) // [31; 34; 37; 40]  // id
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax returns correct length when below max`` variant = task {
        // side-effect sequence yields 10 items on first run
        let! len = Gen.getSeqWithSideEffect variant |> ColdTask.lengthOrMax 100
        len |> should equal 10
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-lengthOrMax returns max and stops evaluation when sequence exceeds max`` variant = task {
        // source has 10 items; max=5 → should stop early after exactly 5 elements
        let mutable evaluated = 0

        let ts = asyncSeq2 {
            for item in Gen.getSeqWithSideEffect variant do
                evaluated <- evaluated + 1
                yield item
        }

        let! len = ts |> ColdTask.lengthOrMax 5
        len |> should equal 5
        // exactly max elements are pulled from the source
        evaluated |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-lengthOrMax stops evaluating source after reaching max`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            for i in 1..100 do
                sideEffects <- sideEffects + 1
                yield i
        }

        let! len = ts |> ColdTask.lengthOrMax 7
        len |> should equal 7
        // exactly max elements are evaluated
        sideEffects |> should equal 7
    }

    [<Fact>]
    let ``AsyncSeq2-lengthOrMax with max=0 evaluates zero elements`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            sideEffects <- sideEffects + 1
            yield 1
            sideEffects <- sideEffects + 1
            yield 2
        }

        let! len = ts |> ColdTask.lengthOrMax 0
        len |> should equal 0
        // no elements evaluated when max=0
        sideEffects |> should equal 0
    }
