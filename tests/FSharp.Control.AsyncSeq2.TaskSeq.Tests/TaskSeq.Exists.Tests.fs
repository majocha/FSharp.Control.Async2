module AsyncSeq2.Tests.Exists

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.exists
// AsyncSeq2.existsAsyncc
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.exists (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.existsAsync (fun _ -> Task.fromResult false) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-exists returns false`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.exists ((=) 12)
        |> Task.map (should be False)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-existsAsync returns false`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.existsAsync (fun x -> task { return x = 12 })
        |> Task.map (should be False)

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exists sad path returns false`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.exists ((=) 0)
        |> Task.map (should be False)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-existsAsync sad path return false`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.existsAsync (fun x -> task { return x = 0 })
        |> Task.map (should be False)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exists happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.exists (fun x -> x < 6 && x > 4)
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-existsAsync happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.existsAsync (fun x -> task { return x < 6 && x > 4 })
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exists happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.exists ((=) 1)
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-existsAsync happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.existsAsync (fun x -> task { return x = 1 })
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-exists happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.exists ((=) 10)
        |> Task.map (should be True)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-existsAsync happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.existsAsync (fun x -> task { return x = 10 })
        |> Task.map (should be True)

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-exists success only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder = (=) 11

        // first: false
        let! found = ColdTask.exists finder ts
        found |> should be False

        // find again: found now, because of side effects
        let! found = ColdTask.exists finder ts
        found |> should be True

        // find once more: false
        let! found = ColdTask.exists finder ts
        found |> should be False
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-existsAsync success only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder x = task { return x = 11 }

        // first: false
        let! found = ColdTask.existsAsync finder ts
        found |> should be False

        // find again: found now, because of side effects
        let! found = ColdTask.existsAsync finder ts
        found |> should be True

        // find once more: false
        let! found = ColdTask.existsAsync finder ts
        found |> should be False
    }

    [<Fact>]
    let ``AsyncSeq2-exists _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.exists ((=) 3)
        found |> should be True
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.exists ((=) 4)
        found |> should be True
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-existsAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.existsAsync (fun x -> task { return x = 3 })
        found |> should be True
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.existsAsync (fun x -> task { return x = 4 })
        found |> should be True
        i |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-exists _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.exists ((=) 42)
        found |> should be True
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-existsAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.existsAsync (fun x -> task { return x = 42 })
        found |> should be True
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-exists _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.exists ((=) 0)
        found |> should be True
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now still starting at '0'
        let! found = ts |> ColdTask.exists ((=) 4)
        found |> should be True
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-existsAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.existsAsync (fun x -> task { return x = 0 })
        found |> should be True
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now still starting at '0'
        let! found = ts |> ColdTask.existsAsync (fun x -> task { return x = 4 })
        found |> should be True
        i |> should equal 4 // only partial evaluation!
    }
