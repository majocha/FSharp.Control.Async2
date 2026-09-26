module AsyncSeq2.Tests.Find

open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.find
// ColdTask.findAsync
// ColdTask.tryFind
// ColdTask.tryFindAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.find (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.findAsync (fun _ -> Task.fromResult false) null

        assertNullArg
        <| fun () -> ColdTask.tryFind (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.tryFindAsync (fun _ -> Task.fromResult false) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-find raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.find ((=) 12)
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-findAsync raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.findAsync (fun x -> task { return x = 12 })
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>


    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryFind returns None`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.tryFind ((=) 12)
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryFindAsync returns None`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.tryFindAsync (fun x -> task { return x = 12 })
        |> Task.map (should be None')

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-find sad path raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.find ((=) 0) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findAsync sad path raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.findAsync (fun x -> task { return x = 0 }) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-find happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.find (fun x -> x < 6 && x > 4)
        |> Task.map (should equal 5)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findAsync happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findAsync (fun x -> task { return x < 6 && x > 4 })
        |> Task.map (should equal 5)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-find happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.find ((=) 1)
        |> Task.map (should equal 1)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findAsync happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findAsync (fun x -> task { return x = 1 })
        |> Task.map (should equal 1)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-find happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.find ((=) 10)
        |> Task.map (should equal 10)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findAsync happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findAsync (fun x -> task { return x = 10 }) // dummy tasks seq ends at 50
        |> Task.map (should equal 10)


    //
    //
    // tryXXX stuff
    //      |
    //      |
    //      V

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFind sad path returns None`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFind ((=) 0)
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindAsync sad path return None`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindAsync (fun x -> task { return x = 0 })
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFind happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFind (fun x -> x < 6 && x > 4)
        |> Task.map (should equal (Some 5))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindAsync happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindAsync (fun x -> task { return x < 6 && x > 4 })
        |> Task.map (should equal (Some 5))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFind happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFind ((=) 1)
        |> Task.map (should equal (Some 1))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindAsync happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindAsync (fun x -> task { return x = 1 })
        |> Task.map (should equal (Some 1))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFind happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFind ((=) 10)
        |> Task.map (should equal (Some 10))

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindAsync happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindAsync (fun x -> task { return x = 10 })
        |> Task.map (should equal (Some 10))

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-find KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder = (=) 11

        // first: error, item is not there
        fun () -> ColdTask.find finder ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // find again: no error, because of side effects
        let! found = ColdTask.find finder ts
        found |> should equal 11

        // find once more: error, item is not there anymore.
        fun () -> ColdTask.find finder ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-findAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder x = task { return x = 11 }

        // first: error, item is not there
        fun () -> ColdTask.findAsync finder ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // find again: no error, because of side effects
        let! found = ColdTask.findAsync finder ts
        found |> should equal 11

        // find once more: error, item is not there anymore.
        fun () -> ColdTask.findAsync finder ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Fact>]
    let ``AsyncSeq2-find _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.find ((=) 3)
        found |> should equal 3
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.find ((=) 4)
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-findAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.findAsync (fun x -> task { return x = 3 })
        found |> should equal 3
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.findAsync (fun x -> task { return x = 4 })
        found |> should equal 4
        i |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-find _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.find ((=) 42)
        found |> should equal 42
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-findAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.findAsync (fun x -> task { return x = 42 })
        found |> should equal 42
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-find _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.find ((=) 0)
        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.find ((=) 4)
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-findAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.findAsync (fun x -> task { return x = 0 })
        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.findAsync (fun x -> task { return x = 4 })
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }


    //
    //
    // tryXXX stuff
    //      |
    //      |
    //      V

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryFind KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder = (=) 11

        // first: None
        let! found = ColdTask.tryFind finder ts
        found |> should be None'

        // find again: found now, because of side effects
        let! found = ColdTask.tryFind finder ts
        found |> should equal (Some 11)

        // find once more: None
        let! found = ColdTask.tryFind finder ts
        found |> should be None'
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryFindAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let finder x = task { return x = 11 }

        // first: None
        let! found = ColdTask.tryFindAsync finder ts
        found |> should be None'

        // find again: found now, because of side effects
        let! found = ColdTask.tryFindAsync finder ts
        found |> should equal (Some 11)

        // find once more: None
        let! found = ColdTask.tryFindAsync finder ts
        found |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-tryFind _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.tryFind ((=) 3)
        found |> should equal (Some 3)
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.tryFind ((=) 4)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.tryFindAsync (fun x -> task { return x = 3 })
        found |> should equal (Some 3)
        i |> should equal 3 // only partial evaluation!

        // find next item. We do get a new iterator, but mutable state is now starting at '3', so first item now returned is '4'.
        let! found = ts |> ColdTask.tryFindAsync (fun x -> task { return x = 4 })
        found |> should equal (Some 4)
        i |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-tryFind _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.tryFind ((=) 42)
        found |> should equal (Some 42)
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.tryFindAsync (fun x -> task { return x = 42 })
        found |> should equal (Some 42)
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryFind _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.tryFind ((=) 0)
        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.tryFind ((=) 4)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.tryFindAsync (fun x -> task { return x = 0 })
        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // find some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.tryFindAsync (fun x -> task { return x = 4 })
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }
