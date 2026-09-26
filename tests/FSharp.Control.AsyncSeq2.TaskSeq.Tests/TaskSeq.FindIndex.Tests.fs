module AsyncSeq2.Tests.FindIndex

open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.findIndex
// ColdTask.findIndexAsync
// ColdTask.tryFindIndex
// ColdTask.tryFindIndexAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.findIndex (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.findIndexAsync (fun _ -> Task.fromResult false) null

        assertNullArg
        <| fun () -> ColdTask.tryFindIndex (fun _ -> false) null

        assertNullArg
        <| fun () -> ColdTask.tryFindIndexAsync (fun _ -> Task.fromResult false) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-findIndex raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.findIndex ((=) 12)
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-findIndexAsync raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.findIndexAsync (fun x -> task { return x = 12 })
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>


    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryFindIndex returns None`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.tryFindIndex ((=) 12)
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryFindIndexAsync returns None`` variant =
        Gen.getEmptyVariant variant
        |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 12 })
        |> Task.map (should be None')

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndex sad path raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.findIndex ((=) 0) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndexAsync sad path raises KeyNotFoundException`` variant =
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.findIndexAsync (fun x -> task { return x = 0 }) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndex happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndex (fun x -> x < 6 && x > 4)
        |> Task.map (should equal 4) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndexAsync happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndexAsync (fun x -> task { return x < 6 && x > 4 })
        |> Task.map (should equal 4)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndex happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndex ((=) 1)
        |> Task.map (should equal 0) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndexAsync happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndexAsync (fun x -> task { return x = 1 })
        |> Task.map (should equal 0) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndex happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndex ((=) 10)
        |> Task.map (should equal 9) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-findIndexAsync happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.findIndexAsync (fun x -> task { return x = 10 }) // dummy tasks seq ends at 50
        |> Task.map (should equal 9) // zero based


    //
    //
    // tryXXX stuff
    //      |
    //      |
    //      V

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndex sad path returns None`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndex ((=) 0)
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndexAsync sad path return None`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 0 })
        |> Task.map (should be None')

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndex happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndex (fun x -> x < 6 && x > 4)
        |> Task.map (should equal (Some 4)) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndexAsync happy path middle of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndexAsync (fun x -> task { return x < 6 && x > 4 })
        |> Task.map (should equal (Some 4)) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndex happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndex ((=) 1)
        |> Task.map (should equal (Some 0)) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndexAsync happy path first item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 1 })
        |> Task.map (should equal (Some 0)) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndex happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndex ((=) 10)
        |> Task.map (should equal (Some 9)) // zero based

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndexAsync happy path last item of seq`` variant =
        Gen.getSeqImmutable variant
        |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 10 })
        |> Task.map (should equal (Some 9)) // zero based

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-findIndex KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let findIndexer = (=) 11

        // first: error, item is not there
        fun () -> ColdTask.findIndex findIndexer ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // findIndex again: no error, because of side effects
        let! found = ColdTask.findIndex findIndexer ts
        found |> should equal 0 // zero based, first item in 'updated' sequence is 11

        // findIndex once more: error, item is not there anymore.
        fun () -> ColdTask.findIndex findIndexer ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-findIndexAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let findIndexer x = task { return x = 11 }

        // first: error, item is not there
        fun () -> ColdTask.findIndexAsync findIndexer ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // findIndex again: no error, because of side effects
        let! found = ColdTask.findIndexAsync findIndexer ts
        found |> should equal 0 // zero based, first item in 'updated' sequence is 11

        // findIndex once more: error, item is not there anymore.
        fun () -> ColdTask.findIndexAsync findIndexer ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Fact>]
    let ``AsyncSeq2-findIndex _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                i <- i + 1
                yield x
        }

        let! found = ts |> ColdTask.findIndex ((=) 13)
        found |> should equal 3
        i |> should equal 4 // only partial evaluation!

        // findIndex next item. We do get a new iterator, but mutable state is now starting at '4'
        let! found = ts |> ColdTask.findIndex ((=) 14)
        found |> should equal 4
        i |> should equal 9 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-findIndexAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                i <- i + 1
                yield x
        }

        let! found = ColdTask.findIndexAsync (fun x -> task { return x = 13 }) ts
        found |> should equal 3
        i |> should equal 4 // only partial evaluation!

        // findIndex next item. We do get a new iterator, but mutable state is now starting at '4'
        let! found = ColdTask.findIndexAsync (fun x -> task { return x = 14 }) ts
        found |> should equal 4
        i |> should equal 9 // started counting again
    }

    [<Fact>]
    let ``AsyncSeq2-findIndex _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.findIndex ((=) 42)
        found |> should equal 0 // first item has index 0
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-findIndexAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ColdTask.findIndexAsync (fun x -> task { return x = 42 }) ts
        found |> should equal 0 // first item has index 0
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-findIndex _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                yield x
                i <- i + 1
        }

        let! found = ts |> ColdTask.findIndex ((=) 10)
        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // findIndex some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.findIndex ((=) 14)
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-findIndexAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                yield x
                i <- i + 1
        }

        let! found =
            ts
            |> ColdTask.findIndexAsync (fun x -> task { return x = 10 })

        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // findIndex some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found =
            ts
            |> ColdTask.findIndexAsync (fun x -> task { return x = 14 })

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
    let ``AsyncSeq2-tryFindIndex KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let findIndexer = (=) 11

        // first: None
        let! found = ColdTask.tryFindIndex findIndexer ts
        found |> should be None'

        // findIndex again: found now, because of side effects
        let! found = ColdTask.tryFindIndex findIndexer ts
        found |> should equal (Some 0) // item with value '11' is at index 0 in 'updated' sequence

        // findIndex once more: None
        let! found = ColdTask.tryFindIndex findIndexer ts
        found |> should be None'
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryFindIndexAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let findIndexer x = task { return x = 11 }

        // first: None
        let! found = ColdTask.tryFindIndexAsync findIndexer ts
        found |> should be None'

        // findIndex again: found now, because of side effects
        let! found = ColdTask.tryFindIndexAsync findIndexer ts
        found |> should equal (Some 0) // item with value '11' is at index 0 in 'updated' sequence

        // findIndex once more: None
        let! found = ColdTask.tryFindIndexAsync findIndexer ts
        found |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndex _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                i <- i + 1
                yield x
        }

        let! found = ts |> ColdTask.tryFindIndex ((=) 13)
        found |> should equal (Some 3)
        i |> should equal 4 // only partial evaluation!

        // findIndex next item. We do get a new iterator, but mutable state is now starting at '4'
        let! found = ts |> ColdTask.tryFindIndex ((=) 14)
        found |> should equal (Some 4)
        i |> should equal 9 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndexAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                i <- i + 1
                yield x
        }

        let! found = ColdTask.tryFindIndexAsync (fun x -> task { return x = 13 }) ts

        found |> should equal (Some 3)
        i |> should equal 4 // only partial evaluation!

        // findIndex next item. We do get a new iterator, but mutable state is now starting at '4'
        let! found = ColdTask.tryFindIndexAsync (fun x -> task { return x = 14 }) ts

        found |> should equal (Some 4)
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndex _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.tryFindIndex ((=) 42)
        found |> should equal (Some 0) // first item has index 0
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndexAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found =
            ts
            |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 42 })

        found |> should equal (Some 0) // first item: idx 0
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndex _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                yield x
                i <- i + 1
        }

        let! found = ts |> ColdTask.tryFindIndex ((=) 10)
        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // findIndex some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.tryFindIndex ((=) 14)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryFindIndexAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for x in 10..19 do
                yield x
                i <- i + 1
        }

        let! found =
            ts
            |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 10 })

        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // findIndex some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found =
            ts
            |> ColdTask.tryFindIndexAsync (fun x -> task { return x = 14 })

        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }
