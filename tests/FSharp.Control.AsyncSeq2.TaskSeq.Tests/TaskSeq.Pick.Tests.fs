module AsyncSeq2.Tests.Pick

open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation


//
// ColdTask.pick
// ColdTask.pickAsync
// ColdTask.tryPick
// ColdTask.tryPickAsync
//

let picker equalTo x = if x = equalTo then Some x else None
let pickerAsync equalTo x = task { return if x = equalTo then Some x else None }


module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.pick (picker 0) null
        assertNullArg <| fun () -> ColdTask.tryPick (picker 0) null

        assertNullArg
        <| fun () -> ColdTask.pickAsync (pickerAsync 0) null

        assertNullArg
        <| fun () -> ColdTask.tryPickAsync (pickerAsync 0) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-pick on an empty sequence raises KeyNotFoundException`` variant = task {
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.pick (picker 12)
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-pickAsync on an empty sequence raises KeyNotFoundException`` variant = task {
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.pickAsync (pickerAsync 12)
            |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryPick on an empty sequence returns None`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryPick (picker 12)

        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryPickAsync on an empty sequence returns None`` variant = task {
        let! nothing =
            Gen.getEmptyVariant variant
            |> ColdTask.tryPickAsync (pickerAsync 12)

        nothing |> should be None'
    }

module Immutable =

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pick sad path raises KeyNotFoundException`` variant = task {
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.pick (picker 0) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pickAsync sad path raises KeyNotFoundException`` variant = task {
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.pickAsync (fun x -> task { return if x < 0 then Some x else None }) // dummy tasks sequence starts at 1
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pick sad path raises KeyNotFoundException variant`` variant = task {
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.pick (picker 11) // dummy tasks sequence ends at 50
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pickAsync sad path raises KeyNotFoundException variant`` variant = task {
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.pickAsync (pickerAsync 11) // dummy tasks sequence ends at 50
            |> Task.ignore

        |> should throwAsyncExact typeof<KeyNotFoundException>
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pick happy path middle of seq`` variant = task {
        let! twentyFive =
            Gen.getSeqImmutable variant
            |> ColdTask.pick (fun x -> if x < 6 && x > 4 then Some "foo" else None)

        twentyFive |> should equal "foo"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pickAsync happy path middle of seq`` variant = task {
        let! twentyFive =
            Gen.getSeqImmutable variant
            |> ColdTask.pickAsync (fun x -> task { return if x < 6 && x > 4 then Some "foo" else None })

        twentyFive |> should equal "foo"
    }

    [<Fact>]
    let ``AsyncSeq2-pick happy path first item of seq`` () = task {
        let! first =
            Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 50
            |> ColdTask.pick (fun x -> if x = 1 then Some $"first{x}" else None) // dummy tasks seq starts at 1

        first |> should equal "first1"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pickAsync happy path first item of seq`` variant = task {
        let! first =
            Gen.getSeqImmutable variant
            |> ColdTask.pickAsync (fun x -> task { return if x = 1 then Some $"first{x}" else None }) // dummy tasks seq starts at 1

        first |> should equal "first1"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pick happy path last item of seq`` variant = task {
        let! last =
            Gen.getSeqImmutable variant
            |> ColdTask.pick (fun x -> if x = 10 then Some $"last{x}" else None) // dummy tasks seq ends at 50

        last |> should equal "last10"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-pickAsync happy path last item of seq`` variant = task {
        let! last =
            Gen.getSeqImmutable variant
            |> ColdTask.pickAsync (fun x -> task { return if x = 10 then Some $"last{x}" else None }) // dummy tasks seq ends at 50

        last |> should equal "last10"
    }

    //
    //
    // tryXXX stuff
    //      |
    //      |
    //      V


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPick sad path returns None`` variant = task {
        let! nothing = Gen.getSeqImmutable variant |> ColdTask.tryPick (picker 0) // dummy tasks sequence starts at 1

        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync sad path return None`` variant = task {
        let! nothing =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPickAsync (pickerAsync 0) // dummy tasks sequence starts at 1

        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPick sad path returns None variant`` variant = task {
        let! nothing =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPick (fun x -> if x >= 11 then Some x else None) // dummy tasks sequence ends at 50 (inverted sign in lambda!)

        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync sad path return None - variant`` variant = task {
        let! nothing =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPickAsync (fun x -> task { return if x >= 11 then Some x else None }) // dummy tasks sequence ends at 50

        nothing |> should be None'
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPick happy path middle of seq`` variant = task {
        let! twentyFive =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPick (fun x -> if x < 6 && x > 4 then Some $"foo{x}" else None)

        twentyFive |> should equal (Some "foo5")
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync happy path middle of seq`` variant = task {
        let! twentyFive =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPickAsync (fun x -> task { return if x < 6 && x > 4 then Some $"foo{x}" else None })

        twentyFive |> should equal (Some "foo5")
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPick happy path first item of seq`` variant = task {
        let! first =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPick (sprintf "foo%i" >> Some) // dummy tasks seq starts at 1

        first |> should equal (Some "foo1")
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync happy path first item of seq`` variant = task {
        let! first =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPickAsync (fun x -> task { return (sprintf "foo%i" >> Some) x }) // dummy tasks seq starts at 1

        first |> should equal (Some "foo1")
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPick happy path last item of seq`` variant = task {
        let! last =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPick (fun x -> if x = 10 then Some $"foo{x}" else None) // dummy tasks seq ends at 50

        last |> should equal (Some "foo10")
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync happy path last item of seq`` variant = task {
        let! last =
            Gen.getSeqImmutable variant
            |> ColdTask.tryPickAsync (fun x -> task { return if x = 10 then Some $"foo{x}" else None }) // dummy tasks seq ends at 50

        last |> should equal (Some "foo10")
    }


module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-pick KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        // first: error, item is not there
        fun () -> ColdTask.pick (picker 11) ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // pick again: no error, because of side effects
        let! found = ColdTask.pick (picker 11) ts
        found |> should equal 11

        // pick once more: error, item is not there anymore.
        fun () -> ColdTask.pick (picker 11) ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-pickAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        // first: error, item is not there
        fun () -> ColdTask.pickAsync (pickerAsync 11) ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>

        // pick again: no error, because of side effects
        let! found = ColdTask.pickAsync (pickerAsync 11) ts
        found |> should equal 11

        // pick once more: error, item is not there anymore.
        fun () -> ColdTask.pickAsync (pickerAsync 11) ts |> Task.ignore
        |> should throwAsyncExact typeof<KeyNotFoundException>
    }

    [<Fact>]
    let ``AsyncSeq2-pick _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.pick (picker 3)
        found |> should equal 3
        i |> should equal 3 // only partial evaluation!

        // pick next item. We do get a new iterator, but mutable state is now starting at '3'
        let! found = ts |> ColdTask.pick (picker 4)
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-pickAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.pickAsync (pickerAsync 3)
        found |> should equal 3
        i |> should equal 3 // only partial evaluation!

        // pick next item. We do get a new iterator, but mutable state is now starting at '3'
        let! found = ts |> ColdTask.pickAsync (pickerAsync 4)
        found |> should equal 4
        i |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-pick _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.pick (picker 42)
        found |> should equal 42
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-pickAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.pickAsync (pickerAsync 42)
        found |> should equal 42
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-pick _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.pick (picker 0)
        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // pick some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.pick (picker 4)
        found |> should equal 4
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-pickAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.pickAsync (pickerAsync 0)
        found |> should equal 0
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // pick some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.pickAsync (pickerAsync 4)
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
    let ``AsyncSeq2-tryPick KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let picker x = if x = 11 then Some x else None

        // first: None
        let! found = ColdTask.tryPick picker ts
        found |> should be None'

        // pick again: found now, because of side effects
        let! found = ColdTask.tryPick picker ts
        found |> should equal (Some 11)

        // pick once more: None
        let! found = ColdTask.tryPick picker ts
        found |> should be None'
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryPickAsync KeyNotFoundException only sometimes for mutated state`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let picker x = task { return if x = 11 then Some x else None }

        // first: None
        let! found = ColdTask.tryPickAsync picker ts
        found |> should be None'

        // pick again: found now, because of side effects
        let! found = ColdTask.tryPickAsync picker ts
        found |> should equal (Some 11)

        // pick once more: None
        let! found = ColdTask.tryPickAsync picker ts
        found |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-tryPick _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.tryPick (picker 3)
        found |> should equal (Some 3)
        i |> should equal 3 // only partial evaluation!

        // pick next item. We do get a new iterator, but mutable state is now starting at '3'
        let! found = ts |> ColdTask.tryPick (picker 4)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryPickAsync _specialcase_ prove we don't read past the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                i <- i + 1
                yield i
        }

        let! found = ts |> ColdTask.tryPickAsync (pickerAsync 3)
        found |> should equal (Some 3)
        i |> should equal 3 // only partial evaluation!

        // pick next item. We do get a new iterator, but mutable state is now starting at '3'
        let! found = ts |> ColdTask.tryPickAsync (pickerAsync 4)
        found |> should equal (Some 4)
        i |> should equal 4
    }

    [<Fact>]
    let ``AsyncSeq2-tryPick _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.tryPick (picker 42)
        found |> should equal (Some 42)
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryPickAsync _specialcase_ prove we don't read past the found item v2`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            yield 42
            i <- i + 1
            i <- i + 1
        }

        let! found = ts |> ColdTask.tryPickAsync (pickerAsync 42)
        found |> should equal (Some 42)
        i |> should equal 0 // because no MoveNext after found item, the last statements are not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryPick _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.tryPick (picker 0)
        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // pick some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.tryPick (picker 4)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }

    [<Fact>]
    let ``AsyncSeq2-tryPickAsync _specialcase_ prove statement after yield is not evaluated`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            for _ in 0..9 do
                yield i
                i <- i + 1
        }

        let! found = ts |> ColdTask.tryPickAsync (pickerAsync 0)
        found |> should equal (Some 0)
        i |> should equal 0 // notice that it should be one higher if the statement after 'yield' is evaluated

        // pick some next item. We do get a new iterator, but mutable state is now starting at '1'
        let! found = ts |> ColdTask.tryPickAsync (pickerAsync 4)
        found |> should equal (Some 4)
        i |> should equal 4 // only partial evaluation!
    }
