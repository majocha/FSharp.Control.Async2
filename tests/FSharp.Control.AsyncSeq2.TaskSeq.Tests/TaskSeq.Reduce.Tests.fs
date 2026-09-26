module AsyncSeq2.Tests.Reduce

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.reduce
// ColdTask.reduceAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> ColdTask.reduce (fun a _ -> a) null

        assertNullArg
        <| fun () -> ColdTask.reduceAsync (fun a _ -> Task.fromResult a) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-reduce raises on empty`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.reduce (fun a b -> a + b)
            |> Task.ignore

        |> should throwAsyncExact typeof<System.ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-reduceAsync raises on empty`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.reduceAsync (fun a b -> task { return a + b })
            |> Task.ignore

        |> should throwAsyncExact typeof<System.ArgumentException>

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-reduce folds from first element`` variant = task {
        // items are 1..10; sum = 55
        let! sum =
            Gen.getSeqImmutable variant
            |> ColdTask.reduce (fun acc item -> acc + item)

        sum |> should equal 55
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-reduceAsync folds from first element`` variant = task {
        let! sum =
            Gen.getSeqImmutable variant
            |> ColdTask.reduceAsync (fun acc item -> task { return acc + item })

        sum |> should equal 55
    }

    [<Fact>]
    let ``AsyncSeq2-reduce returns single element without calling folder`` () = task {
        let mutable called = false

        let! result =
            AsyncSeq2.singleton 42
            |> ColdTask.reduce (fun _ _ ->
                called <- true
                failwith "should not be called")

        result |> should equal 42
        called |> should equal false
    }

    [<Fact>]
    let ``AsyncSeq2-reduceAsync returns single element without calling folder`` () = task {
        let mutable called = false

        let! result =
            AsyncSeq2.singleton 42
            |> ColdTask.reduceAsync (fun _ _ -> task {
                called <- true
                return failwith "should not be called"
            })

        result |> should equal 42
        called |> should equal false
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-reduce uses first element as initial accumulator`` variant = task {
        // reduce must use element[0] as initial state; for 1..10 summing gives 55
        // if it used 0 as initial, sum would also be 55 — but we verify the folder is called n-1 times
        let mutable callCount = 0

        let! sum =
            Gen.getSeqImmutable variant
            |> ColdTask.reduce (fun acc item ->
                callCount <- callCount + 1
                acc + item)

        sum |> should equal 55
        callCount |> should equal 9 // 10 elements => 9 reduce calls
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-reduce can concatenate strings`` variant = task {
        // items 1..10 as chars: ABCDEFGHIJ
        let! letters =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.map (fun i -> string (char (i + 64)))
            |> ColdTask.reduce (fun acc item -> acc + item)

        letters |> should equal "ABCDEFGHIJ"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-reduceAsync can concatenate strings`` variant = task {
        let! letters =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.map (fun i -> string (char (i + 64)))
            |> ColdTask.reduceAsync (fun acc item -> task { return acc + item })

        letters |> should equal "ABCDEFGHIJ"
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-reduce folds correctly with side-effecting sequences`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! sum = ts |> ColdTask.reduce (fun acc item -> acc + item)

        sum |> should equal 55

        // second enumeration produces next 10 elements: 11..20, sum = 155
        let! sum2 = ts |> ColdTask.reduce (fun acc item -> acc + item)

        sum2 |> should equal 155
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-reduceAsync folds correctly with side-effecting sequences`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let! sum =
            ts
            |> ColdTask.reduceAsync (fun acc item -> task { return acc + item })

        sum |> should equal 55

        let! sum2 =
            ts
            |> ColdTask.reduceAsync (fun acc item -> task { return acc + item })

        sum2 |> should equal 155
    }
