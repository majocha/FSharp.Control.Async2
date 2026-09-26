module AsyncSeq2.Tests.ZipWith

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.zipWith
// TaskCallbacks.zipWithAsync
// AsyncSeq2.zipWith3
// TaskCallbacks.zipWithAsync3
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid for zipWith`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.zipWith (+) null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.zipWith (+) (AsyncSeq2.empty ()) null

        assertNullArg
        <| fun () -> AsyncSeq2.zipWith (+) null (null: AsyncSeq2<int>)

    [<Fact>]
    let ``Null source is invalid for zipWithAsync`` () =
        assertNullArg
        <| fun () -> TaskCallbacks.zipWithAsync (fun a b -> Task.fromResult (a + b)) null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> TaskCallbacks.zipWithAsync (fun a b -> Task.fromResult (a + b)) (AsyncSeq2.empty ()) null

    [<Fact>]
    let ``Null source is invalid for zipWith3`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.zipWith3 (fun a b c -> a) null (AsyncSeq2.empty ()) (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.zipWith3 (fun a b c -> a) (AsyncSeq2.empty ()) null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> AsyncSeq2.zipWith3 (fun a b c -> a) (AsyncSeq2.empty ()) (AsyncSeq2.empty ()) null

    [<Fact>]
    let ``Null source is invalid for zipWithAsync3`` () =
        let f a b c = Task.fromResult (a + b + c)

        assertNullArg
        <| fun () -> TaskCallbacks.zipWithAsync3 f null (AsyncSeq2.empty ()) (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> TaskCallbacks.zipWithAsync3 f (AsyncSeq2.empty ()) null (AsyncSeq2.empty ())

        assertNullArg
        <| fun () -> TaskCallbacks.zipWithAsync3 f (AsyncSeq2.empty ()) (AsyncSeq2.empty ()) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zipWith with two empty gives empty`` variant =
        AsyncSeq2.zipWith (+) (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zipWith with one empty gives empty`` variant =
        AsyncSeq2.zipWith (+) (AsyncSeq2.empty<int> ()) (Gen.getEmptyVariant variant)
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-zipWith3 with empties gives empty`` variant =
        AsyncSeq2.zipWith3 (fun a b c -> a + b + c) (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant) (Gen.getEmptyVariant variant)
        |> verifyEmpty


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zipWith applies mapping correctly`` variant = task {
        let one = Gen.getSeqImmutable variant
        let two = Gen.getSeqImmutable variant
        let! result = AsyncSeq2.zipWith (+) one two |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> (i + 1) + (i + 1)))
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zipWithAsync applies async mapping correctly`` variant = task {
        let one = Gen.getSeqImmutable variant
        let two = Gen.getSeqImmutable variant

        let! result =
            TaskCallbacks.zipWithAsync (fun a b -> Task.fromResult (a * b)) one two
            |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> (i + 1) * (i + 1)))
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zipWith3 applies three-way mapping`` variant = task {
        let s1 = Gen.getSeqImmutable variant
        let s2 = Gen.getSeqImmutable variant
        let s3 = Gen.getSeqImmutable variant

        let! result =
            AsyncSeq2.zipWith3 (fun a b c -> a + b + c) s1 s2 s3
            |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> 3 * (i + 1)))
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-zipWithAsync3 applies async three-way mapping`` variant = task {
        let s1 = Gen.getSeqImmutable variant
        let s2 = Gen.getSeqImmutable variant
        let s3 = Gen.getSeqImmutable variant

        let! result =
            TaskCallbacks.zipWithAsync3 (fun a b c -> Task.fromResult (a + b + c)) s1 s2 s3
            |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> 3 * (i + 1)))
    }

    [<Fact>]
    let ``AsyncSeq2-zipWith truncates to shorter sequence`` () = task {
        let short = asyncSeq2 {
            yield 1
            yield 2
        }

        let long = asyncSeq2 { yield! [ 10..20 ] }
        let! result = AsyncSeq2.zipWith (+) short long |> ColdTask.toArrayAsync
        result |> should equal [| 11; 13 |]
    }

    [<Fact>]
    let ``AsyncSeq2-zipWith string concatenation`` () = task {
        let keys = asyncSeq2 {
            yield "a"
            yield "b"
            yield "c"
        }

        let values = asyncSeq2 {
            yield 1
            yield 2
            yield 3
        }

        let! result =
            AsyncSeq2.zipWith (fun k v -> sprintf "%s=%d" k v) keys values
            |> ColdTask.toArrayAsync

        result |> should equal [| "a=1"; "b=2"; "c=3" |]
    }

    [<Fact>]
    let ``AsyncSeq2-zipWith is equivalent to zip-then-map`` () = task {
        let s1 = asyncSeq2 { yield! [ 1..5 ] }
        let s2 = asyncSeq2 { yield! [ 10..14 ] }
        let! viaZipWith = AsyncSeq2.zipWith (+) s1 s2 |> ColdTask.toArrayAsync
        let s1b = asyncSeq2 { yield! [ 1..5 ] }
        let s2b = asyncSeq2 { yield! [ 10..14 ] }

        let! viaZipMap =
            AsyncSeq2.zip s1b s2b
            |> AsyncSeq2.map (fun (a, b) -> a + b)
            |> ColdTask.toArrayAsync

        viaZipWith |> should equal viaZipMap
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zipWith on two side-effect seqs combines elements correctly`` variant = task {
        let s1 = Gen.getSeqWithSideEffect variant
        let s2 = Gen.getSeqWithSideEffect variant

        // Both sequences yield 1..10 on first iteration; side effects increment independently
        let! result = AsyncSeq2.zipWith (+) s1 s2 |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> (i + 1) + (i + 1)))
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zipWithAsync on two side-effect seqs combines elements correctly`` variant = task {
        let s1 = Gen.getSeqWithSideEffect variant
        let s2 = Gen.getSeqWithSideEffect variant

        let! result =
            TaskCallbacks.zipWithAsync (fun a b -> Task.fromResult (a * b)) s1 s2
            |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> (i + 1) * (i + 1)))
    }

    [<Fact>]
    let ``AsyncSeq2-zipWith consumes both sequences one element at a time`` () = task {
        let mutable count1 = 0
        let mutable count2 = 0

        let s1 = asyncSeq2 {
            for i in 1..5 do
                count1 <- count1 + 1
                yield i
        }

        let s2 = asyncSeq2 {
            for i in 10..14 do
                count2 <- count2 + 1
                yield i
        }

        let! result = AsyncSeq2.zipWith (+) s1 s2 |> ColdTask.toArrayAsync
        result |> should equal [| 11; 13; 15; 17; 19 |]
        count1 |> should equal 5
        count2 |> should equal 5
    }

    [<Fact>]
    let ``AsyncSeq2-zipWith truncates at shorter side-effect seq, output is correct`` () = task {
        let mutable longCount = 0

        let short = asyncSeq2 { yield! [ 1; 2 ] }

        let long = asyncSeq2 {
            for i in 10..20 do
                longCount <- longCount + 1
                yield i
        }

        let! result = AsyncSeq2.zipWith (+) short long |> ColdTask.toArrayAsync
        result |> should equal [| 11; 13 |]
        // The implementation reads one element from each sequence to check for stop condition,
        // so the longer sequence is advanced one step beyond the last paired element.
        longCount |> should be (greaterThanOrEqualTo 2)
        longCount |> should be (lessThanOrEqualTo 3)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-zipWith3 on three side-effect seqs combines elements correctly`` variant = task {
        let s1 = Gen.getSeqWithSideEffect variant
        let s2 = Gen.getSeqWithSideEffect variant
        let s3 = Gen.getSeqWithSideEffect variant

        let! result =
            AsyncSeq2.zipWith3 (fun a b c -> a + b + c) s1 s2 s3
            |> ColdTask.toArrayAsync

        result
        |> should equal (Array.init 10 (fun i -> 3 * (i + 1)))
    }
