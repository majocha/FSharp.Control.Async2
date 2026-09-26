module AsyncSeq2.Tests.SumBy

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

module private PortSum =
    let inline sum source = AsyncSeq2.sum source |> Async2.StartAsTask
    let inline sumBy projection source = AsyncSeq2.sumBy projection source |> Async2.StartAsTask
    let inline sumByAsync (projection: 'T -> System.Threading.Tasks.Task< ^U >) (source: AsyncSeq2<'T>) =
        AsyncSeq2.sumByAsync (fun value -> Async2.Await(projection value)) source
        |> Async2.StartAsTask
    let inline average source = AsyncSeq2.average source |> Async2.StartAsTask
    let inline averageBy projection source = AsyncSeq2.averageBy projection source |> Async2.StartAsTask
    let inline averageByAsync (projection: 'T -> System.Threading.Tasks.Task< ^U >) (source: AsyncSeq2<'T>) =
        AsyncSeq2.averageByAsync (fun value -> Async2.Await(projection value)) source
        |> Async2.StartAsTask

//
// PortSum.sum
// PortSum.sumBy
// PortSum.sumByAsync
// PortSum.average
// PortSum.averageBy
// PortSum.averageByAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid for sum`` () =
        assertNullArg
        <| fun () -> PortSum.sum (null: System.Collections.Generic.IAsyncEnumerable<int>)

    [<Fact>]
    let ``Null source is invalid for sumBy`` () =
        assertNullArg
        <| fun () -> PortSum.sumBy id (null: System.Collections.Generic.IAsyncEnumerable<int>)

    [<Fact>]
    let ``Null source is invalid for sumByAsync`` () =
        assertNullArg
        <| fun () -> PortSum.sumByAsync (id >> Task.fromResult) (null: System.Collections.Generic.IAsyncEnumerable<int>)

    [<Fact>]
    let ``Null source is invalid for average`` () =
        assertNullArg
        <| fun () -> PortSum.average (null: System.Collections.Generic.IAsyncEnumerable<float>)

    [<Fact>]
    let ``Null source is invalid for averageBy`` () =
        assertNullArg
        <| fun () -> PortSum.averageBy float (null: System.Collections.Generic.IAsyncEnumerable<int>)

    [<Fact>]
    let ``Null source is invalid for averageByAsync`` () =
        assertNullArg
        <| fun () -> PortSum.averageByAsync (float >> Task.fromResult) (null: System.Collections.Generic.IAsyncEnumerable<int>)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-sum returns zero on empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> PortSum.sum
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-sumBy returns zero on empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> PortSum.sumBy id
        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-sumByAsync returns zero on empty`` variant = task {
        let! result =
            Gen.getEmptyVariant variant
            |> PortSum.sumByAsync Task.fromResult

        result |> should equal 0
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-average raises on empty`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> AsyncSeq2.map float
            |> PortSum.average
            |> Task.ignore

        |> should throwAsyncExact typeof<System.ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-averageBy raises on empty`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> PortSum.averageBy float
            |> Task.ignore

        |> should throwAsyncExact typeof<System.ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-averageByAsync raises on empty`` variant =
        fun () ->
            Gen.getEmptyVariant variant
            |> PortSum.averageByAsync (float >> Task.fromResult)
            |> Task.ignore

        |> should throwAsyncExact typeof<System.ArgumentException>

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-sum returns sum of 1..10`` variant = task {
        // items are 1..10; sum = 55
        let! result = Gen.getSeqImmutable variant |> PortSum.sum
        result |> should equal 55
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-sumBy returns sum of id 1..10`` variant = task {
        let! result = Gen.getSeqImmutable variant |> PortSum.sumBy id
        result |> should equal 55
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-sumBy with projection returns sum of doubled values`` variant = task {
        // sum of 2*i for i in 1..10 = 2 * 55 = 110
        let! result = Gen.getSeqImmutable variant |> PortSum.sumBy ((*) 2)
        result |> should equal 110
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-sumByAsync with async projection returns sum`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> PortSum.sumByAsync (fun x -> task { return x * 3 })

        // 3 * 55 = 165
        result |> should equal 165
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-average returns average of 1..10 as float`` variant = task {
        // items are 1..10; average = 5.5
        let! result =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.map float
            |> PortSum.average

        result |> should (equalWithin 0.001) 5.5
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-averageBy returns average of float projections`` variant = task {
        // average of float values 1.0..10.0 = 5.5
        let! result = Gen.getSeqImmutable variant |> PortSum.averageBy float
        result |> should (equalWithin 0.001) 5.5
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-averageBy with custom projection returns correct average`` variant = task {
        // sum of 2*i / count = 2 * 5.5 = 11.0
        let! result =
            Gen.getSeqImmutable variant
            |> PortSum.averageBy (float >> (*) 2.0)

        result |> should (equalWithin 0.001) 11.0
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-averageByAsync with async projection returns correct average`` variant = task {
        let! result =
            Gen.getSeqImmutable variant
            |> PortSum.averageByAsync (fun x -> task { return float x })

        result |> should (equalWithin 0.001) 5.5
    }

    [<Fact>]
    let ``AsyncSeq2-sum works with a single element`` () = task {
        let! result = AsyncSeq2.singleton 42 |> PortSum.sum
        result |> should equal 42
    }

    [<Fact>]
    let ``AsyncSeq2-average works with a single element`` () = task {
        let! result = AsyncSeq2.singleton 42.0 |> PortSum.average
        result |> should (equalWithin 0.001) 42.0
    }

    [<Fact>]
    let ``AsyncSeq2-sum works with float projection`` () = task {
        let! result = AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5 ] |> PortSum.sumBy float

        result |> should (equalWithin 0.001) 15.0
    }

    [<Fact>]
    let ``AsyncSeq2-sum works with int64`` () = task {
        let! result = AsyncSeq2.ofSeq [ 1L; 2L; 3L; 4L; 5L ] |> PortSum.sum

        result |> should equal 15L
    }

    [<Fact>]
    let ``AsyncSeq2-average works with float32`` () = task {
        let! result = AsyncSeq2.ofSeq [ 1.0f; 2.0f; 3.0f ] |> PortSum.average

        result |> should (equalWithin 0.001f) 2.0f
    }

    [<Fact>]
    let ``AsyncSeq2-sum result matches Seq-sum`` () = task {
        let items = [ 3; 1; 4; 1; 5; 9; 2; 6; 5; 3 ]
        let expected = Seq.sum items

        let! result = AsyncSeq2.ofList items |> PortSum.sum
        result |> should equal expected
    }

    [<Fact>]
    let ``AsyncSeq2-average result matches Seq-average`` () = task {
        let items = [ 3.0; 1.0; 4.0; 1.0; 5.0; 9.0; 2.0; 6.0; 5.0; 3.0 ]
        let expected = Seq.average items

        let! result = AsyncSeq2.ofList items |> PortSum.average
        result |> should (equalWithin 0.0001) expected
    }

    [<Fact>]
    let ``AsyncSeq2-sumBy result matches Seq-sumBy`` () = task {
        let items = [ 1; 2; 3; 4; 5 ]
        let expected = Seq.sumBy (fun x -> x * x) items

        let! result = AsyncSeq2.ofList items |> PortSum.sumBy (fun x -> x * x)
        result |> should equal expected
    }

    [<Fact>]
    let ``AsyncSeq2-averageBy result matches Seq-averageBy`` () = task {
        let items = [ 1; 2; 3; 4; 5 ]
        let expected = Seq.averageBy float items

        let! result = AsyncSeq2.ofList items |> PortSum.averageBy float
        result |> should (equalWithin 0.0001) expected
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-sum iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> PortSum.sum
        result |> should equal 55
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-sumBy iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> PortSum.sumBy id
        result |> should equal 55
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-sumByAsync iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> PortSum.sumByAsync Task.fromResult
        result |> should equal 55
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-average iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> AsyncSeq2.map float |> PortSum.average
        result |> should (equalWithin 0.001) 5.5
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-averageBy iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> PortSum.averageBy float
        result |> should (equalWithin 0.001) 5.5
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-averageByAsync iterates exactly once`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! result = ts |> PortSum.averageByAsync (float >> Task.fromResult)
        result |> should (equalWithin 0.001) 5.5
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-sum second iteration sees side-effect values`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! first = ts |> PortSum.sum
        first |> should equal 55 // 1+2+...+10

        // side-effect sequences yield next 10 items (11..20) on second consumption
        let! second = ts |> PortSum.sum
        second |> should equal 155 // 11+12+...+20
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-averageBy second iteration sees side-effect values`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! first = ts |> PortSum.averageBy float
        first |> should (equalWithin 0.001) 5.5 // avg(1..10) = 5.5

        // side-effect sequences yield next 10 items (11..20) on second consumption
        let! second = ts |> PortSum.averageBy float
        second |> should (equalWithin 0.001) 15.5 // avg(11..20) = 15.5
    }
