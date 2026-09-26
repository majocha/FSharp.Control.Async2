module AsyncSeq2.Tests.Item

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.item
// ColdTask.tryItem
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> ColdTask.item 42 null
        assertNullArg <| fun () -> ColdTask.tryItem 42 null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-item throws on empty sequences`` variant = task {
        fun () -> Gen.getEmptyVariant variant |> ColdTask.item 0 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-item throws on empty sequence - high index`` variant = task {
        fun () ->
            Gen.getEmptyVariant variant
            |> ColdTask.item 50000
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryItem returns None on empty sequences`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryItem 0
        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryItem returns None on empty sequence - high index`` variant = task {
        let! nothing = Gen.getEmptyVariant variant |> ColdTask.tryItem 50000
        nothing |> should be None'
    }

module Singleton =
    [<Fact>]
    let ``AsyncSeq2-item gets the first item in a singleton sequence`` () = task {
        let! head = asyncSeq2 { yield 10 } |> ColdTask.item 0 // zero-based!
        head |> should equal 10
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem gets the first item in a singleton sequence`` () = task {
        let! head = asyncSeq2 { yield 10 } |> ColdTask.tryItem 0 // zero-based!
        head |> should be Some'
        head |> should equal (Some 10)
    }

    [<Fact>]
    let ``AsyncSeq2-item throws when accessing 2nd item in singleton sequence`` () = task {
        fun () -> asyncSeq2 { yield 10 } |> ColdTask.item 1 |> Task.ignore // zero-based!
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem returns None when accessing 2nd item in singleton sequence`` () = task {
        let! nothing = asyncSeq2 { yield 10 } |> ColdTask.tryItem 1 // zero-based!
        nothing |> should be None'
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-item throws when not found`` variant = task {
        fun () ->
            Gen.getSeqImmutable variant
            |> ColdTask.item 10
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryItem returns None when not found`` variant = task {
        let! nothing = Gen.getSeqImmutable variant |> ColdTask.tryItem 10 // zero-based index

        nothing |> should be None'
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-item can get the first and last item in a longer sequence`` variant = task {
        let! head = Gen.getSeqImmutable variant |> ColdTask.item 0
        let! tail = Gen.getSeqImmutable variant |> ColdTask.item 9
        head |> should equal 1
        tail |> should equal 10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryItem can get the first and last item in a longer sequence`` variant = task {
        let! head = Gen.getSeqImmutable variant |> ColdTask.tryItem 0 // zero-based!
        let! tail = Gen.getSeqImmutable variant |> ColdTask.tryItem 9

        head |> should equal (Some 1)
        tail |> should equal (Some 10)
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-item prove it searches the whole sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        fun () -> ts |> ColdTask.item 10 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        let! head = ColdTask.head ts
        head |> should equal 11 // all side effects have executed
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryItem prove it searches the whole sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! item = ts |> ColdTask.tryItem 10

        item |> should be None'
        let! head = ColdTask.head ts
        head |> should equal 11 // all side effects have executed
    }

    [<Fact>]
    let ``AsyncSeq2-item prove we don't iterate further than the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1
            yield 2
            i <- i + 1 // we never get here
        }

        // zero-based index
        do! ts |> ColdTask.item 1 |> Task.map (should equal 2)
        i |> should equal 2 // last side effect is not executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem prove we don't iterate further than the found item`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1
            yield 2
            i <- i + 1 // we never get here
        }

        // zero-based index
        do! ts |> ColdTask.tryItem 1 |> Task.map (should equal (Some 2))
        i |> should equal 2 // last side effect is not executed
    }

    [<Fact>]
    let ``AsyncSeq2-item prove we iterate beyond the end when not found`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1
            yield 2
            i <- i + 1 // we never get here
        }

        // zero-based
        fun () -> ts |> ColdTask.item 2 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        i |> should equal 3 // last side effect MUST be executed
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem prove we iterate beyond the end when not found`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            yield 1
            i <- i + 1
            yield 2
            i <- i + 1 // we never get here
        }

        do! ts |> ColdTask.tryItem 2 |> Task.map (should be None')
        i |> should equal 3 // last side effect MUST be executed
    }


module Performance =

    [<Theory; InlineData 10_000; InlineData 100_000; InlineData 1_000_000>]
    let ``AsyncSeq2-tryItem in a very long sequence -- yield variant`` total = task {
        let! head =
            asyncSeq2 {
                for i in [ 0..total ] do
                    yield i
            }
            |> ColdTask.tryItem total // zero-based!

        head |> should equal (Some total)
    }

    [<Theory; InlineData 10_000; InlineData 100_000; InlineData 1_000_000>]
    let ``[compare] Seq-tryItem in a very long sequence -- yield using F# Seq`` total = task {
        // this test is just for smoke-test perf comparison with AsyncSeq2 above
        let head =
            seq {
                for i in [ 0..total ] do
                    yield i
            }
            |> Seq.tryItem total // zero-based!

        head |> should equal (Some total)
    }

    [<Theory; InlineData 10_000; InlineData 100_000; InlineData 1_000_000>]
    let ``AsyncSeq2-tryItem in a very long sequence -- array variant`` total = task {
        let! head = asyncSeq2 { yield! [| 0..total |] } |> ColdTask.tryItem total // zero-based!

        head |> should equal (Some total)
    }

    [<Theory; InlineData 10_000; InlineData 100_000; InlineData 1_000_000>]
    let ``[compare] Seq-tryItem in a very long sequence -- array using F# Seq`` total = task {
        // this test is just for smoke-test perf comparison with AsyncSeq2 above
        let head = seq { yield! [| 0..total |] } |> Seq.tryItem total // zero-based!

        head |> should equal (Some total)
    }

module Other =
    [<Fact>]
    let ``AsyncSeq2-item accepts Int-MaxValue`` () = task {
        let make50 () = Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 50

        fun () -> make50 () |> ColdTask.item Int32.MaxValue |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            (AsyncSeq2.empty<string> ())
            |> ColdTask.item Int32.MaxValue
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem accepts Int-MaxValue`` () = task {
        let! nope =
            Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 50
            |> ColdTask.tryItem Int32.MaxValue

        nope |> should be None'

        let! nope = (AsyncSeq2.empty<string> ()) |> ColdTask.tryItem Int32.MaxValue
        nope |> should be None'
    }

    [<Fact>]
    let ``AsyncSeq2-item always throws with negative values`` () = task {
        let make50 () = Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 50

        fun () -> make50 () |> ColdTask.item -1 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> make50 () |> ColdTask.item -10000 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> make50 () |> ColdTask.item Int32.MinValue |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> (AsyncSeq2.empty<string> ()) |> ColdTask.item -1 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () -> (AsyncSeq2.empty<string> ()) |> ColdTask.item -10000 |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>

        fun () ->
            (AsyncSeq2.empty<string> ())
            |> ColdTask.item Int32.MinValue
            |> Task.ignore
        |> should throwAsyncExact typeof<ArgumentException>
    }

    [<Fact>]
    let ``AsyncSeq2-tryItem throws with negative values`` () = task {
        let make50 () = Gen.sideEffectTaskSeqMicro 50L<µs> 1000L<µs> 50

        let! nothing = make50 () |> ColdTask.tryItem -1
        nothing |> should be None'

        let! nothing = make50 () |> ColdTask.tryItem -10000
        nothing |> should be None'

        let! nothing = make50 () |> ColdTask.tryItem Int32.MinValue
        nothing |> should be None'

        let! nothing = (AsyncSeq2.empty<string> ()) |> ColdTask.tryItem -1
        nothing |> should be None'

        let! nothing = (AsyncSeq2.empty<string> ()) |> ColdTask.tryItem -10000
        nothing |> should be None'

        let! nothing = (AsyncSeq2.empty<string> ()) |> ColdTask.tryItem Int32.MinValue
        nothing |> should be None'
    }
