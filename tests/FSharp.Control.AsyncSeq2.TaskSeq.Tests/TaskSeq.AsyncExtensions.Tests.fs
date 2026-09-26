module AsyncSeq2.Tests.AsyncExtensions

open System
open System.Threading
open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// Async extensions
//

module Async2Extensions =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``Async2-for CE consumes asyncSeq2`` variant =
        async2 {
            let mutable sum = 0
            for value in Gen.getSeqImmutable variant do
                sum <- sum + value
            return sum
        }
        |> Async2.StartAsTask
        |> Task.map (should equal 55)

    [<Fact>]
    let ``Async2-for CE forwards cancellation to asyncSeq2`` () = task {
        use cts = new CancellationTokenSource()
        let source = asyncSeq2 {
            let! token = Async2.CancellationToken
            yield token
        }
        let computation = async2 {
            for token in source do
                token |> should equal cts.Token
        }
        do! Async2.StartAsTask(computation, cancellationToken = cts.Token)
    }

module EmptySeq =
    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``Async-for CE with empty asyncSeq2`` variant = async {
        let values = Gen.getEmptyVariant variant

        let mutable sum = 42

        for x in values do
            sum <- sum + x

        sum |> should equal 42
    }

    [<Fact>]
    let ``Async-for CE must execute side effect in empty asyncSeq2`` () = async {
        let mutable data = 0
        let values = asyncSeq2 { do data <- 42 }

        for _ in values do
            ()

        data |> should equal 42
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``Async-for CE with asyncSeq2`` variant = async {
        let values = Gen.getSeqImmutable variant

        let mutable sum = 0

        for x in values do
            sum <- sum + x

        sum |> should equal 55
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``Async-for CE with asyncSeq2 multiple iterations`` variant = async {
        let values = Gen.getSeqImmutable variant

        let mutable sum = 0

        for x in values do
            sum <- sum + x

        // each following iteration should start at the beginning
        for x in values do
            sum <- sum + x

        for x in values do
            sum <- sum + x

        sum |> should equal 165
    }

    [<Fact>]
    let ``Async-for mixing both types of for loops`` () = async {
        // this test ensures overload resolution is correct
        let ts = AsyncSeq2.singleton 20
        let sq = Seq.singleton 20
        let mutable sum = 2

        for x in ts do
            sum <- sum + x

        for x in sq do
            sum <- sum + x

        sum |> should equal 42
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``Async-for CE with asyncSeq2`` variant = async {
        let values = Gen.getSeqWithSideEffect variant

        let mutable sum = 0

        for x in values do
            sum <- sum + x

        sum |> should equal 55
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``Async-for CE with asyncSeq2 multiple iterations`` variant = async {
        let values = Gen.getSeqWithSideEffect variant

        let mutable sum = 0

        for x in values do
            sum <- sum + x

        // each following iteration should start at the beginning
        // with the "side effect" tests, the mutable state updates
        for x in values do
            sum <- sum + x // starts at 11

        for x in values do
            sum <- sum + x // starts at 21

        sum |> should equal 465 // eq to: List.sum [1..30]
    }

module ExceptionPropagation =
    [<Fact>]
    let ``Async-for CE propagates exception without AggregateException wrapping`` () =
        // Verifies fix for https://github.com/fsprojects/FSharp.Control.AsyncSeq2/issues/129
        // Async.AwaitTask previously wrapped all task exceptions in AggregateException,
        // breaking try/catch blocks in async {} expressions that expect the original type.
        let run () = async {
            let values = asyncSeq2 { yield 1 }

            try
                for _ in values do
                    raise (InvalidOperationException "test error")
            with :? InvalidOperationException ->
                ()
        }

        // Should complete without AggregateException escaping
        run () |> Async.RunSynchronously

    [<Fact>]
    let ``Async-for CE try-catch catches original exception type, not AggregateException`` () =
        // Verifies that the original exception type is visible in catch blocks,
        // not wrapped in AggregateException as Async.AwaitTask used to do.
        let mutable caughtType: Type option = None

        let run () = async {
            let values = asyncSeq2 { yield 1 }

            try
                for _ in values do
                    raise (ArgumentException "test")
            with ex ->
                caughtType <- Some(ex.GetType())
        }

        run () |> Async.RunSynchronously
        caughtType |> should equal (Some typeof<ArgumentException>)

module Other =
    [<Fact>]
    let ``Async-for CE must call dispose in empty asyncSeq2`` () = async {
        let disposed = ref 0
        let values = Gen.getEmptyDisposableTaskSeq disposed

        for _ in values do
            ()

        // the DisposeAsync should be called by now
        disposed.Value |> should equal 1
    }

    [<Fact>]
    let ``Async-for CE must call dispose on singleton`` () = async {
        let disposed = ref 0
        let mutable sum = 0
        let values = Gen.getSingletonDisposableTaskSeq disposed

        for x in values do
            sum <- x

        // the DisposeAsync should be called by now
        disposed.Value |> should equal 1
        sum |> should equal 42
    }

// Tests for nested for loops in the async CE with IAsyncEnumerable as the outer sequence.
// Related to: https://github.com/fsprojects/FSharp.Control.AsyncSeq2/issues/269
module NestedLoops =
    [<Fact>]
    let ``Async-for CE with nested regular list inside asyncSeq2 loop`` () = async {
        // outer: IAsyncEnumerable<int list>, inner: regular list
        let outer = asyncSeq2 {
            yield [ 1; 2; 3 ]
            yield [ 4; 5 ]
            yield [ 6; 7; 8; 9; 10 ]
        }

        let mutable sum = 0

        for inner in outer do
            for x in inner do
                sum <- sum + x

        sum |> should equal 55
    }

    [<Fact>]
    let ``Async-for CE with nested array inside asyncSeq2 loop`` () = async {
        // outer: IAsyncEnumerable<int[]>, inner: regular array
        let outer = asyncSeq2 {
            yield [| 1; 2; 3 |]
            yield [| 4; 5 |]
        }

        let mutable sum = 0

        for inner in outer do
            for x in inner do
                sum <- sum + x

        sum |> should equal 15
    }

    [<Fact>]
    let ``Async-for CE with nested tuple-destructuring array inside asyncSeq2 loop`` () = async {
        // outer: IAsyncEnumerable<int[]>, inner: zipped array with tuple destructuring
        // this pattern reproduces the scenario from issue #269
        let outer = asyncSeq2 { yield [| 1; 2; 3 |] }
        let mutable sum = 0

        for arr in outer do
            for (a, b) in Array.zip arr arr do
                sum <- sum + a + b

        // (1+1) + (2+2) + (3+3) = 12
        sum |> should equal 12
    }

    [<Fact>]
    let ``Async-for CE with nested asyncSeq2 inside asyncSeq2 loop`` () = async {
        // outer: IAsyncEnumerable<IAsyncEnumerable<int>>, inner: asyncSeq2
        let inner1 = asyncSeq2 {
            yield 1
            yield 2
            yield 3
        }

        let inner2 = asyncSeq2 {
            yield 4
            yield 5
        }

        let outer = asyncSeq2 {
            yield inner1
            yield inner2
        }

        let mutable sum = 0

        for inner in outer do
            for x in inner do
                sum <- sum + x

        sum |> should equal 15
    }
