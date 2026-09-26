module AsyncSeq2.Tests.``WithCancellation``

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

/// A simple IAsyncEnumerable whose GetAsyncEnumerator records the token it was called with.
type TokenCapturingSeq<'T>(items: 'T list) =
    let mutable capturedToken = CancellationToken.None

    member _.CapturedToken = capturedToken

    interface IAsyncEnumerable<'T> with
        member _.GetAsyncEnumerator(ct) =
            capturedToken <- ct

            let source = asyncSeq2 {
                for x in items do
                    yield x
            }

            source.GetAsyncEnumerator(ct)

module ``Null check`` =

    [<Fact>]
    let ``AsyncSeq2-withCancellation: null source throws ArgumentNullException`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.withCancellation CancellationToken.None null

module ``Token threading`` =

    [<Fact>]
    let ``AsyncSeq2-withCancellation: passes supplied token to GetAsyncEnumerator`` () = task {
        let source = TokenCapturingSeq([ 1; 2; 3 ])
        use cts = new CancellationTokenSource()

        let wrapped = AsyncSeq2.withCancellation cts.Token (source :> IAsyncEnumerable<_>)
        let! _ = ColdTask.toArrayAsync wrapped
        source.CapturedToken |> should equal cts.Token
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation: overrides any token passed to GetAsyncEnumerator`` () = task {
        let source = TokenCapturingSeq([ 1; 2; 3 ])
        use cts = new CancellationTokenSource()

        let wrapped = AsyncSeq2.withCancellation cts.Token (source :> IAsyncEnumerable<_>)

        // Consume with a different token; withCancellation should win
        use outerCts = new CancellationTokenSource()
        let enum = wrapped.GetAsyncEnumerator(outerCts.Token)

        while! enum.MoveNextAsync() do
            ()

        source.CapturedToken |> should equal cts.Token
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation: CancellationToken.None passes through correctly`` () = task {
        let source = TokenCapturingSeq([ 10; 20 ])

        let wrapped = AsyncSeq2.withCancellation CancellationToken.None (source :> IAsyncEnumerable<_>)
        let! _ = ColdTask.toArrayAsync wrapped
        source.CapturedToken |> should equal CancellationToken.None
    }

module ``Cancellation behaviour`` =

    [<Fact>]
    let ``AsyncSeq2-withCancellation: pre-cancelled token causes OperationCanceledException on iteration`` () = task {
        use cts = new CancellationTokenSource()
        cts.Cancel()

        let source = asyncSeq2 {
            while true do
                yield 1
        }

        let wrapped = AsyncSeq2.withCancellation cts.Token source

        fun () -> AsyncSeq2.iter ignore wrapped |> Async2.StartAsTask |> Task.ignore
        |> should throwAsync typeof<OperationCanceledException>
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation: token cancelled mid-iteration raises OperationCanceledException`` () = task {
        use cts = new CancellationTokenSource()

        let source = asyncSeq2 {
            for i in 1..100 do
                yield i
        }

        let wrapped = AsyncSeq2.withCancellation cts.Token source

        fun () ->
            task {
                let mutable count = 0
                use enum = wrapped.GetAsyncEnumerator(CancellationToken.None)

                while! enum.MoveNextAsync() do
                    count <- count + 1

                    if count = 3 then
                        cts.Cancel()
            }
            |> Task.ignore
        |> should throwAsync typeof<OperationCanceledException>
    }

module ``Sequence contents`` =

    [<Fact>]
    let ``AsyncSeq2-withCancellation: empty source produces empty sequence`` () =
        (AsyncSeq2.empty<int> ())
        |> AsyncSeq2.withCancellation CancellationToken.None
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-withCancellation: finite source produces all items`` () = task {
        let! result =
            asyncSeq2 {
                for i in 1..10 do
                    yield i
            }
            |> AsyncSeq2.withCancellation CancellationToken.None
            |> ColdTask.toArrayAsync

        result |> should equal [| 1..10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation: can be used with AsyncSeq2 combinators`` () = task {
        use cts = new CancellationTokenSource()

        let! result =
            asyncSeq2 {
                for i in 1..5 do
                    yield i
            }
            |> AsyncSeq2.withCancellation cts.Token
            |> AsyncSeq2.map (fun x -> x * 2)
            |> ColdTask.toArrayAsync

        result |> should equal [| 2; 4; 6; 8; 10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation: can be piped like .WithCancellation usage pattern`` () = task {
        use cts = new CancellationTokenSource()
        let mutable collected = ResizeArray()

        let source = asyncSeq2 {
            for i in 1..5 do
                yield i
        }

        do!
            source
            |> AsyncSeq2.withCancellation cts.Token
            |> AsyncSeq2.iterAsync (fun x -> async2 { collected.Add(x) })
            |> Async2.StartAsTask

        collected |> Seq.toArray |> should equal [| 1..5 |]
    }

module SideEffects =

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-withCancellation applied multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let wrapped = AsyncSeq2.withCancellation CancellationToken.None ts

        let! first = wrapped |> ColdTask.toArrayAsync
        let! second = wrapped |> ColdTask.toArrayAsync
        let! third = wrapped |> ColdTask.toArrayAsync

        first |> should equal [| 1..10 |]
        second |> should equal [| 11..20 |]
        third |> should equal [| 21..30 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-withCancellation with active CancellationToken applied multiple times`` variant = task {
        use cts = new CancellationTokenSource()
        let ts = Gen.getSeqWithSideEffect variant
        let wrapped = AsyncSeq2.withCancellation cts.Token ts

        let! first = wrapped |> ColdTask.toArrayAsync
        let! second = wrapped |> ColdTask.toArrayAsync

        first |> should equal [| 1..10 |]
        second |> should equal [| 11..20 |]
    }

    [<Fact>]
    let ``AsyncSeq2-withCancellation evaluates each source element exactly once per iteration`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! _ =
            ts
            |> AsyncSeq2.withCancellation CancellationToken.None
            |> ColdTask.toArrayAsync

        count |> should equal 5

        let! _ =
            ts
            |> AsyncSeq2.withCancellation CancellationToken.None
            |> ColdTask.toArrayAsync

        count |> should equal 10
    }
