module FSharp.Core.UnitTests.Control.AsyncSeq2Tests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Xunit

let private run computation = Async2.RunSynchronouslyImmediate computation

let private drain source = source |> AsyncSeq2.toArray |> run

type private TrackedResource(onDispose: unit -> unit) =
    interface IAsyncDisposable with
        member _.DisposeAsync() =
            ValueTask(task {
                do! Task.Yield()
                onDispose ()
            })

type private SyncResource(onDispose: unit -> unit) =
    interface IDisposable with
        member _.Dispose() = onDispose ()

[<Fact>]
let ``sequence is cold and repeatable`` () =
    let mutable calls = 0
    let source = asyncSeq2 {
        calls <- calls + 1
        yield calls
        yield calls + 1
    }

    Assert.Equal(0, calls)
    Assert.Equal<int>([| 1; 2 |], drain source)
    Assert.Equal<int>([| 2; 3 |], drain source)

[<Fact>]
let ``yield and nested yield from seq and async sequence preserve order`` () =
    let source = asyncSeq2 {
        yield 0
        yield! [ 1; 2 ]
        for i in asyncSeq2 { yield 3; yield 4 } do
            yield i
        yield! asyncSeq2 { yield 5 }
    }
    Assert.Equal<int>([| 0; 1; 2; 3; 4; 5 |], drain source)

[<Fact>]
let ``empty singleton and ofSeq are interoperable`` () =
    Assert.Empty(drain (AsyncSeq2.empty<int> ()))
    Assert.Equal<int>([| 42 |], drain (AsyncSeq2.singleton 42))
    Assert.Equal<int>([| 1; 2 |], drain (AsyncSeq2.ofSeq [ 1; 2 ]))

[<Fact>]
let ``task value task and async2 bindings work together`` () =
    let source = asyncSeq2 {
        let! first = Task.FromResult 10
        let! second = ValueTask<int>(20)
        let! third = async2 {
            do! Async2.Sleep 1
            return 12
        }
        yield first + second + third
    }
    Assert.Equal<int>([| 42 |], drain source)

[<Fact>]
let ``cold cancellation aware awaitables see enumerator token`` () = task {
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        let! token = (fun (ct: CancellationToken) -> Task.FromResult ct)
        yield token
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! result = enumerator.MoveNextAsync()
    Assert.True result
    Assert.Equal(cts.Token, enumerator.Current)
}

[<Fact>]
let ``async2 binding observes enumerator token`` () = task {
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        let! token = Async2.CancellationToken
        yield token
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! hasNext = enumerator.MoveNextAsync()
    Assert.True hasNext
    Assert.Equal(cts.Token, enumerator.Current)
}

[<Fact>]
let ``built in async binding observes enumerator token`` () = task {
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        let! token = async { return! Async.CancellationToken }
        yield token
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! hasNext = enumerator.MoveNextAsync()
    Assert.True hasNext
    Assert.Equal(cts.Token, enumerator.Current)
}

[<Fact>]
let ``yield from and for forward cancellation to nested enumerators`` () = task {
    use cts = new CancellationTokenSource()
    let tokens = ResizeArray<CancellationToken>()
    let inner =
        { new IAsyncEnumerable<int> with
            member _.GetAsyncEnumerator ct =
                tokens.Add ct
                (asyncSeq2 { yield 1 }).GetAsyncEnumerator ct }
    let source = asyncSeq2 {
        yield! inner
        for item in inner do
            yield item
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! first = enumerator.MoveNextAsync()
    let! second = enumerator.MoveNextAsync()
    Assert.True first
    Assert.True second
    Assert.Equal<CancellationToken>([| cts.Token; cts.Token |], tokens.ToArray())
}

[<Fact>]
let ``pre cancelled token does not execute sequence body`` () = task {
    use cts = new CancellationTokenSource()
    cts.Cancel()
    let mutable started = false
    let source = asyncSeq2 {
        started <- true
        yield 1
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> enumerator.MoveNextAsync().AsTask() :> Task)
    Assert.Equal(cts.Token, error.CancellationToken)
    Assert.False started
}

[<Fact>]
let ``cancellation between yields stops iteration`` () = task {
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        for item in 1..10 do
            yield item
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! first = enumerator.MoveNextAsync()
    Assert.True first
    cts.Cancel()
    let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> enumerator.MoveNextAsync().AsTask() :> Task)
    Assert.Equal(cts.Token, error.CancellationToken)
}

[<Fact>]
let ``cancellation interrupts async2 sleep in sequence`` () = task {
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        do! Async2.Sleep 30000
        yield 1
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let next = enumerator.MoveNextAsync().AsTask()
    cts.Cancel()
    let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> next :> Task)
    Assert.Equal(cts.Token, error.CancellationToken)
}

[<Fact>]
let ``separate enumerators have separate cancellation`` () = task {
    let source = asyncSeq2 {
        let! ct = Async2.CancellationToken
        yield ct
    }
    use first = new CancellationTokenSource()
    use second = new CancellationTokenSource()
    use e1 = source.GetAsyncEnumerator(first.Token)
    use e2 = source.GetAsyncEnumerator(second.Token)
    let! _ = e1.MoveNextAsync()
    let! _ = e2.MoveNextAsync()
    Assert.Equal(first.Token, e1.Current)
    Assert.Equal(second.Token, e2.Current)
}

[<Fact>]
let ``async2 for and terminal operations pass their token`` () =
    use cts = new CancellationTokenSource()
    let source = asyncSeq2 {
        let! ct = Async2.CancellationToken
        yield ct
    }
    let computation = async2 {
        let values = ResizeArray<CancellationToken>()
        for token in source do
            values.Add token
        let! array = AsyncSeq2.toArray source
        return values.[0], array.[0]
    }
    let fromFor, fromTerminal = Async2.RunSynchronouslyImmediate(computation, cancellationToken = cts.Token)
    Assert.Equal(cts.Token, fromFor)
    Assert.Equal(cts.Token, fromTerminal)

[<Fact>]
let ``terminal operation uses async2 default cancellation token`` () =
    let source = asyncSeq2 {
        let! ct = Async2.CancellationToken
        yield ct
    }
    Assert.Equal(Async2.DefaultCancellationToken, (drain source).[0])

[<Fact>]
let ``ported sequence transformations forward the enumerator token`` () =
    use cts = new CancellationTokenSource()
    let tokens = ResizeArray<CancellationToken>()
    let source =
        { new IAsyncEnumerable<int> with
            member _.GetAsyncEnumerator ct =
                tokens.Add ct
                (asyncSeq2 { yield 1; yield 2; yield 3 }).GetAsyncEnumerator ct }

    let consume sequence =
        Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray sequence, cancellationToken = cts.Token)
        |> ignore

    consume (AsyncSeq2.zip source source)
    consume (AsyncSeq2.take 2 source)
    consume (AsyncSeq2.skip 1 source)
    consume (AsyncSeq2.takeWhile (fun n -> n < 3) source)
    consume (AsyncSeq2.distinctUntilChanged source)
    consume (AsyncSeq2.pairwise source)
    Assert.Equal(7, tokens.Count)
    Assert.All(tokens, fun token -> Assert.Equal(cts.Token, token))

[<Fact>]
let ``terminal operations and transformations compose`` () =
    let source =
        AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
        |> AsyncSeq2.filter (fun n -> n % 2 = 0)
        |> AsyncSeq2.map ((*) 10)
        |> AsyncSeq2.collect (fun n -> asyncSeq2 { yield n; yield n + 1 })
        |> fun values -> AsyncSeq2.append values (AsyncSeq2.singleton 50)
    Assert.Equal<int>([| 20; 21; 40; 41; 50 |], drain source)
    Assert.Equal(5, source |> AsyncSeq2.length |> run)
    Assert.False(source |> AsyncSeq2.isEmpty |> run)
    Assert.Equal<int list>([ 20; 21; 40; 41; 50 ], source |> AsyncSeq2.toList |> run)
    Assert.True(AsyncSeq2.empty<int> () |> AsyncSeq2.isEmpty |> run)

[<Fact>]
let ``async transformations and iteration use async2`` () =
    let source =
        AsyncSeq2.ofSeq [ 1; 2; 3 ]
        |> AsyncSeq2.mapAsync (fun n -> async2 { return n * 2 })
    let seen = ResizeArray<int>()
    source
    |> AsyncSeq2.iterAsync (fun n -> async2 { seen.Add n })
    |> run
    Assert.Equal<int>([| 2; 4; 6 |], seen.ToArray())

[<Fact>]
let ``using disposes asynchronously after full iteration`` () =
    let mutable disposed = 0
    let source = asyncSeq2 {
        use _resource = new TrackedResource(fun () -> disposed <- disposed + 1)
        yield 1
    }
    Assert.Equal<int>([| 1 |], drain source)
    Assert.Equal(1, disposed)
    Assert.Equal<int>([| 1 |], drain source)
    Assert.Equal(2, disposed)

[<Fact>]
let ``using and use bang dispose synchronous and asynchronous resources`` () =
    let mutable synchronous = 0
    let mutable asynchronous = 0
    let source = asyncSeq2 {
        use _sync = new SyncResource(fun () -> synchronous <- synchronous + 1)
        use! _async = async2 {
            return new TrackedResource(fun () -> asynchronous <- asynchronous + 1)
        }
        yield 1
    }
    Assert.Equal<int>([| 1 |], drain source)
    Assert.Equal(1, synchronous)
    Assert.Equal(1, asynchronous)

[<Fact>]
let ``using disposes after a sequence fault`` () =
    let mutable disposed = false
    let source = asyncSeq2 {
        use _resource = new TrackedResource(fun () -> disposed <- true)
        yield 1
        failwith "boom"
    }
    let error = Assert.Throws<Exception>(fun () -> drain source |> ignore)
    Assert.Equal("boom", error.Message)
    Assert.True disposed

[<Fact>]
let ``using disposes on early termination`` () = task {
    let mutable disposed = false
    let source = asyncSeq2 {
        use _resource = new TrackedResource(fun () -> disposed <- true)
        yield 1
        yield 2
    }
    let enumerator = source.GetAsyncEnumerator()
    let! first = enumerator.MoveNextAsync()
    Assert.True first
    do! enumerator.DisposeAsync()
    Assert.True disposed
}

[<Fact>]
let ``cancellation disposes acquired resource`` () = task {
    use cts = new CancellationTokenSource()
    let mutable disposed = false
    let source = asyncSeq2 {
        use _resource = new TrackedResource(fun () -> disposed <- true)
        yield 1
        yield 2
    }
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! first = enumerator.MoveNextAsync()
    Assert.True first
    cts.Cancel()
    let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> enumerator.MoveNextAsync().AsTask() :> Task)
    Assert.True disposed
}

[<Fact>]
let ``try with handles faults and finally runs`` () =
    let mutable finished = false
    let source = asyncSeq2 {
        try
            try
                yield 1
                failwith "boom"
            with
            | :? InvalidOperationException ->
                yield 2
            | error when error.Message = "boom" ->
                yield 3
        finally
            finished <- true
    }
    Assert.Equal<int>([| 1; 3 |], drain source)
    Assert.True finished

[<Fact>]
let ``null sources are rejected`` () =
    Assert.Throws<ArgumentNullException>(fun () -> AsyncSeq2.toArray null |> ignore) |> ignore
    Assert.Throws<ArgumentNullException>(fun () -> AsyncSeq2.map id null |> ignore) |> ignore
