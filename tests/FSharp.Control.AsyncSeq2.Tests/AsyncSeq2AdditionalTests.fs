module FSharp.Core.UnitTests.Control.AsyncSeq2AdditionalTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Xunit

let private run computation = Async2.RunSynchronouslyImmediate computation
let private drain source = AsyncSeq2.toArray source |> run

[<Fact>]
let ``cycle repeats values but stays empty for an empty source`` () =
    let values = AsyncSeq2.cycle (AsyncSeq2.ofSeq [ 1; 2 ]) |> AsyncSeq2.take 5
    Assert.Equal<int>([| 1; 2; 1; 2; 1 |], drain values)
    Assert.Empty(drain (AsyncSeq2.cycle (AsyncSeq2.empty<int> ())))

[<Fact>]
let ``rev and splitInto enumerate once and retain balanced order`` () =
    let mutable enumerations = 0
    let values = asyncSeq2 {
        enumerations <- enumerations + 1
        yield! [ 1; 2; 3; 4; 5 ]
    }
    Assert.Equal<int>([| 5; 4; 3; 2; 1 |], drain (AsyncSeq2.rev values))
    Assert.Equal(1, enumerations)
    let chunks = AsyncSeq2.splitInto 2 values |> run
    Assert.Equal<int>([| 1; 2; 3 |], chunks[0])
    Assert.Equal<int>([| 4; 5 |], chunks[1])
    Assert.Equal(2, enumerations)
    Assert.Throws<ArgumentException>(fun () -> AsyncSeq2.splitInto 0 values |> ignore) |> ignore

[<Fact>]
let ``sorted results use a cold Async2 and preserve cancellation`` () =
    use cts = new CancellationTokenSource()
    let mutable starts = 0
    let source = asyncSeq2 {
        starts <- starts + 1
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        yield! [ 3; 1; 2 ]
    }
    let computation = AsyncSeq2.sortAsync source
    Assert.Equal(0, starts)
    let sorted = Async2.RunSynchronouslyImmediate(computation, cancellationToken = cts.Token)
    Assert.Equal<int>([| 1; 2; 3 |], sorted)
    Assert.Equal(1, starts)
    Assert.Equal<int>([| 3; 2; 1 |], Async2.RunSynchronouslyImmediate(AsyncSeq2.sortDescendingAsync source, cancellationToken = cts.Token))

[<Fact>]
let ``map2 map3 allPairs and concatSeq follow source order`` () =
    let a = AsyncSeq2.ofSeq [ 1; 2 ]
    let b = AsyncSeq2.ofSeq [ 10; 20; 30 ]
    Assert.Equal<int>([| 11; 22 |], drain (AsyncSeq2.map2 (+) a b))
    Assert.Equal<int>([| 111; 222 |], drain (AsyncSeq2.map3 (fun x y z -> x + y + z) a b (AsyncSeq2.ofSeq [ 100; 200 ])))
    Assert.Equal<int * int>([| 1, 10; 1, 20; 1, 30; 2, 10; 2, 20; 2, 30 |], drain (AsyncSeq2.allPairs a b))
    Assert.Equal<int>([| 1; 2; 3 |], drain (AsyncSeq2.concatSeq (AsyncSeq2.ofSeq [ [ 1; 2 ]; [ 3 ] ])))

[<Fact>]
let ``backward searches visit all elements and return last match`` () =
    let values = AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
    Assert.Equal(Some 4, AsyncSeq2.tryFindBack (fun x -> x % 2 = 0) values |> run)
    Assert.Equal(Some 3, AsyncSeq2.tryFindIndexBack (fun x -> x % 2 = 0) values |> run)
    Assert.Equal(4, AsyncSeq2.findBack (fun x -> x % 2 = 0) values |> run)
    Assert.Equal(3, AsyncSeq2.findIndexBackAsync (fun x -> async2 { return x % 2 = 0 }) values |> run)
    Assert.Equal(None, AsyncSeq2.tryFindBackAsync (fun x -> async2 { return x > 10 }) values |> run)

[<Fact>]
let ``exists2 and forall2 short circuit and use Async2 callback tokens`` () =
    use cts = new CancellationTokenSource()
    let mutable called = 0
    let a = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let b = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let check x y = async2 {
        called <- called + 1
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        return x = y
    }
    Assert.True(Async2.RunSynchronouslyImmediate(AsyncSeq2.forall2Async check a b, cancellationToken = cts.Token))
    Assert.Equal(3, called)
    Assert.True(AsyncSeq2.exists2 (=) a b |> run)
    Assert.False(AsyncSeq2.forall2 (=) a (AsyncSeq2.ofSeq [ 0; 2; 3 ]) |> run)

[<Fact>]
let ``unzip and unzip3 consume the input once`` () =
    let first, second = AsyncSeq2.unzip (AsyncSeq2.ofSeq [ 1, "a"; 2, "b" ]) |> run
    Assert.Equal<int>([| 1; 2 |], first)
    Assert.Equal<string>([| "a"; "b" |], second)
    let a, b, c = AsyncSeq2.unzip3 (AsyncSeq2.ofSeq [ 1, 2, 3 ]) |> run
    Assert.Equal<int>([| 1 |], a)
    Assert.Equal<int>([| 2 |], b)
    Assert.Equal<int>([| 3 |], c)

[<Fact>]
let ``channel source and async2 sources forward cancellation`` () =
    use cts = new CancellationTokenSource()
    let channel = Channel.CreateUnbounded<int>()
    Assert.True(channel.Writer.TryWrite 42)
    channel.Writer.Complete()
    let consumed = Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray (AsyncSeq2.fromChannel channel.Reader), cancellationToken = cts.Token)
    Assert.Equal<int>([| 42 |], consumed)
    let source = AsyncSeq2.ofSeqAsync [ async2 { let! token = Async2.CancellationToken in return token } ]
    Assert.Equal(cts.Token, Async2.RunSynchronouslyImmediate(AsyncSeq2.tryFirst source, cancellationToken = cts.Token).Value)

[<Fact>]
let ``indexed Async2 mapping sees the enumeration token`` () =
    use cts = new CancellationTokenSource()
    let projection i x = async2 {
        let! ct = Async2.CancellationToken
        Assert.Equal(cts.Token, ct)
        return i + int64 x
    }
    let mapped = AsyncSeq2.zipWithIndexAsync projection (AsyncSeq2.ofSeq [ 10; 20 ])
    let values = Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray mapped, cancellationToken = cts.Token)
    Assert.Equal<int64>([| 10L; 21L |], values)

[<Fact>]
let ``interleave emits the remaining values and forwards cancellation`` () =
    use cts = new CancellationTokenSource()
    let tokens = ResizeArray<CancellationToken>()
    let track values =
        { new IAsyncEnumerable<int> with
            member _.GetAsyncEnumerator ct =
                tokens.Add ct
                (AsyncSeq2.ofSeq values).GetAsyncEnumerator ct }
    let merged = AsyncSeq2.interleave (track [ 1; 3; 5 ]) (track [ 2 ])
    let result = Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray merged, cancellationToken = cts.Token)
    Assert.Equal<int>([| 1; 2; 3; 5 |], result)
    Assert.Equal<CancellationToken>([| cts.Token; cts.Token |], tokens.ToArray())
    Assert.Equal<Choice<int, string>>(
        [| Choice1Of2 1; Choice2Of2 "a"; Choice1Of2 2 |],
        drain (AsyncSeq2.interleaveChoice (AsyncSeq2.ofSeq [ 1; 2 ]) (AsyncSeq2.singleton "a")))
    let many = AsyncSeq2.interleaveMany [ AsyncSeq2.ofSeq [ 1; 3 ]; AsyncSeq2.ofSeq [ 2; 4 ] ]
    Assert.Equal<int>([| 1; 2; 3; 4 |], drain many)

[<Fact>]
let ``interleaveMany preserves upstream order for uneven streams`` () =
    let streams =
        [ AsyncSeq2.ofSeq [ "a"; "b" ]
          AsyncSeq2.ofSeq [ "i"; "j"; "k"; "l" ]
          AsyncSeq2.ofSeq [ "x"; "y"; "z" ] ]
    Assert.Equal<string>([| "a"; "x"; "i"; "y"; "b"; "z"; "j"; "k"; "l" |],
                         drain (AsyncSeq2.interleaveMany streams))
    Assert.Empty(drain (AsyncSeq2.interleaveMany Seq.empty<AsyncSeq2<int>>))

[<Fact>]
let ``traverse short circuits and returns a repeatable sequence`` () =
    let mutable calls = 0
    let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let choose x = async2 {
        calls <- calls + 1
        return if x < 2 then Some(x * 2) else None
    }
    Assert.Equal(None, AsyncSeq2.traverseOptionAsync choose source |> run)
    Assert.Equal(2, calls)
    let traverse x = async2 { return Choice1Of2(x * 2) }
    match AsyncSeq2.traverseChoiceAsync traverse source |> run with
    | Choice1Of2 values ->
        Assert.Equal<int>([| 2; 4; 6 |], drain values)
        Assert.Equal<int>([| 2; 4; 6 |], drain values)
    | Choice2Of2 error -> failwithf "Unexpected failure: %A" error

[<Fact>]
let ``zapp and buffered values align with input`` () =
    let functions = AsyncSeq2.ofSeq [ ((+) 1); ((*) 3) ]
    Assert.Equal<int>([| 2; 6 |], drain (AsyncSeq2.zapp functions (AsyncSeq2.ofSeq [ 1; 2; 3 ])))
    let buffered = AsyncSeq2.bufferByCount 2 (AsyncSeq2.ofSeq [ 1; 2; 3 ]) |> drain
    Assert.Equal<int>([| 1; 2 |], buffered[0])
    Assert.Equal<int>([| 3 |], buffered[1])

[<Fact>]
let ``toChannel completes on success and propagates source errors`` () =
    let success = Channel.CreateUnbounded<int>()
    AsyncSeq2.toChannel success.Writer (AsyncSeq2.ofSeq [ 1; 2 ]) |> run
    Assert.Equal<int>([| 1; 2 |], drain (AsyncSeq2.fromChannel success.Reader))
    let failure = Channel.CreateUnbounded<int>()
    let source = asyncSeq2 {
        yield 1
        failwith "source failed"
    }
    let error = Assert.Throws<Exception>(fun () -> AsyncSeq2.toChannel failure.Writer source |> run)
    Assert.Equal("source failed", error.Message)
    Assert.ThrowsAny<Exception>(fun () -> AsyncSeq2.fromChannel failure.Reader |> drain |> ignore) |> ignore
    Assert.True(failure.Reader.Completion.IsFaulted)

[<Fact>]
let ``parallel zip starts both enumerators before awaiting either`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let left = asyncSeq2 {
        do! release.Task
        yield 1
    }
    let right = asyncSeq2 {
        release.TrySetResult(()) |> ignore
        yield 2
    }
    let mapping x y = async2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        return x + y
    }
    let source = AsyncSeq2.zipWithAsyncParallel mapping left right
    let! values = Async2.StartAsTask(AsyncSeq2.toArray source, cancellationToken = cts.Token)
    Assert.Equal<int>([| 3 |], values)
}

[<Fact>]
let ``merge starts both streams and preserves failures`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let ready = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let first = asyncSeq2 {
        do! ready.Task
        yield Choice1Of2 1
    }
    let second = asyncSeq2 {
        ready.TrySetResult(()) |> ignore
        yield Choice2Of2 "two"
    }
    let values = AsyncSeq2.merge first second |> AsyncSeq2.toArray
    let! merged = Async2.StartAsTask(values, cancellationToken = cts.Token)
    Assert.Equal(2, merged.Length)
    Assert.Contains(Choice1Of2 1, merged)
    Assert.Contains(Choice2Of2 "two", merged)

    let broken = asyncSeq2 {
        yield 1
        failwith "merge failure"
    }
    let sleeping = asyncSeq2 {
        do! Async2.Sleep 30000
        yield 2
    }
    let! error = Assert.ThrowsAnyAsync<Exception>(fun () ->
        Async2.StartAsTask(AsyncSeq2.toArray (AsyncSeq2.merge broken sleeping), cancellationToken = cts.Token) :> Task)
    Assert.Equal("merge failure", error.Message)
}

[<Fact>]
let ``combineLatest updates after both sources have a value`` () =
    let left = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let right = AsyncSeq2.singleton 10
    let values = AsyncSeq2.combineLatest left right |> drain
    Assert.True(values.Length >= 1)
    Assert.Equal((3, 10), values[values.Length - 1])
    Assert.All(values, fun (a, b) -> Assert.InRange(a, 1, 3); Assert.Equal(10, b))

[<Fact>]
let ``parallel map preserves order and honors its concurrency limit`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let firstTwo = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable active = 0
    let mutable peak = 0
    let mapping value = async2 {
        let! token = Async2.CancellationToken
        Assert.True(token.CanBeCanceled)
        let running = Interlocked.Increment(&active)
        peak <- max peak running
        if running = 2 then firstTwo.TrySetResult(()) |> ignore
        do! firstTwo.Task
        Interlocked.Decrement(&active) |> ignore
        return value * 10
    }
    let source = AsyncSeq2.mapAsyncParallelThrottled 2 mapping (AsyncSeq2.ofSeq [ 1; 2; 3 ])
    let! values = Async2.StartAsTask(AsyncSeq2.toArray source, cancellationToken = cts.Token)
    Assert.Equal<int>([| 10; 20; 30 |], values)
    Assert.Equal(2, peak)
    Assert.Equal(0, active)
    Assert.Throws<ArgumentException>(fun () -> AsyncSeq2.mapAsyncParallelThrottled 0 mapping (AsyncSeq2.empty ()) |> ignore) |> ignore
}

[<Fact>]
let ``unordered parallel map emits completed values first`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let release = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let mapping value = async2 {
        if value = 1 then do! release.Task
        return value
    }
    let source = AsyncSeq2.mapAsyncUnorderedParallel mapping (AsyncSeq2.ofSeq [ 1; 2 ])
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let! first = enumerator.MoveNextAsync()
    Assert.True first
    Assert.Equal(2, enumerator.Current)
    release.TrySetResult(()) |> ignore
    let! second = enumerator.MoveNextAsync()
    Assert.True second
    Assert.Equal(1, enumerator.Current)
}

[<Fact>]
let ``takeUntil cancels a pending source move when the signal arrives`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let source = asyncSeq2 {
        yield 1
        do! Async2.Sleep 30000
        yield 2
    }
    let signal = Async2.Sleep 20
    let! values = Async2.StartAsTask(AsyncSeq2.toArray (AsyncSeq2.takeUntil signal source), cancellationToken = cts.Token)
    Assert.Equal<int>([| 1 |], values)
}

[<Fact>]
let ``skipUntil emits items observed after signal completion`` () =
    let trigger = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let source = asyncSeq2 {
        yield 1
        trigger.TrySetResult(()) |> ignore
        do! Async2.Sleep 10
        yield 2
        yield 3
    }
    let signal = async2 { do! trigger.Task }
    Assert.Equal<int>([| 2; 3 |], drain (AsyncSeq2.skipUntil signal source))

[<Fact>]
let ``time and count buffers flush trailing values`` () =
    let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let buffered = AsyncSeq2.bufferByCountAndTime 2 30 source |> drain
    Assert.Equal(2, buffered.Length)
    Assert.Equal<int>([| 1; 2 |], buffered[0])
    Assert.Equal<int>([| 3 |], buffered[1])
    Assert.Empty(drain (AsyncSeq2.bufferByTime 5 (AsyncSeq2.empty<int> ())))
    Assert.Throws<ArgumentException>(fun () -> AsyncSeq2.bufferByTime 0 source |> ignore) |> ignore

[<Fact>]
let ``time buffer emits values before a slow producer completes`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let source = asyncSeq2 {
        yield 1
        do! Async2.Sleep 80
        yield 2
    }
    let! arrays = Async2.StartAsTask(AsyncSeq2.toArray (AsyncSeq2.bufferByTime 20 source), cancellationToken = cts.Token)
    Assert.Equal(2, arrays.Length)
    Assert.Equal<int>([| 1 |], arrays[0])
    Assert.Equal<int>([| 2 |], arrays[1])
}

[<Fact>]
let ``buffered observable forwards items and completion`` () = task {
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds 3.)
    let mutable observer = None
    let mutable disposed = false
    let observable =
        { new IObservable<int> with
            member _.Subscribe(subscriber) =
                observer <- Some subscriber
                { new IDisposable with member _.Dispose() = disposed <- true } }
    let source = AsyncSeq2.ofObservableBuffered observable
    use enumerator = source.GetAsyncEnumerator(cts.Token)
    let pending = enumerator.MoveNextAsync().AsTask()
    let subscriber = Option.get observer
    subscriber.OnNext(1)
    subscriber.OnNext(2)
    subscriber.OnCompleted()
    let! first = pending
    Assert.True first
    Assert.Equal(1, enumerator.Current)
    let! second = enumerator.MoveNextAsync()
    Assert.True second
    Assert.Equal(2, enumerator.Current)
    let! last = enumerator.MoveNextAsync()
    Assert.False last
    do! enumerator.DisposeAsync()
    Assert.True disposed
}

[<Fact>]
let ``toObservable publishes values then completion`` () =
    use finished = new ManualResetEventSlim()
    let values = ResizeArray<int>()
    let mutable error = None
    let mutable completed = false
    let observer =
        { new IObserver<int> with
            member _.OnNext value = values.Add value
            member _.OnError exn = error <- Some exn; finished.Set()
            member _.OnCompleted() = completed <- true; finished.Set() }
    use _subscription =
        AsyncSeq2.ofSeq [ 1; 2; 3 ]
        |> AsyncSeq2.toObservable
        |> fun observable -> observable.Subscribe(observer)
    Assert.True(finished.Wait(TimeSpan.FromSeconds 3.))
    Assert.True completed
    Assert.Equal(None, error)
    Assert.Equal<int>([| 1; 2; 3 |], values.ToArray())

[<Fact>]
let ``iterator pulls one value at a time and disposes at the end`` () =
    let mutable disposed = false
    let source = asyncSeq2 {
        try
            yield 1
            yield 2
        finally
            disposed <- true
    }
    let next = AsyncSeq2.getIterator source
    Assert.Equal(Some 1, next () |> run)
    Assert.False disposed
    Assert.Equal(Some 2, next () |> run)
    Assert.Equal(None, next () |> run)
    Assert.True disposed

[<Fact>]
let ``cache shares lazy source evaluation across enumerations`` () =
    use cts = new CancellationTokenSource()
    let mutable reads = 0
    let source = asyncSeq2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        for number in 1..3 do
            reads <- reads + 1
            yield number
    }
    let cached = AsyncSeq2.cache source
    Assert.Equal(0, reads)
    let consume () =
        Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray cached, cancellationToken = cts.Token)
    Assert.Equal<int>([| 1; 2; 3 |], consume ())
    Assert.Equal<int>([| 1; 2; 3 |], consume ())
    Assert.Equal(3, reads)

[<Fact>]
let ``ported async callback receives enumerator cancellation token`` () =
    use cts = new CancellationTokenSource()
    let mapped =
        AsyncSeq2.ofSeq [ 1; 2 ]
        |> AsyncSeq2.mapiAsync (fun index value -> async2 {
            let! token = Async2.CancellationToken
            Assert.Equal(cts.Token, token)
            return index + value
        })
    let values = Async2.RunSynchronouslyImmediate(AsyncSeq2.toArray mapped, cancellationToken = cts.Token)
    Assert.Equal<int>([| 1; 3 |], values)

[<Fact>]
let ``ported terminals stay cold and forward the Async2 token`` () =
    use cts = new CancellationTokenSource()
    let seen = ResizeArray<CancellationToken>()
    let source = asyncSeq2 {
        let! token = Async2.CancellationToken
        seen.Add token
        yield 1
        yield 2
    }
    let maximum = AsyncSeq2.max source
    let groupKey value = async2 {
        let! token = Async2.CancellationToken
        seen.Add token
        return value % 2
    }
    let grouped = AsyncSeq2.groupByAsync groupKey source
    Assert.Empty seen
    Assert.Equal(2, Async2.RunSynchronouslyImmediate(maximum, cancellationToken = cts.Token))
    let groups = Async2.RunSynchronouslyImmediate(grouped, cancellationToken = cts.Token)
    Assert.Equal(2, groups.Length)
    Assert.All(seen, fun token -> Assert.Equal(cts.Token, token))

[<Fact>]
let ``async terminal searches use the ambient token and short circuit`` () =
    use cts = new CancellationTokenSource()
    let mutable calls = 0
    let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let check value = async2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        calls <- calls + 1
        return value = 2
    }
    let execute computation = Async2.RunSynchronouslyImmediate(computation, cancellationToken = cts.Token)
    let search = AsyncSeq2.tryFindAsync check source
    Assert.Equal(0, calls)
    Assert.Equal(Some 2, execute search)
    Assert.Equal(2, calls)
    Assert.Equal(2, execute (AsyncSeq2.findAsync check source))
    Assert.Equal(Some 1, execute (AsyncSeq2.tryFindIndexAsync check source))
    Assert.Equal(1, execute (AsyncSeq2.findIndexAsync check source))
    let choose value = async2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        return if value = 2 then Some(value * 10) else None
    }
    Assert.Equal(Some 20, execute (AsyncSeq2.tryPickAsync choose source))
    Assert.Equal(20, execute (AsyncSeq2.pickAsync choose source))
    Assert.Throws<KeyNotFoundException>(fun () ->
        AsyncSeq2.findAsync (fun _ -> async2 { return false }) source |> execute |> ignore) |> ignore

[<Fact>]
let ``async terminal aggregates run callbacks directly with the ambient token`` () =
    use cts = new CancellationTokenSource()
    let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
    let execute computation = Async2.RunSynchronouslyImmediate(computation, cancellationToken = cts.Token)
    let callback value = async2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        return value
    }
    let mutable indices = []
    Assert.Equal(3, execute (AsyncSeq2.maxByAsync callback source))
    Assert.Equal(1, execute (AsyncSeq2.minByAsync callback source))
    Assert.Equal(2, execute (AsyncSeq2.lengthByAsync (fun x -> callback (x % 2 = 1)) source))
    execute (AsyncSeq2.iteriAsync (fun i x -> async2 {
        let! _ = callback x
        indices <- (i, x) :: indices
    }) source)
    Assert.Equal<int * int>([| 0, 1; 1, 2; 2, 3 |], indices |> List.rev |> List.toArray)
    let folder state value = callback (state + value)
    Assert.Equal(6, execute (AsyncSeq2.foldAsync folder 0 source))
    Assert.Equal(6, execute (AsyncSeq2.reduceAsync folder source))
    Assert.Equal(3, execute (AsyncSeq2.foldWhileAsync
        (fun state _ -> callback (state < 3)) folder 0 source))
    let mapped, finalState =
        AsyncSeq2.mapFoldAsync (fun state value -> callback (state + value, state + value)) 0 source
        |> execute
    Assert.Equal<int>([| 1; 3; 6 |], mapped)
    Assert.Equal(6, finalState)
    Assert.Throws<ArgumentException>(fun () ->
        AsyncSeq2.reduceAsync folder (AsyncSeq2.empty<int> ()) |> execute |> ignore) |> ignore

[<Fact>]
let ``async terminal grouping and comparison retain ordering`` () =
    use cts = new CancellationTokenSource()
    let execute computation = Async2.RunSynchronouslyImmediate(computation, cancellationToken = cts.Token)
    let callback value = async2 {
        let! token = Async2.CancellationToken
        Assert.Equal(cts.Token, token)
        return value
    }
    let source = AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
    let group = callback << (fun x -> x % 2)
    let groups = execute (AsyncSeq2.groupByAsync group source)
    Assert.Equal<int>([| 1; 3 |], snd groups[0])
    Assert.Equal<int>([| 2; 4 |], snd groups[1])
    Assert.Equal<int * int>([| 1, 2; 0, 2 |], execute (AsyncSeq2.countByAsync group source))
    let yes, no = execute (AsyncSeq2.partitionAsync (fun x -> callback (x % 2 = 0)) source)
    Assert.Equal<int>([| 2; 4 |], yes)
    Assert.Equal<int>([| 1; 3 |], no)
    let compare a b = callback (Operators.compare a b)
    Assert.Equal(0, execute (AsyncSeq2.compareWithAsync compare source source))
    Assert.Equal(-1, execute (AsyncSeq2.compareWithAsync compare (AsyncSeq2.ofSeq [ 1 ]) source))
    Assert.Equal(1, execute (AsyncSeq2.compareWithAsync compare source (AsyncSeq2.ofSeq [ 1 ])))

[<Fact>]
let ``async terminal callback cancellation disposes the enumerator`` () = task {
    use cts = new CancellationTokenSource()
    let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let mutable disposed = false
    let source = asyncSeq2 {
        use resource = { new IDisposable with member _.Dispose() = disposed <- true }
        yield 1
        yield 2
    }
    let predicate _ = async2 {
        started.TrySetResult(()) |> ignore
        do! Async2.Sleep 30000
        return true
    }
    let running = Async2.StartAsTask(AsyncSeq2.lengthByAsync predicate source, cancellationToken = cts.Token)
    do! started.Task
    cts.Cancel()
    let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> running :> Task)
    Assert.Equal(cts.Token, error.CancellationToken)
    Assert.True(disposed)
}

[<Fact>]
let ``ported Async sources and channels honor enumeration cancellation`` () = task {
    use cts = new CancellationTokenSource()
    let builtIn = AsyncSeq2.ofAsyncSeq [ async { return! Async.CancellationToken } ]
    let! values = Async2.StartAsTask(AsyncSeq2.toArray builtIn, cancellationToken = cts.Token)
    Assert.Equal(cts.Token, values[0])
    let channel = Channel.CreateUnbounded<int>()
    let pending = Async2.StartAsTask(AsyncSeq2.toArray (AsyncSeq2.ofChannel channel.Reader), cancellationToken = cts.Token)
    cts.Cancel()
    let! error = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> pending :> Task)
    Assert.Equal(cts.Token, error.CancellationToken)
}

[<Fact>]
let ``broadcast source replays from subscription and closes`` () =
    for count in 0..100 do
        let source = AsyncSeq2Src.create<int> ()
        let first = AsyncSeq2Src.toAsyncSeq source
        AsyncSeq2Src.put -1 source
        let second = AsyncSeq2Src.toAsyncSeq source
        for value in 0..count - 1 do
            AsyncSeq2Src.put value source
        AsyncSeq2Src.close source
        Assert.Equal<int>(Array.append [| -1 |] [| 0..count - 1 |], drain first)
        Assert.Equal<int>([| 0..count - 1 |], drain second)
        Assert.Empty(drain (AsyncSeq2Src.toAsyncSeq source))

[<Fact>]
let ``broadcast source cancellation does not affect another subscriber`` () = task {
    let source = AsyncSeq2Src.create<int> ()
    let first = AsyncSeq2Src.toAsyncSeq source
    let second = AsyncSeq2Src.toAsyncSeq source
    use cts = new CancellationTokenSource()
    let cancelled = Async2.StartAsTask(AsyncSeq2.toArray first, cancellationToken = cts.Token)
    cts.Cancel()
    let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(fun () -> cancelled :> Task)
    AsyncSeq2Src.put 42 source
    AsyncSeq2Src.close source
    let! remaining = Async2.StartAsTask(AsyncSeq2.toArray second)
    Assert.Equal<int>([| 42 |], remaining)
}

[<Fact>]
let ``broadcast source forwards errors to existing subscribers`` () =
    let source = AsyncSeq2Src.create<int> ()
    let values = AsyncSeq2Src.toAsyncSeq source
    let failure = InvalidOperationException("source failed")
    AsyncSeq2Src.error failure source
    let raised = Assert.Throws<InvalidOperationException>(fun () -> drain values |> ignore)
    Assert.Same(failure, raised)

[<Fact>]
let ``cache replays its prefix after source failure`` () =
    let source = asyncSeq2 {
        yield 1
        failwith "source failed"
    }
    let cached = AsyncSeq2.cache source
    let raised = Assert.Throws<Exception>(fun () -> drain cached |> ignore)
    Assert.Equal("source failed", raised.Message)
    Assert.Equal<int>([| 1 |], drain (AsyncSeq2.take 1 cached))

[<Fact>]
let ``short circuit terminals dispose enumerators`` () =
    let checks =
        [ AsyncSeq2.exists ((=) 1)
          AsyncSeq2.existsAsync (fun _ -> async2 { return true })
          AsyncSeq2.forall (fun _ -> false)
          AsyncSeq2.forallAsync (fun _ -> async2 { return false })
          AsyncSeq2.contains 1 ]
    for check in checks do
        let mutable disposed = false
        let source = asyncSeq2 {
            try
                yield 1
                yield 2
            finally
                disposed <- true
        }
        check source |> run |> ignore
        Assert.True(disposed)

[<Fact>]
let ``ported terminal engines dispose on completion and exceptions`` () =
    let checks : (AsyncSeq2<int> -> Async2<unit>) list =
        [ fun source -> AsyncSeq2.tryHead source |> Async2.map ignore
          fun source -> AsyncSeq2.tryLast source |> Async2.map ignore
          fun source -> AsyncSeq2.tryExactlyOne source |> Async2.map ignore
          fun source -> AsyncSeq2.tryItem 0 source |> Async2.map ignore
          fun source -> AsyncSeq2.tryPick (fun value -> Some value) source |> Async2.map ignore
          fun source -> AsyncSeq2.tryFind ((=) 1) source |> Async2.map ignore
          fun source -> AsyncSeq2.tryFindIndex ((=) 1) source |> Async2.map ignore
          fun source -> AsyncSeq2.maxBy id source |> Async2.map ignore ]
    for check in checks do
        let mutable disposed = false
        let source = asyncSeq2 {
            try
                yield 1
                yield 2
            finally
                disposed <- true
        }
        check source |> run
        Assert.True(disposed)

    let mutable disposedOnError = false
    let source = asyncSeq2 {
        try yield 1
        finally disposedOnError <- true
    }
    Assert.Throws<Exception>(fun () ->
        AsyncSeq2.tryFind (fun _ -> failwith "predicate failed") source |> run |> ignore)
    |> ignore
    Assert.True(disposedOnError)

[<Fact>]
let ``ported tail and split operations dispose after their remainder`` () =
    let mutable tailDisposed = false
    let tailSource = asyncSeq2 {
        try yield! [ 1; 2; 3 ]
        finally tailDisposed <- true
    }
    let remainder = AsyncSeq2.tryTail tailSource |> run |> Option.get
    Assert.False(tailDisposed)
    Assert.Equal<int>([| 2; 3 |], drain remainder)
    Assert.True(tailDisposed)

    let mutable splitDisposed = false
    let splitSource = asyncSeq2 {
        try yield! [ 1; 2; 3 ]
        finally splitDisposed <- true
    }
    let prefix, rest = AsyncSeq2.splitAt 1 splitSource |> run
    Assert.Equal<int>([| 1 |], prefix)
    Assert.False(splitDisposed)
    Assert.Equal<int>([| 2; 3 |], drain rest)
    Assert.True(splitDisposed)
