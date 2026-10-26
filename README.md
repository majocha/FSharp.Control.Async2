# FSharp.Control.Async2

A reimplementation of F# `async` on .NET runtime async. It is named `Async2` /
`async2` so it can live next to FSharp.Core's `Async` / `async`, and it follows
their API and semantics unless listed below.

- **FSharp.Control.Async2**: `async2`, `Async2` and `MailboxProcessor2`.
- **FSharp.Control.AsyncSeq2**: `asyncSeq2` and the `AsyncSeq2` operations.

Experimental: it needs a preview F# compiler with runtime-async support (see
[Building](#building)).

## Differences from `async`

- `async2` binds `Task`, `ValueTask` and any awaitable directly, as well as F#
  `Async`. No `Async.AwaitTask` needed.
- `let! ct = CurrentCancellationToken` reads the token without starting a
  computation. `Async2.CancellationToken` also works but costs a small start.
- A tail-position `return!` hands over to its callee and the caller's frame
  finishes, so the caller does not appear in stack traces. The callee still
  starts in the caller's `ExecutionContext` and sees its `AsyncLocal` values.
- Extra operations: `Async2.RunSynchronouslyImmediate`, `Async2.Await` and
  `Async2.StartTaskImmediate` for tasks and awaitables, and the module
  functions `Async2.map`, `bind`, `catch`, `catchWith`, `parallelLimit` and
  friends.

`MailboxProcessor2<'Msg>` is FSharp.Core's `MailboxProcessor` with an `async2`
body and `AsyncReplyChannel2<'Reply>` replies.

## AsyncSeq2

`asyncSeq2 { ... }` builds an `AsyncSeq2<'T>`, which is `IAsyncEnumerable<'T>`.
The token passed to `GetAsyncEnumerator` flows into every `Async2` and nested
sequence it binds. Terminal operations return `Async2`.

```fsharp
let numbers =
    asyncSeq2 {
        for i in 1 .. 3 do
            do! Async2.Sleep 10
            yield i
    }

let result = AsyncSeq2.toArray numbers |> Async2.RunSynchronously
```

`AsyncSeq2` combines the operations of
[FSharp.Control.AsyncSeq](https://github.com/fsprojects/FSharp.Control.AsyncSeq)
and [FSharp.Control.TaskSeq](https://github.com/fsprojects/FSharp.Control.TaskSeq).
Coming from AsyncSeq:

- `Async<'T>` becomes `Async2<'T>`.
- Signatures follow TaskSeq: counts and indexes are `int`, not `int64`, and
  `groupBy`, `chunkBy` and similar return arrays.
- Behaviour follows AsyncSeq: `take` and `skip` stop quietly at the end,
  `tail` of an empty sequence is empty, and `head`, `last`, `reduce`, `min`,
  `max` and friends raise `InvalidOperationException` on an empty sequence.
- `prefetch` takes no count; it reads one element ahead.

The `asyncSeq2` builder is adapted from TaskSeq. See `ASYNCSEQ-LICENSE.txt` and
`TASKSEQ-LICENSE.txt` in `src/FSharp.Control.AsyncSeq2`.

## Building

`global.json` pins a .NET 11 preview SDK. `Directory.Build.props` takes the F#
compiler and FSharp.Core from `RuntimeAsyncBin`, which defaults to
`c:\dev\repos\fsharp\artifacts\bin`. Set `RuntimeAsyncBin` (or
`RuntimeAsyncBitsPath`) to use another F# checkout.

```sh
dotnet build
dotnet test
```

The tests include ports of FSharp.Core's async tests, side-by-side checks
against FSharp.Core's `MailboxProcessor`, AsyncSeq's 494 tests and TaskSeq's 74
test files, each adjusted for the differences above.

## Benchmarks

`benchmarks/FSharp.Control.Async2.Benchmarks` has two kinds of benchmark.

`RecursiveComputationBenchmarks` measures `async2` alone on eight workloads
(recursion, cancellation, exceptions, `try/with` and `try/finally` at every
level) at depths 16, 256 and 2048, awaiting `Task.Yield()` every 16 levels.

The scenario benchmarks write one application workload three times, with
`async` (the baseline), `async2` and `task`, each as it is usually written in
that flavour. Remote calls are `Task`s that resume on the thread pool, and
cache hits are synchronously completed `ValueTask`s.

| Benchmark | Workload |
| --- | --- |
| `RequestPipeline` | A Giraffe-style middleware stack (`try/with`, `try/finally`, auth) over a read-through cache, at 100%, 90% and 0% cache hits |
| `ScatterGather` | One page calling 4 or 32 services at once (`Parallel` / `Task.WhenAll`) |
| `ThrottledUpload` | 256 uploads, at most 8 in flight (`maxDegreeOfParallelism` / `Parallel.ForEachAsync`) |
| `TreeCrawl` | Recursive parallel crawl of a folder tree of fan-out 4 |
| `ChannelPipeline` | Three concurrent stages over bounded `System.Threading.Channels` of capacity 1 and 100 |
| `StreamParsing` | Counting lines of a 1 MB log read from a socket-like stream in 4 KB reads |
| `Mailbox` | A metrics agent: a burst of posts, and sequential request/reply (`MailboxProcessor`, `MailboxProcessor2`, a channel-based task agent) |
| `Retry` | A retry policy around an upstream that times out 0 or 2 times per call |
| `Pagination` | A paginated API as an async sequence: filter, map through a cache, fold (AsyncSeq, AsyncSeq2, TaskSeq) |

```sh
dotnet run -c Release --project benchmarks/FSharp.Control.Async2.Benchmarks/FSharp.Control.Async2.Benchmarks.fsproj
```

Pass BenchmarkDotNet options after `--`, such as `-- --filter *DeepException*`.
`-- --filter "*" --job Dry` runs every case once as a quick smoke test.
