# FSharp.Control.Async2

A separate asynchronous computation expression and module, renamed to `Async2` and `async2` so it can be developed independently without conflicting with the built-in implementation.

The current implementation uses the preview runtime-async compiler feature. `Async2<'T>` is a named cold carrier whose start-time `CancellationToken` is passed to a runtime-async `Task<'T>` factory. The library targets `net11.0`; tests use xUnit v3 with the Microsoft Testing Platform runner.

The root `Directory.Build.props` selects the local F# compiler at `$(RuntimeAsyncBin)\fsc\Release\net11.0\fsc.dll` and references `$(RuntimeAsyncBin)\FSharp.Core\Release\net10.0\FSharp.Core.dll`, with `RuntimeAsyncBin` defaulting to `D:\source\fsharp\artifacts\bin`. Override `RuntimeAsyncBin` or provide `RuntimeAsyncBitsPath` when building against another local F# checkout.

The focused `AsyncType`, `AsyncModule`, and `AsyncModuleFunctions` suites are accompanied by direct xUnit ports of older compiler-library regressions. RuntimeAsync compiler-component tests are intentionally outside this project.

## AsyncSeq2

`src/FSharp.Control.AsyncSeq2` remains a separate library referencing Async2,
so applications using only `async2` do not take on the sequence API or its
packaging dependencies. It provides
`asyncSeq2 { ... }` in `Microsoft.FSharp.Control`, yielding `AsyncSeq2<'T>` (an
`IAsyncEnumerable<'T>`). It supports `yield`, `yield!`, `for`, `let!`/`do!` on
tasks, value tasks, `Async` and `Async2`, and synchronous or asynchronous `use`.
The builder is adapted from FSharp.Control.TaskSeq (see `src/FSharp.Control.AsyncSeq2/TASKSEQ-LICENSE.txt`).
Each enumeration is cold; `GetAsyncEnumerator(token)` passes its token to
bound `Async2` computations and nested asynchronous sequences. `async2` can
consume an `AsyncSeq2<'T>` directly with `for`.

`AsyncSeq2` provides constructors, transformations, and the TaskSeq operation
surface, as well as AsyncSeq-derived operations such as `cycle`, `rev`,
`splitInto`, `sortAsync`, `findBack`, `map2`, `allPairs`, and `unzip`. The
AsyncSeq-derived operators and the TaskSeq-compatible terminal operations use
`Async2` for asynchronous callbacks and cold terminal results; their
cancellation token flows implicitly to the enumerator. Existing `Task` and
`ValueTask` sources remain supported, but a previously started task cannot
retroactively inherit an enumeration token.
`AsyncSeq2Src` provides a broadcast source: `toAsyncSeq` subscribes at the
current tail, and `put`, `close`, or `error` notify existing subscribers.
See `ASYNCSEQ-LICENSE.txt` and `TASKSEQ-LICENSE.txt` for upstream attribution.

```fsharp
let values = asyncSeq2 {
    for i in 1..3 do
        do! Async2.Sleep 10
        yield i
}

let result = AsyncSeq2.toArray values |> Async2.RunSynchronously
```

`FSharp.Control.AsyncSeq2.Tests` contains xUnit v3/MTP builder, AsyncSeq
operator, cleanup, composition, and cancellation tests.
`FSharp.Control.AsyncSeq2.TaskSeq.Tests` ports all 74 non-smoke TaskSeq test
files (including two upstream-skipped cases) to xUnit v3/MTP.
