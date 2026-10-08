# FSharp.Control.Async2

A separate asynchronous computation-expression library, named `Async2` and
`async2` so it can be developed independently without conflicting with
FSharp.Core's `Async` and `async`.

The implementation uses the preview runtime-async compiler feature.
`Async2<'T>` is a cold carrier: its runtime-async `Task<'T>` factory receives
the `CancellationToken` supplied when the computation is started. Both library
projects target `net11.0`; the test projects use xUnit v3 with the Microsoft
Testing Platform runner.

## Async2

The core library provides the `async2 { ... }` computation expression and
`Async2` operations for starting, composing, and awaiting asynchronous work.
The builder accepts `Async2`, `Task`, `ValueTask`, and awaitable sources;
`Async2.Sleep` and other cold operations observe the token used to start the
computation.

Cancellation is propagated between native computations as a struct
`Async2Result<'T>` rather than by repeatedly throwing. The token remains the
source of cancellation requests; the result records whether a computation
actually observed cancellation, so a successful resource acquisition still
enters its cleanup scope even if its token was canceled during the await.
Task and synchronous boundaries throw the cancellation exception; continuation
boundaries invoke the cancellation callback directly. Active `try/finally`
scopes and synchronous or asynchronous disposal still complete before their
cancellation propagates. `Parallel` and `Choice` rejoin started workers;
`and!` retains its left-failure short circuit without waiting for a right
source that was already started.

Actual faults retain exception-based propagation through an `ExceptionCache`
that weakly associates each exception with its first captured
`ExceptionDispatchInfo`, avoiding repeated accumulation of async stack traces.
Ambient cancellation thrown by an external await becomes a native cancellation
result; a canceled external task remains catchable when the ambient token is
live. Cancellation does not interrupt a non-cancellable external await or
insert checks into ordinary synchronous code. The struct avoids a separate
result-object allocation, but enlarges native task and state-machine payloads.

```fsharp
open System.Threading
open Microsoft.FSharp.Control

let computation =
    async2 {
        do! Async2.Sleep 100
        return 42
    }

use cancellation = new CancellationTokenSource()
let result =
    Async2.RunSynchronously(computation, cancellationToken = cancellation.Token)
```

`src/FSharp.Control.Async2` builds independently of the sequence library.
`MailboxProcessor2<'Msg>` is also part of this core library; see its section
below.

## Build and tests

The repository pins a .NET 11 preview SDK in `global.json` and uses a local F#
compiler build with runtime-async support. The root `Directory.Build.props`
selects `$(RuntimeAsyncBin)\fsc\Release\net11.0\fsc.dll` and references
`$(RuntimeAsyncBin)\FSharp.Core\Release\net10.0\FSharp.Core.dll`.
`RuntimeAsyncBin` defaults to `c:\dev\repos\fsharp\artifacts\bin`; override
`RuntimeAsyncBin` or set `RuntimeAsyncBitsPath` to use artifacts from another
local F# checkout.

Build the solution and run its tests with:

```sh
dotnet build
dotnet test
```

The focused `AsyncType`, `AsyncModule`, and `AsyncModuleFunctions` suites are accompanied by direct xUnit ports of older compiler-library regressions. RuntimeAsync compiler-component tests are intentionally outside this project.

## MailboxProcessor2

`MailboxProcessor2<'Msg>` is an `async2`-native mailbox agent with `Post`,
`Receive`, `TryReceive`, `Scan`, `TryScan`, reply channels, cancellation,
`Start`, and `StartImmediate`. Its name and `AsyncReplyChannel2<'Reply>` keep
it distinct from FSharp.Core's `MailboxProcessor` and `AsyncReplyChannel`.

Its mailbox semantics follow FSharp.Core: only one reader operation may run
at a time, asynchronous reply methods post their message when called, and
`Dispose` clears the mailbox without canceling its body. Supply a cancellation
token explicitly when cancellable infinite waits are required.

```fsharp
type Message = | Add of int | Read of AsyncReplyChannel2<int>

let counter =
    MailboxProcessor2<Message>.Start(fun inbox ->
        let rec loop total =
            async2 {
                match! inbox.Receive() with
                | Add value -> return! loop (total + value)
                | Read reply ->
                    reply.Reply total
                    return! loop total
            }
        loop 0)

counter.Post(Add 42)
let total =
    counter.PostAndAsyncReply((fun reply -> Read reply))
    |> Async2.RunSynchronously
```

## AsyncSeq2

`src/FSharp.Control.AsyncSeq2` remains a separate library referencing Async2,
so applications using only `async2` do not take on the sequence API or its
packaging dependencies. It provides
`asyncSeq2 { ... }` in `Microsoft.FSharp.Control`, yielding `AsyncSeq2<'T>` (an
`IAsyncEnumerable<'T>`). It supports `yield`, `yield!`, `for`, `let!`/`do!` on
tasks, value tasks, and `Async2`, plus synchronous or asynchronous `use`.
The builder is adapted from FSharp.Control.TaskSeq (see `src/FSharp.Control.AsyncSeq2/TASKSEQ-LICENSE.txt`).
Each enumeration is cold; `GetAsyncEnumerator(token)` passes its token to
bound `Async2` computations and nested asynchronous sequences. `async2` can
consume an `AsyncSeq2<'T>` directly with `for`.
`ofAsync2Seq`, `ofAsync2List`, and `ofAsync2Array` turn collections of cold
`Async2` computations into sequences; `ofSeqAsync` remains an equivalent
constructor for `seq<Async2<'T>>`.

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
operator, cleanup, composition, and cancellation tests. It also ports all 494
active tests from FSharp.Control.AsyncSeq's `AsyncSeqTests.fs`, adapting
`Async<'T>` to `Async2<'T>` and accounting for the existing TaskSeq-compatible
API (including strict `take`/`skip`, `truncate`/`drop`, array-valued groups,
and `removeManyAt` range behavior). `Async2.StartChild` starts its child as
soon as the parent creates it, so bounded channel producers and consumers
can run concurrently. Disposing an enumerator from `toBlockingSeq` cancels
its enumeration token.
`FSharp.Control.AsyncSeq2.TaskSeq.Tests` ports all 74 non-smoke TaskSeq test
files (including two upstream-skipped cases) to xUnit v3/MTP. Transformation
tests call `AsyncSeq2` directly with native `async2` callbacks; xUnit test
boundaries and explicit `Task` and `ValueTask` interoperability tests retain
those carriers. The old F# `Async<'T>` sources and conversion helpers have
been replaced by `Async2<'T>`; use `Async2` computations for cold, cancellable
operations.
