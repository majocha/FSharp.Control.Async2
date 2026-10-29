namespace FSharp.Control.Async2.Tests

open System
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Xunit
open Microsoft.FSharp.Control

/// Recursion that blocks on its own tasks (sync over async), ported from the IcedTasks `trampoline`
/// branch (`tests/IcedTasks.Tests/SyncOverAsyncTests.fs`). Each case completes when every start runs
/// inline. The start trampoline must not turn any of them into a deadlock.
module SyncOverAsyncTests =

    let largeStack = 64 * 1024 * 1024

    /// A small stack makes the trampoline bounce at the depths below.
    let smallStack = 512 * 1024

    let stackSize name =
        match name with
        | "large" -> largeStack
        | "small" -> smallStack
        | other -> invalidArg (nameof name) other

    /// Runs `body` on a dedicated thread and fails the test if it does not finish in time.
    /// A deadlocked thread is a background thread, so it does not stop the test run.
    let runGuarded stack (body: unit -> 'T) : 'T =
        let timeout = TimeSpan.FromSeconds 10.
        let mutable result = ValueNone

        let thread =
            Thread(
                (fun () ->
                    result <-
                        try
                            ValueSome(Ok(body ()))
                        with error ->
                            ValueSome(Error(ExceptionDispatchInfo.Capture error))),
                stack,
                IsBackground = true
            )

        thread.Start()

        if not (thread.Join timeout) then
            Assert.Fail $"Deadlock: the computation did not finish in {timeout}"

        match result with
        | ValueSome(Ok value) -> value
        | ValueSome(Error error) ->
            error.Throw()
            Unchecked.defaultof<_>
        | ValueNone -> failwith "The computation thread ended without a result"

    /// Starts a computation from user code and blocks on it.
    type Wait = Async2<int> -> int

    let startTask (computation: Async2<int>) = Async2.StartImmediateAsTask computation

    let waits: (string * Wait) list = [
        "Result", (fun c -> (startTask c).Result)
        "Wait",
        (fun c ->
            let t = startTask c
            t.Wait()
            t.Result)
        "GetAwaiter().GetResult()", (fun c -> (startTask c).GetAwaiter().GetResult())
        "Async.RunSynchronously", (fun c -> Async.RunSynchronously(Async.AwaitTask(startTask c)))
        "Async2.RunSynchronously", (fun c -> Async2.RunSynchronously c)
        "Async2.RunSynchronouslyImmediate", (fun c -> Async2.RunSynchronouslyImmediate c)
    ]

    let wait name = waits |> List.find (fst >> (=) name) |> snd

    let depths = [ 100; 1_000 ]

    let stacks = [ "large"; "small" ]

    let waitCases stackNames : seq<obj array> =
        seq {
            for stack in stackNames do
                for waitName, _ in waits do
                    for depth in depths do
                        yield [| box stack; box waitName; box depth |]
        }

    let stackCases stackNames : seq<obj array> =
        seq {
            for stack in stackNames do
                for depth in depths do
                    yield [| box stack; box depth |]
        }

    /// Async2 has no thread hop: like FSharp.Core async, it stays on the current thread, and a start from user
    /// code runs on the current stack. So recursion through blocking starts overflows a small stack at depth
    /// 1 000, with or without the trampoline. A stack overflow ends the test process, so these cases are skipped
    /// instead of run. Async2.RunSynchronously moves off a thread that is not a pool thread, so it recurses on a
    /// pool thread's stack whatever the stack of the test thread, and that one overflows at depth 1 000 in Debug.
    let skipIfOverflows stack waitName depth =
        if depth >= 1_000 && (stack = "small" || waitName = "Async2.RunSynchronously") then
            Assert.Skip "Overflows the stack: blocking starts from user code run on the current stack (no thread hop)."

    /// A SynchronizationContext that drops every Post, like a UI thread that is blocked on the outer task.
    type BlockedContext() =
        inherit SynchronizationContext()
        override _.Post(_, _) = ()
        override _.Send(callback, state) = callback.Invoke state
        override this.CreateCopy() = this

    let blockUnderContext stack (start: unit -> Task<int>) =
        runGuarded stack (fun () ->
            let previous = SynchronizationContext.Current
            SynchronizationContext.SetSynchronizationContext(BlockedContext())

            try
                (start ()).GetAwaiter().GetResult()
            finally
                SynchronizationContext.SetSynchronizationContext previous)

    /// Every level is let! bound: the trampoline can bounce here.
    let rec bound n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! x = bound (n - 1)
                return x + 1
        }

    /// Every level starts the child from user code and blocks on it.
    let rec blocking (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                return wait (blocking wait (n - 1)) + 1
        }

    /// Odd levels block, even levels let! bind. A bounced child can end up behind a blocked frame.
    let rec mixed (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            elif n % 2 = 0 then
                let! x = mixed wait (n - 1)
                return x + 1
            else
                return wait (mixed wait (n - 1)) + 1
        }

    // The plain functions below return ValueTask: `let!` of a `unit -> Task<'T>` does not compile,
    // because Task<'T> has two GetAwaiter overloads for the awaitable source.

    /// The let! bound function is not an Async2: it blocks on one inside.
    let rec wrapped (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! x = fun () -> ValueTask<int>(wait (wrapped wait (n - 1)))
                return x + 1
        }

    /// Like wrapped, with a function that takes the cancellation token.
    let rec wrappedCancellable (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! x = fun (_: CancellationToken) -> ValueTask<int>(wait (wrappedCancellable wait (n - 1)))
                return x + 1
        }

    /// A let! of a plain function, then a blocking start from user code in the same frame.
    let rec afterPlainBind (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! zero = fun () -> ValueTask<int> 0
                return wait (afterPlainBind wait (n - 1)) + zero + 1
        }

    /// Mutual recursion with F# async: let! on the async2 side, blocking on the async side.
    let rec mutualAsync (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! x = mutualAsyncBlocking wait (n - 1)
                return x + 1
        }

    and mutualAsyncBlocking (wait: Wait) n : Async<int> =
        async {
            if n = 0 then
                return 0
            else
                return wait (mutualAsync wait (n - 1)) + 1
        }

    /// Mutual recursion with task: let! on the async2 side, blocking on the task side.
    let rec mutualTask (wait: Wait) n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let! x = mutualTaskBlocking wait (n - 1)
                return x + 1
        }

    and mutualTaskBlocking (wait: Wait) n : Task<int> =
        task {
            if n = 0 then
                return 0
            else
                return wait (mutualTask wait (n - 1)) + 1
        }

    /// Completing a TaskCompletionSource resumes an awaiting computation inline. It let! binds
    /// deep recursion, then the completing frame blocks on it.
    let resumeInlineThenBlock depth : Async2<int> =
        let source = TaskCompletionSource<int>()

        let waiter =
            async2 {
                let! v = source.Task
                let! r = bound depth
                return r + v
            }

        async2 {
            let started = startTask waiter
            let! x = bound depth
            source.SetResult 1
            return started.GetAwaiter().GetResult() + x
        }

    /// Like resumeInlineThenBlock, but a let! starts the computation that the source resumes. Such a
    /// computation can also bounce before it sets its result.
    let resumeInlineNestedThenBlock depth : Async2<int> =
        let source = TaskCompletionSource<int>()

        let resumed =
            async2 {
                let! v = source.Task
                let! r = bound depth
                return r + v
            }

        let waiter =
            async2 {
                let! r = resumed
                return r
            }

        async2 {
            let started = startTask waiter
            let! x = bound depth
            source.SetResult 1
            return started.GetAwaiter().GetResult() + x
        }

    let private local = AsyncLocal<int>()

    /// Each level checks the value its parent set, sets its own, binds its child, and checks its own value
    /// again. Returns the number of wrong values seen.
    let rec flowing n : Async2<int> =
        async2 {
            if n = 0 then
                return 0
            else
                let parent = local.Value
                let wrongAtStart = if parent <> 0 && parent <> n + 1 then 1 else 0
                local.Value <- n
                let! wrong = flowing (n - 1)
                let wrongAfter = if local.Value <> n then 1 else 0
                local.Value <- parent
                return wrong + wrongAtStart + wrongAfter
        }

    /// A deep chain suspends on a TaskCompletionSource at its bottom. Completing it resumes the whole chain.
    let rec deepResumedBy (source: TaskCompletionSource<int>) n : Async2<int> =
        async2 {
            if n = 0 then
                return! source.Task
            else
                let! x = deepResumedBy source (n - 1)
                return x + 1
        }

    let checkWaitCase (case: Wait -> int -> Async2<int>) stack waitName depth =
        skipIfOverflows stack waitName depth
        let wait = wait waitName
        Assert.Equal(depth, runGuarded (stackSize stack) (fun () -> wait (case wait depth)))

    let allWaitCases = waitCases stacks
    let allStackCases = stackCases stacks
    let largeStackCases = stackCases [ "large" ]

    [<Theory; MemberData(nameof allStackCases)>]
    let ``let! recursion`` (stack: string, depth: int) =
        Assert.Equal(depth, runGuarded (stackSize stack) (fun () -> (startTask (bound depth)).GetAwaiter().GetResult()))

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``Block on a user started child`` (stack: string, waitName: string, depth: int) =
        checkWaitCase blocking stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``A - Block and let! on alternate levels`` (stack: string, waitName: string, depth: int) =
        checkWaitCase mixed stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``B - let! a plain function that blocks`` (stack: string, waitName: string, depth: int) =
        checkWaitCase wrapped stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``B - let! a plain cancellable function that blocks`` (stack: string, waitName: string, depth: int) =
        checkWaitCase wrappedCancellable stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``C - Block after a let! of a plain function`` (stack: string, waitName: string, depth: int) =
        checkWaitCase afterPlainBind stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``D - Mutual recursion with async, blocking on the async side`` (stack: string, waitName: string, depth: int) =
        checkWaitCase mutualAsync stack waitName depth

    [<Theory; MemberData(nameof allWaitCases)>]
    let ``D - Mutual recursion with task, blocking on the task side`` (stack: string, waitName: string, depth: int) =
        checkWaitCase mutualTask stack waitName depth

    /// The BlockedContext cases run only on the large stack: under a SynchronizationContext nested starts never bounce, so they overflow the small one, as without the trampoline.
    [<Theory; MemberData(nameof largeStackCases)>]
    let ``E - let! recursion under a blocked SynchronizationContext`` (stack: string, depth: int) =
        Assert.Equal(depth, blockUnderContext (stackSize stack) (fun () -> startTask (bound depth)))

    [<Theory; MemberData(nameof largeStackCases)>]
    let ``E - Block and let! under a blocked SynchronizationContext`` (stack: string, depth: int) =
        let wait = wait "GetAwaiter().GetResult()"
        Assert.Equal(depth, blockUnderContext (stackSize stack) (fun () -> startTask (mixed wait depth)))

    [<Theory; MemberData(nameof allStackCases)>]
    let ``F - Inline resume from a TaskCompletionSource, then block`` (stack: string, depth: int) =
        let actual = runGuarded (stackSize stack) (fun () -> (startTask (resumeInlineThenBlock depth)).GetAwaiter().GetResult())
        Assert.Equal(2 * depth + 1, actual)

    [<Theory; MemberData(nameof allStackCases)>]
    let ``F - Inline resume of a let! started computation, then block`` (stack: string, depth: int) =
        let actual = runGuarded (stackSize stack) (fun () -> (startTask (resumeInlineNestedThenBlock depth)).GetAwaiter().GetResult())
        Assert.Equal(2 * depth + 1, actual)

    /// Deeper than the small stack holds: the waiter's let! bounces because the stack runs low. The waiter's start
    /// owns its own trampoline, so it drains the bounce before SetResult returns.
    [<Theory>]
    [<InlineData(5_000)>]
    let ``F - Inline resume from a TaskCompletionSource, then block, deeper than the stack`` (depth: int) =
        let actual = runGuarded smallStack (fun () -> (startTask (resumeInlineThenBlock depth)).GetAwaiter().GetResult())
        Assert.Equal(2 * depth + 1, actual)

    [<Theory; MemberData(nameof allStackCases)>]
    let ``H - AsyncLocal values flow across bounces`` (stack: string, depth: int) =
        Assert.Equal(0, runGuarded (stackSize stack) (fun () -> (startTask (flowing depth)).GetAwaiter().GetResult()))

    [<Theory; MemberData(nameof allStackCases)>]
    let ``H - A deep chain resumed by a TaskCompletionSource from another thread`` (stack: string, depth: int) =
        let actual =
            runGuarded (stackSize stack) (fun () ->
                let source = TaskCompletionSource<int>()
                let running = startTask (deepResumedBy source (depth * 10))
                Task.Run(fun () -> source.SetResult 0).Wait()
                running.GetAwaiter().GetResult())

        Assert.Equal(depth * 10, actual)
