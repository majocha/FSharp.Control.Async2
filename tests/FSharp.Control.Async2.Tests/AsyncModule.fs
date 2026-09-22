// Copyright (c) Microsoft Corporation.  All Rights Reserved.  See License.txt in the project root for license information.

// Various tests for the:
// Microsoft.FSharp.Control.Async2 module

namespace FSharp.Core.UnitTests.Control

open System
open Microsoft.FSharp.Control
open System.Threading
open System.Threading.Tasks
open FSharp.Core.UnitTests.LibraryTestFx
open Xunit

#nowarn "3397" // This expression uses 'unit' for an 'obj'-typed argument. This will lead to passing 'null' at runtime.
// Why warned - the tests here are actually trying to assert that Async2<unit> still works.

module Utils =
    let internal memoizeAsync f =
        let cache = System.Collections.Concurrent.ConcurrentDictionary<'a, System.Threading.Tasks.Task<'b>>()
        fun (x: 'a) -> // task.Result serialization to sync after done.
            cache.GetOrAdd(x, fun x -> f(x) |> Async2.StartAsTask) |> Async2.AwaitTask

type [<Struct>] Dummy (x: int) =
  member _.X = x
  interface IDisposable with
    member _.Dispose () = ()


[<AutoOpen>]
module ChoiceUtils =

    // FsCheck driven Async2.Choice specification test

    exception ChoiceExn of index:int

    /// represents a child computation of a choice workflow
    type ChoiceOp =
        | NoneResultAfter of timeout:int
        | SomeResultAfter of timeout:int
        | ExceptionAfter of timeout:int

        member c.Timeout =
            match c with
            | NoneResultAfter t -> t
            | SomeResultAfter t -> t
            | ExceptionAfter t -> t

    /// represent a choice workflow
    type ChoiceWorkflow = ChoiceWorkflow of children:ChoiceOp list * cancelAfter:int option

    /// normalizes random timeout arguments
    let normalize (ChoiceWorkflow(ops, cancelAfter)) =
        let ms t = 2000 * (abs t % 15) // timeouts only positive multiples of 2 seconds, up to 30 seconds
        let mkOp op =
            match op with
            | NoneResultAfter t -> NoneResultAfter (ms t)
            | SomeResultAfter t -> SomeResultAfter (ms t)
            | ExceptionAfter t -> ExceptionAfter (ms t)

        let ops = ops |> List.map mkOp
        let cancelAfter = cancelAfter |> Option.map ms
        ChoiceWorkflow(ops, cancelAfter)

    /// runs specified choice workflow and checks that
    /// Async2.Choice spec is satisfied
    let runChoice (ChoiceWorkflow(ops, cancelAfter)) =
        // Step 1. build a choice workflow from the abstract representation
        let completed = ref 0
        let returnAfter (time: int) f = async2 {
            do! Async2.Sleep time
            let _ = Interlocked.Increment completed
            return f ()
        }

        let mkOp (index : int) = function
            | NoneResultAfter t -> returnAfter t (fun () ->  None)
            | SomeResultAfter t -> returnAfter t (fun () -> Some index)
            | ExceptionAfter t -> returnAfter t (fun () -> raise (ChoiceExn index))

        let choiceWorkflow = ops |> List.mapi mkOp |> Async2.Choice

        // Step 2. run the choice workflow and keep the results
        let result =
            let cancellationToken =
                match cancelAfter with
                | Some ca ->
                    let cts = new CancellationTokenSource()
                    cts.CancelAfter(ca)
                    Some cts.Token
                | None -> None

            try Async2.RunSynchronously(choiceWorkflow, ?cancellationToken = cancellationToken) |> Choice1Of2
            with e -> Choice2Of2 e

        // Step 3. check that results are up to spec
        let getMinTime() =
            seq {
                yield Int32.MaxValue // "infinity": avoid exceptions if list is empty

                for op in ops do
                    match op with
                    | NoneResultAfter _ -> ()
                    | op -> yield op.Timeout

                match cancelAfter with Some t -> yield t | None -> ()
            } |> Seq.min

        let verifyIndex index =
            if index < 0 || index >= ops.Length then
                Assert.Fail "Returned choice index is out of bounds."

        // Step 3a. check that output is up to spec
        match result with
        | Choice1Of2 (Some index) ->
            verifyIndex index
            match ops.[index] with
            | SomeResultAfter timeout -> Assert.Equal<int>(getMinTime(), timeout)
            | op -> Assert.True(false, sprintf "Should be 'Some' but got %A" op)

        | Choice1Of2 None ->
            Assert.True(ops |> List.forall (function NoneResultAfter _ -> true | _ -> false))

        | Choice2Of2 (:? OperationCanceledException) ->
            match cancelAfter with
            | None -> Assert.Fail "Got unexpected cancellation exception."
            | Some ca -> Assert.Equal(getMinTime(), ca)

        | Choice2Of2 (ChoiceExn index) ->
            verifyIndex index
            match ops.[index] with
            | ExceptionAfter timeout -> Assert.Equal<int>(getMinTime(), timeout)
            | op -> Assert.True(false, sprintf "Should be 'Exception' but got %A" op)

        | Choice2Of2 e -> Assert.Fail(sprintf "Unexpected exception %O" e)

        // Step 3b. check that nested cancellation happens as expected
        if not <| List.isEmpty ops then
            let minTimeout = getMinTime()
            let minTimeoutOps = ops |> Seq.filter (fun op -> op.Timeout <= minTimeout) |> Seq.length
            Assert.True(completed.Value <= minTimeoutOps)

module LeakUtils =
    // when testing for liveness, the things that we want to observe must always be created in
    // a nested function call to avoid the GC (possibly) treating them as roots past the last use in the block.
    // We also need something non trivial to dissuade the compiler from inlining in Release builds.
    type ToRun<'a>(f : unit -> 'a) =
        member _.Invoke() = f()

    let run (toRun : ToRun<'a>) = toRun.Invoke()

// ---------------------------------------------------

[<Collection(nameof FSharp.Test.NotThreadSafeResourceCollection)>]
type AsyncModule() =

    /// Simple asynchronous task that delays 200ms and returns a list of the current tick count
    let getTicksTask =
        async2 {
            do! Async2.SwitchToThreadPool()
            let mutable tickstamps = [] // like timestamps but for ticks :)

            for i = 1 to 10 do
                tickstamps <- DateTime.UtcNow.Ticks :: tickstamps
                do! Async2.Sleep(20)

            return tickstamps
        }

    let wait (wh : System.Threading.WaitHandle) (timeoutMilliseconds : int) =
        wh.WaitOne(timeoutMilliseconds, exitContext=false)

    let dispose(d : #IDisposable) = d.Dispose()

    let testErrorAndCancelRace testCaseName computation =
        for i in 1..20 do
            let cts = new System.Threading.CancellationTokenSource()
            use barrier = new System.Threading.ManualResetEvent(false)
            async2 { cts.Cancel() }
            |> Async2.Start

            let c = ref 0
            let incr () = System.Threading.Interlocked.Increment(c) |> ignore

            Async2.StartWithContinuations(
                computation,
                (fun _ -> failwith (sprintf "Testcase: %s  --- success not expected iterations 1 .. 20 - failed on iteration %d" testCaseName i)),
                (fun _ -> incr()),
                (fun _ -> incr()),
                cts.Token
            )

            wait barrier 100
            |> ignore
            if c.Value = 2 then Assert.Fail("both error and cancel continuations were called")

    [<Fact>]
    member _.AwaitIAsyncResult() =

        let beginOp, endOp, cancelOp = Async2.AsBeginEnd(fun() -> getTicksTask)

        // Begin the async2 operation and wait
        let operationIAR = beginOp ((), new AsyncCallback(fun iar -> ()), null)
        match Async2.AwaitIAsyncResult(operationIAR) |> Async2.RunSynchronously with
        | true  -> ()
        | false -> Assert.Fail("Timed out. Expected to succeed.")

        // When the operation has already completed
        let operationIAR = beginOp ((), new AsyncCallback(fun iar -> ()), null)
        sleep(250)

        let result = Async2.AwaitIAsyncResult(operationIAR) |> Async2.RunSynchronously
        match result with
        | true  -> ()
        | false -> Assert.Fail("Timed out. Expected to succeed.")

        // Now with a timeout
        let operationIAR = beginOp ((), new AsyncCallback(fun iar -> ()), null)
        let result = Async2.AwaitIAsyncResult(operationIAR, 1) |> Async2.RunSynchronously
        match result with
        | true  -> Assert.Fail("Timeout expected")
        | false -> ()

    [<Fact(Skip = "Flaky")>]
    member _.``AwaitWaitHandle.Timeout``() =
        use waitHandle = new System.Threading.ManualResetEvent(false)
        let startTime = DateTime.UtcNow

        let r =
            Async2.AwaitWaitHandle(waitHandle, 500)
            |> Async2.RunSynchronously

        Assert.False(r, "Timeout expected")

        let endTime = DateTime.UtcNow
        let delta = endTime - startTime
        Assert.True(delta.TotalMilliseconds < 1100.0, sprintf "Expected faster timeout than %.0f ms" delta.TotalMilliseconds)

    [<Fact>]
    member _.``AwaitWaitHandle.TimeoutWithCancellation``() =
        use barrier = new System.Threading.ManualResetEvent(false)
        use waitHandle = new System.Threading.ManualResetEvent(false)
        let cts = new System.Threading.CancellationTokenSource()

        Async2.AwaitWaitHandle(waitHandle, 5000)
        |> Async2.Ignore
        |> fun c ->
                    Async2.StartWithContinuations(
                        c,
                        (failwithf "Unexpected success %A"),
                        (failwithf "Unexpected error %A"),
                        (fun _ -> barrier.Set() |> ignore),
                        cts.Token
                    )

        // wait a bit then signal cancellation
        let timeout = wait barrier 500
        Assert.False(timeout, "timeout=true is not expected")

        cts.Cancel()

        // wait 10 seconds for completion
        let ok = wait barrier 10000
        if not ok then Assert.Fail("Async2 computation was not completed in given time")

    [<Fact>]
    member _.``AwaitWaitHandle.DisposedWaitHandle1``() =
        let wh = new System.Threading.ManualResetEvent(false)

        dispose wh
        let test = async2 {
            try
                let! timeout = Async2.AwaitWaitHandle wh
                Assert.Fail(sprintf "Unexpected success %A" timeout)
            with
                | :? ObjectDisposedException -> ()
                | e -> Assert.Fail(sprintf "Unexpected error %A" e)
            }
        Async2.RunSynchronously test

    [<Fact(Skip="test is flaky: https://github.com/dotnet/fsharp/issues/11586")>]
    member _.``OnCancel.RaceBetweenCancellationHandlerAndDisposingHandlerRegistration``() =
        let test() =
            use flag = new ManualResetEvent(false)
            use cancelHandlerRegistered = new ManualResetEvent(false)
            let cts = new System.Threading.CancellationTokenSource()
            let go = async2 {
                use! holder = Async2.OnCancel(fun() -> lock flag (fun() -> flag.Set()) |> ignore)
                let _ = cancelHandlerRegistered.Set()
                while true do
                    do! Async2.Sleep 50
                }

            Async2.Start (go, cancellationToken = cts.Token)
            //wait until we are sure the Async2.OnCancel has run:
            Assert.True(cancelHandlerRegistered.WaitOne(TimeSpan.FromSeconds 5.))
            //now cancel:
            cts.Cancel()
            //cancel handler should have run:
            Assert.True(flag.WaitOne(TimeSpan.FromSeconds 5.))

        for _i = 1 to 300 do test()

    [<Fact(Skip="test is flaky: https://github.com/dotnet/fsharp/issues/11586")>]
    member _.``OnCancel.RaceBetweenCancellationAndDispose``() =
        let mutable flag = 0
        let cts = new System.Threading.CancellationTokenSource()
        let go = async2 {
            use disp =
                cts.Cancel()
                { new IDisposable with
                    override _.Dispose() = flag <- flag + 1}
            while true do
                do! Async2.Sleep 50
            }
        try
            Async2.RunSynchronously (go, cancellationToken = cts.Token)
        with
            :? System.OperationCanceledException -> ()
        Assert.Equal(1, flag)

    [<Fact(Skip="test is flaky: https://github.com/dotnet/fsharp/issues/11586")>]
    member _.``OnCancel.CancelThatWasSignalledBeforeRunningTheComputation``() =
        let test() =
            let cts = new System.Threading.CancellationTokenSource()
            let go e (flag : bool ref) = async2 {
                let! _ = Async2.AwaitWaitHandle e
                use! _holder = Async2.OnCancel(fun () -> flag.Value <- true)
                while true do
                    do! Async2.Sleep 100
                }

            let evt = new System.Threading.ManualResetEvent(false)
            let finish = new System.Threading.ManualResetEvent(false)
            let cancelledWasCalled = ref false
            Async2.StartWithContinuations(go evt cancelledWasCalled, ignore, ignore, (fun _ -> finish.Set() |> ignore),  cancellationToken = cts.Token)
            evt.Set() |> ignore
            cts.Cancel()

            let ok = wait finish 3000
            Assert.True(ok, "Computation should be completed")
            Assert.False(cancelledWasCalled.Value, "Cancellation handler should not be called")

        for _i = 1 to 3 do test()

#if EXPENSIVE
    [<Test; Category("Expensive"); Explicit>]
    member _.``Async2.AwaitWaitHandle does not leak memory`` () =
        // This test checks that AwaitWaitHandle does not leak continuations (described in #131),
        // We only test the worst case - when the AwaitWaitHandle is already set.
        use manualResetEvent = new System.Threading.ManualResetEvent(true)
        
        let tryToLeak() = 
            let resource = 
                LeakUtils.ToRun (fun () ->
                    let resource = obj()
                    let work = 
                        async2 { 
                            let! _ = Async2.AwaitWaitHandle manualResetEvent
                            GC.KeepAlive(resource)
                            return ()
                        }

                    work |> Async2.RunSynchronously |> ignore
                    WeakReference(resource))
                  |> LeakUtils.run

            Assert.True(resource.IsAlive)

            GC.Collect()
            GC.WaitForPendingFinalizers()
            GC.Collect()
            GC.WaitForPendingFinalizers()
            GC.Collect()

            Assert.False(resource.IsAlive)
        
        // The leak hangs on a race condition which is really hard to trigger in F# 3.0, hence the 100000 runs...
        for _ in 1..10 do tryToLeak()
#endif

    [<Fact>]
    member _.``AwaitWaitHandle.DisposedWaitHandle2``() =
        let wh = new ManualResetEvent(false)
        let started = new ManualResetEventSlim(false)
        let cts = new CancellationTokenSource()
        let test =
            Async2.StartAsTask( async2 {
                printfn "starting the test"
                started.Set()
                let! _ = Async2.AwaitWaitHandle(wh)
                printfn "should never get here"
            }, cancellationToken = cts.Token)

        // Wait for the test to start then dispose waithandle - nothing should happen.
        started.Wait()
        Assert.False(test.Wait 1000, "Test completed too early.")
        printfn "disposing"
        dispose wh
        printfn "cancelling in 1 second"
        cts.CancelAfter 1000
        Assert.ThrowsAsync<TaskCanceledException>(fun () -> test)

    [<Fact>]
    member _.``RunSynchronously.NoThreadJumpsAndTimeout``() =
            let longRunningTask = async2 { sleep(5000) }
            try
                Async2.RunSynchronously(longRunningTask, timeout = 500)
                Assert.Fail("TimeoutException expected")
            with
                :? System.TimeoutException -> ()

    [<Fact>]
    member _.``RunSynchronously.NoThreadJumpsAndTimeout.DifferentSyncContexts``() =
        let run syncContext =
            let old = SynchronizationContext.Current
            SynchronizationContext.SetSynchronizationContext(syncContext)
            let longRunningTask = async2 { sleep(5000) }
            let mutable failed = false
            try
                Async2.RunSynchronously(longRunningTask, timeout = 500)
                failed <- true
            with
                :? System.TimeoutException -> ()
            SynchronizationContext.SetSynchronizationContext(old)
            if failed then Assert.Fail("TimeoutException expected")
        run null
        run (System.Threading.SynchronizationContext())

    [<Fact>]
    // See https://github.com/dotnet/fsharp/issues/12637#issuecomment-1020199383
    member _.``RunSynchronously.ThreadJump.IfSyncCtxtNonNull``() =
        async2 {
            do! Async2.SwitchToThreadPool()
            let old = SynchronizationContext.Current
            SynchronizationContext.SetSynchronizationContext(SynchronizationContext())
            Assert.NotNull(SynchronizationContext.Current)
            Assert.True(Thread.CurrentThread.IsThreadPoolThread)
            let computation =
                async2 {
                    let ctxt = SynchronizationContext.Current
                    Assert.Null(ctxt)
                    Assert.True(Thread.CurrentThread.IsThreadPoolThread)
                }
            Async2.RunSynchronously(computation)
            SynchronizationContext.SetSynchronizationContext(old)
        }
        |> Async2.RunSynchronously

    // ---- RunSynchronouslyImmediate: basic functionality ----

    [<Fact>]
    member _.``RunSynchronouslyImmediate returns value``() =
        let result = async2 { return 42 } |> Async2.RunSynchronouslyImmediate
        Assert.Equal(42, result)

    [<Fact>]
    member _.``RunSynchronouslyImmediate propagates exception``() =
        Assert.Throws<InvalidOperationException>(fun () ->
            async2 { invalidOp "test" }
            |> Async2.RunSynchronouslyImmediate
            |> ignore
        ) |> ignore

    [<Fact>]
    member _.``RunSynchronouslyImmediate respects pre-cancelled token``() =
        use cts = new CancellationTokenSource()
        cts.Cancel()
        let oce = Assert.Throws<OperationCanceledException>(Action(fun () -> Async2.RunSynchronouslyImmediate(async2 { () }, cancellationToken = cts.Token)))
        Assert.Equal(cts.Token, oce.CancellationToken)

    [<Fact>]
    member _.``RunSynchronouslyImmediate works with Sleep``() =
        let result =
            async2 {
                do! Async2.Sleep 10
                return 17
            }
            |> Async2.RunSynchronouslyImmediate
        Assert.Equal(17, result)

    // ---- RunSynchronouslyImmediate: differences from RunSynchronously ----
    //
    // RunSynchronously will offload to the thread pool when SynchronizationContext.Current is
    // non-null or Thread.IsThreadPoolThread is false (e.g. FSI, GUI threads, dedicated test threads).
    // In those cases the computation commences on a different thread and exception stack traces are
    // incomplete. RunSynchronouslyImmediate always executes the first step on the calling thread,
    // giving a complete call stack that is much more useful during interactive testing.

    static member private OnFreshThread f =
        let mutable exn = null
        let t = Thread(fun () ->
            try f ()
            with e -> exn <- e)
        t.Start()
        t.Join()
        if exn <> null then raise exn

    [<Fact>]
    // RunSynchronously offloads to the thread pool when SynchronizationContext.Current is non-null
    // (see RunSynchronously.ThreadJump.IfSyncCtxtNonNull).
    // and/or the caller is not a threadpool thread
    // RunSynchronouslyImmediate always starts on the calling thread regardless.
    member _.``RunSynchronouslyImmediate Starts on calling thread even when SynchronizationContext present``() =
        AsyncModule.OnFreshThread(fun () ->
            // Aside: bonus condition that would also make RunSynchronously offload
            Assert.False(Thread.CurrentThread.IsThreadPoolThread)
            let old = SynchronizationContext.Current
            try SynchronizationContext.SetSynchronizationContext(SynchronizationContext())
                let mutable startThreadId = -1
                async2 { startThreadId <- Thread.CurrentThread.ManagedThreadId }
                |> Async2.RunSynchronouslyImmediate
                Assert.Equal(Thread.CurrentThread.ManagedThreadId, startThreadId)
            finally SynchronizationContext.SetSynchronizationContext old )

    [<Fact>]
    // Demonstrates the key difference in starting-thread identity between the two methods when called
    // from a non-thread-pool thread (e.g. FSI, a test runner's main thread, or a dedicated thread):
    // RunSynchronously offloads the computation to a thread-pool thread (different thread ID),
    // while RunSynchronouslyImmediate keeps it on the calling thread (same thread ID).
    // The latter ensures that exception stack traces include frames from the caller's thread,
    // making failures much easier to diagnose during interactive testing.
    member _.``RunSynchronouslyImmediate.vs.RunSynchronously.CallerThreadIdentity``() =
        let mutable runSyncThreadId = -1
        let mutable immThreadId = -1
        let mutable callerThreadId = -1
        AsyncModule.OnFreshThread(fun () ->
            callerThreadId <- Thread.CurrentThread.ManagedThreadId
            async2 { runSyncThreadId <- Thread.CurrentThread.ManagedThreadId }
            |> Async2.RunSynchronously
            async2 { immThreadId <- Thread.CurrentThread.ManagedThreadId }
            |> Async2.RunSynchronouslyImmediate)
        Assert.NotEqual(callerThreadId, runSyncThreadId)
        Assert.Equal(callerThreadId, immThreadId)

    [<Fact>]
    // Because RunSynchronouslyImmediate starts on the calling thread, an exception thrown before
    // any do! in the computation is captured on that thread. When re-raised to the caller the
    // exception stack trace will include it as a nested exception.
    member _.``RunSynchronouslyImmediate.ExceptionOriginatesOnCallingThread``() =
        let mutable callerThreadId = -1
        let mutable exceptionOriginThreadId = -1
        AsyncModule.OnFreshThread(fun () ->
            callerThreadId <- Thread.CurrentThread.ManagedThreadId
            try async2 {
                    exceptionOriginThreadId <- Thread.CurrentThread.ManagedThreadId
                    failwith "boom"
                }
                |> Async2.RunSynchronouslyImmediate
            with e ->
                // Not part of the test, but useful for understanding:
                // shows full stack trace from test thread down
                // Equivalent code under RunSynchronously would be capturing a partial trace from the threadpool thread here,
                //  followed by rethrowing it as a nested exception at the wait site (via AsyncResult.Commit())
                printfn $"STACKTRACE ===\n{e.StackTrace}\n===")
        Assert.Equal(callerThreadId, exceptionOriginThreadId)

    [<Fact>]
    member _.``RaceBetweenCancellationAndError.AwaitWaitHandle``() =
        let disposedEvent = new System.Threading.ManualResetEvent(false)
        dispose disposedEvent
        testErrorAndCancelRace "RaceBetweenCancellationAndError.AwaitWaitHandle" (Async2.AwaitWaitHandle disposedEvent)

    [<Fact>]
    member _.``RaceBetweenCancellationAndError.Sleep``() =
        testErrorAndCancelRace "RaceBetweenCancellationAndError.Sleep" (Async2.Sleep (-5))

    [<Fact>]
    member _.``dispose should not throw when called on null``() =
        let result = async2 { use x = null in return () } |> Async2.RunSynchronously

        Assert.Equal((), result)

    [<Fact>]
    member _.``dispose should not throw when called on null struct``() =
        let result = async2 { use x = new Dummy(1) in return () } |> Async2.RunSynchronously

        Assert.Equal((), result)

    [<Fact>]
    member _.``error on one workflow should cancel all others``() =
        task {
            use failOnlyOne = new Semaphore(0, 1)
            let mutable cancelled = 0
            let mutable started = 0

            let job i = async2 {
                let! ct = Async2.CancellationToken
                Interlocked.Increment &started |> ignore
                try
                    do! failOnlyOne |> Async2.AwaitWaitHandle |> Async2.Ignore
                    failwith "boom"
                finally
                    if ct.IsCancellationRequested then
                        Interlocked.Increment &cancelled |> ignore

            }

            let test = Async2.Parallel [ for i in 1 .. 100 -> job i ] |> Async2.Catch |> Async2.Ignore |> Async2.StartAsTask
            // Wait for more than one job to start
            while started < 2 do
                do! Task.Yield()
            printfn $"started jobs: {started}"
            failOnlyOne.Release() |> ignore
            do! test
            Assert.Equal(cancelled, started - 1)
        }

    [<Fact>]
    member _.``AwaitWaitHandle.ExceptionsAfterTimeout``() =
        let wh = new System.Threading.ManualResetEvent(false)
        let test = async2 {
            try
                let! timeout = Async2.AwaitWaitHandle(wh, 1000)
                do! Async2.Sleep 500
                raise (new InvalidOperationException("EXPECTED"))
                return Assert.Fail("Should not get here")
            with
                :? InvalidOperationException as e when e.Message = "EXPECTED" -> return ()
            }
        Async2.RunSynchronously(test)

    [<Fact>]
    member _.``FromContinuationsCanTailCallCurrentThread``() =
        let mutable cnt = 0
        let origTid = System.Threading.Thread.CurrentThread.ManagedThreadId
        let mutable finalTid = -1
        let rec f n =
            if n = 0 then
                async2 {
                    finalTid <- System.Threading.Thread.CurrentThread.ManagedThreadId
                    return () }
            else
                async2 {
                    cnt <- cnt + 1
                    do! Async2.FromContinuations(fun (k,_,_) -> k())
                    do! f (n-1)
                }
        // 5000 is big enough that does-not-stackoverflow means we are tailcalling thru FromContinuations
        f 5000 |> Async2.StartImmediate
        Assert.Equal(origTid, finalTid)
        Assert.Equal(5000, cnt)

    [<Fact>]
    member _.``AwaitWaitHandle With Cancellation``() =
        let run wh = async2 {
            let! r = Async2.AwaitWaitHandle wh
            Assert.True(r, "Timeout not expected")
            return()
            }
        let test () =
            let wh = new System.Threading.ManualResetEvent(false)
            let cts = new System.Threading.CancellationTokenSource()
            let asyncs =
                [
                  yield! List.init 100 (fun _ -> run wh)
                  yield async2 { cts.Cancel() }
                  yield async2 { wh.Set() |> ignore }
                ]
            try
                asyncs
                  |> Async2.Parallel
                  |> fun c -> Async2.RunSynchronously(c, cancellationToken = cts.Token)
                  |> ignore
            with
                :? System.OperationCanceledException -> () // OK
        for _ in 1..1000 do test()

    [<Fact>]
    member _.``StartWithContinuationsVersusDoBang``() =
        // worthwhile to note these three
        // case 1
        let mutable r = ""
        async2 {
            try
                do! Async2.FromContinuations(fun (s, _, _) -> s())
                return failwith "boom"
            with
                e-> r <- e.Message
            } |> Async2.RunSynchronously
        Assert.Equal("boom", r)
        // case 2
        r <- ""
        try
            Async2.StartWithContinuations(Async2.FromContinuations(fun (s, _, _) -> s()), (fun () -> failwith "boom"), (fun e -> r <- e.Message), (fun oce -> ()))
        with
            e -> r <- "EX: " + e.Message
        Assert.Equal("EX: boom", r)
        // case 3
        r <- ""
        Async2.StartWithContinuations(async2 { return! failwith "boom" }, (fun x -> ()), (fun e -> r <- e.Message), (fun oce -> ()))
        Assert.Equal("boom", r)


#if IGNORED
    [<Test; Ignore("See https://github.com/dotnet/fsharp/issues/4887")>]
    member _.``SleepContinuations``() = 
        let okCount = ref 0
        let errCount = ref 0
        let test() = 
            let cts = new System.Threading.CancellationTokenSource() 
 
            System.Threading.ThreadPool.QueueUserWorkItem(fun _-> 
                System.Threading.Thread.Sleep 50 
                try 
                    Async2.StartWithContinuations( 
                        Async2.Sleep(1000), 
                        (fun _ -> printn "ok"; incr okCount), 
                        (fun _ -> printn "error"; incr errCount), 
                        (fun _ -> printfn "cancel"; failwith "BOOM!"), 
                        cancellationToken = cts.Token 
                    ) 
                with _ -> () 
            ) |> ignore 
            System.Threading.Thread.Sleep 50 
            try cts.Cancel() with _ -> () 
            System.Threading.Thread.Sleep 1500 
            printn "====" 
        for i = 1 to 3 do test()
        Assert.Equal(0, !okCount)
        Assert.Equal(0, !errCount)
#endif

    [<Fact>]
    member _.``Async2 caching should work``() =
        let mutable x = 0
        let someSlowFunc _mykey = async2 {
            Console.WriteLine "Simulated downloading..."
            do! Async2.Sleep 400
            Console.WriteLine "Simulated downloading Done."
            x <- x + 1 // Side effect!
            return "" }

        let memFunc : string -> Async2<string> = Utils.memoizeAsync <| someSlowFunc

        async2 {
            Console.WriteLine "Do the same memoized thing many ways...."
            do! memFunc "a" |> Async2.Ignore
            do! memFunc "a" |> Async2.Ignore
            do! memFunc "a" |> Async2.Ignore
            do! [|1 .. 30|] |> Seq.map(fun _ -> (memFunc "a"))
                |> Async2.Parallel |> Async2.Ignore

            Console.WriteLine "Still more ways...."
            for _i = 1 to 30 do
                Async2.Start( memFunc "a" |> Async2.Ignore )
                Async2.Start( memFunc "a" |> Async2.Ignore )
            do! Async2.Sleep 500
            do! memFunc "a" |> Async2.Ignore
            do! memFunc "a" |> Async2.Ignore
            Console.WriteLine "Still more ways again...."
            for _i = 1 to 30 do
                Async2.Start( memFunc "a" |> Async2.Ignore )

            Console.WriteLine "Still more ways again again...."
            do! [|1 .. 30|] |> Seq.map(fun _ -> (memFunc "a"))
                |> Async2.Parallel |> Async2.Ignore
        } |> Async2.RunSynchronously
        Console.WriteLine "Checking result...."
        Assert.Equal(1, x)

    [<Fact>]
    member _.``Parallel with maxDegreeOfParallelism`` () =
        let mutable i = 1
        let action j = async2 {
            Assert.Equal(j, i)
            i <- i + 1
        }
        let computation =
            [| for i in 1 .. 1000 -> action i |]
            |> fun cs -> Async2.Parallel(cs, 1)
        Async2.RunSynchronously(computation) |> ignore

    [<Fact>]
    member _.``maxDegreeOfParallelism cannot be 0`` () =
        try
            [| for i in 1 .. 10 -> async2 { return i } |]
            |> fun cs -> Async2.Parallel(cs, 0)
            |> ignore
            Assert.Fail("Unexpected success")
        with
        | :? System.ArgumentException as exc ->
            Assert.Equal("maxDegreeOfParallelism", exc.ParamName)
            Assert.True(exc.Message.Contains("maxDegreeOfParallelism must be positive, was 0"))

    [<Fact>]
    member _.``maxDegreeOfParallelism cannot be negative`` () =
        try
            [| for i in 1 .. 10 -> async2 { return i } |]
            |> fun cs -> Async2.Parallel(cs, -1)
            |> ignore
            Assert.Fail("Unexpected success")
        with
        | :? System.ArgumentException as exc ->
            Assert.Equal("maxDegreeOfParallelism", exc.ParamName)
            Assert.True(exc.Message.Contains("maxDegreeOfParallelism must be positive, was -1"))

    [<Fact>]
    member _.``RaceBetweenCancellationAndError.Parallel(maxDegreeOfParallelism)``() =
        [| for i in 1 .. 1000 -> async2 { failwith "boom" } |]
        |> fun cs -> Async2.Parallel(cs, 1)
        |> testErrorAndCancelRace "RaceBetweenCancellationAndError.Parallel(maxDegreeOfParallelism)"

    [<Fact>]
    member _.``RaceBetweenCancellationAndError.Parallel``() =
        [| for i in 1 .. 1000 -> async2 { failwith "boom" } |]
        |> fun cs -> Async2.Parallel(cs)
        |> testErrorAndCancelRace "RaceBetweenCancellationAndError.Parallel"

    [<Fact>]
    member _.``error on one workflow should cancel all others with maxDegreeOfParallelism``() =
        let counter =
            async2 {
                let mutable counter = 0
                let job i = async2 {
                    if i = 55 then failwith "boom"
                    else
                        counter <- counter + 1
                }

                let! _ = Async2.Parallel ([ for i in 1 .. 100 -> job i ], 1) |> Async2.Catch
                return counter
            } |> Async2.RunSynchronously

        Assert.Equal(54, counter)

    [<Fact>]
    member _.``async2 doesn't do cancel check between do! and try-finally``() =
        let gate = obj()
        for i in 0..10 do
            let procCount = 3
            use semaphore = new SemaphoreSlim(procCount-1)
            printfn "Semaphore count available: %i" semaphore.CurrentCount
            let mutable acquiredCount = 0
            let mutable releaseCount = 0
            try
                List.init procCount (fun index ->
                    async2 {
                        lock gate <| fun () -> printfn "[%i] Waiting to enter semaphore" index
                        let! cancellationToken = Async2.CancellationToken

                        // The semaphore lets two threads through at a time
                        do! semaphore.WaitAsync(cancellationToken) |> Async2.AwaitTask

                        // No implicit cancellation checks should take place between a do! and a try
                        // if there are no other async2 control constructs present.  If there is synchronous code
                        // it runs without cancellation checks
                        //
                        // index 1 will enter the try/finally quickly, call failwith and cancel the other tasks
                        // One of index 2 and index 3 will be stuck here before the try/finally. But having got
                        // this far it should enter the try/finally before cancellation takes effect
                        do
                          lock gate <| fun () -> printfn "[%i] Acquired semaphore" index
                          Interlocked.Increment(&acquiredCount) |> ignore
                          if index <> 0 then
                              lock gate <| fun () -> printfn "[%i] Slowly entering try/finally" index
                              System.Threading.Thread.Sleep(100)

                        try
                            lock gate <| fun () -> printfn "[%i] Within try-finally" index
                            if index = 0 then
                                lock gate <| fun () -> printfn "[%i] Error" index
                                // The failure will cause others to cancel
                                failwith "Something bad happened!"
                        finally
                            semaphore.Release() |> ignore
                            // This should always get executed
                            Interlocked.Increment(&releaseCount) |> ignore
                            lock gate <| fun () -> printfn "[%i] Semaphore released" index
                    })
                |> Async2.Parallel
                |> Async2.Ignore
                |> Async2.RunSynchronously
            with
            | exn ->
                lock gate <| fun () -> printfn "Unhandled exception: %s" exn.Message
                lock gate <| fun () -> printfn "Semaphore count available: %i" semaphore.CurrentCount
            Assert.Equal(acquiredCount, releaseCount)

    [<Fact>]
    member _.``Async2.Parallel blows stack when cancelling many`` () =
        let gen (i : int) = async2 {
            if i <> 0 then do! Async2.Sleep i
            else return failwith "OK"}
        let count = 3600
        let comps = Seq.init count gen
        let result = Async2.Parallel(comps, 16) |> Async2.Catch |> Async2.RunSynchronously
        match result with
        | Choice2Of2 e -> Assert.Equal("OK", e.Message)
        | x -> failwithf "unexpected %A" x
