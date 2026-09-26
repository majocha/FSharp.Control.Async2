// Copyright (c) Microsoft Corporation.  All Rights Reserved.  See License.txt in the project root for license information.

// Various tests for the:
// Microsoft.FSharp.Control.Async2 type

namespace FSharp.Core.UnitTests.Control

open System
open Microsoft.FSharp.Control
open FSharp.Core.UnitTests.LibraryTestFx
open Xunit
open System.Threading
open System.Threading.Tasks

// Test affects global state via Async2.CancelDefaultToken
[<Collection(nameof FSharp.Test.NotThreadSafeResourceCollection)>]
module AsyncType =

    type ExpectedContinuation = Success | Exception | Cancellation

    [<Fact>]
    let startWithContinuations() =

        let cont actual expected _ =
            if expected <> actual then
                failwith $"expected {expected} continuation, but ran {actual}"

        let onSuccess = cont Success
        let onException = cont Exception
        let onCancellation = cont Cancellation

        let expect expected computation =
            Async2.StartWithContinuations(computation, onSuccess expected, onException expected, onCancellation expected)

        async2 {
            Async2.CancelDefaultToken()
            return ()
        } |> expect Cancellation

        async2 { failwith "computation failed" } |> expect Exception

        async2 { return () } |> expect Success

module Helpers =

    // Use a generous timeout to avoid flaky failures on loaded CI machines where the thread pool may be saturated.
    let verifyTaskCompletion (t: Task) =
        let completed = t.Wait(TimeSpan.FromSeconds 30.0)
        Assert.True(completed, "Task did not finish after waiting for 30 seconds.")

    let asyncWait immediate (a: Async2<'T>): 'T =
        if immediate then Async2.RunSynchronouslyImmediate a
        else Async2.RunSynchronously a
    let asyncWaitWithCt immediate (ct: CancellationToken) (a: Async2<'T>): 'T =
        if immediate then Async2.RunSynchronouslyImmediate(a, cancellationToken = ct)
        else Async2.RunSynchronously(a, cancellationToken = ct)

    let asyncWaitImm a = asyncWait true a
    let asyncWaitWithCtImm ct a = asyncWaitWithCt true ct a

open Helpers

type AsyncType() =

    let ignoreSynchCtx f =
        f ()

    [<VolatileField>]
    let mutable spinloop = true

    [<Theory; InlineData true; InlineData false>]
    member _.AsyncRunSynchronouslyReusesThreadPoolThread(immediate) =
        let run a = asyncWait immediate a
        let action _ =
            async2 {
                return
                    async2 { return Thread.CurrentThread.ManagedThreadId }
                    |> run
            }
        // This test needs approximately 1000 ThreadPool threads (if Async2.RunSynchronously[Immediate] doesn't reuse them)
        let usedThreads =
            Seq.init 1000 action
            |> Async2.Parallel
            |> run
            |> Set.ofArray
        printfn $"RunSynchronously used {usedThreads.Count} threads. Environment.ProcessorCount is {Environment.ProcessorCount}."
        // Some arbitrary large number but in practice it should not use more threads than there are CPU cores.
        Assert.True(usedThreads.Count < 256, $"RunSynchronously used {usedThreads.Count} threads.")

    [<Theory>]
    [<InlineData("int32")>]
    [<InlineData("timespan")>]
    member _.AsyncSleepCancellation1(sleepType) =
        ignoreSynchCtx (fun () ->
            let computation =
                match sleepType with
                | "int32"    -> Async2.Sleep(10000000)
                | "timespan" -> Async2.Sleep(10000000.0 |> TimeSpan.FromMilliseconds)
                | unknown    -> failwith $"Unknown {unknown}"
            let mutable result = ""
            use cts = new CancellationTokenSource()
            Async2.StartWithContinuations(computation,
                                            (fun _ -> result <- "Ok"),
                                            (fun _ -> result <- "Exception"),
                                            (fun _ -> result <- "Cancel"),
                                            cts.Token)
            cts.Cancel()
            Async2.Sleep 1000 |> asyncWaitImm
            Assert.Equal("Cancel", result)
        )

    [<Theory>]
    [<InlineData("int32")>]
    [<InlineData("timespan")>]
    member _.AsyncSleepCancellation2(sleepType) =
        ignoreSynchCtx (fun () ->
            let computation =
                match sleepType with
                | "int32"    -> Async2.Sleep(10)
                | "timespan" -> Async2.Sleep(10.0 |> TimeSpan.FromMilliseconds)
                | unknown    -> failwith $"Unknown {unknown}"
            for i in 1..100 do
                let mutable result = ""
                use completedEvent = new ManualResetEvent(false)
                use cts = new CancellationTokenSource()
                Async2.StartWithContinuations(computation,
                                                (fun _ -> result <- "Ok"; completedEvent.Set() |> ignore),
                                                (fun _ -> result <- "Exception"; completedEvent.Set() |> ignore),
                                                (fun _ -> result <- "Cancel"; completedEvent.Set() |> ignore),
                                                cts.Token)
                sleep(10)
                cts.Cancel()
                completedEvent.WaitOne() |> Assert.True
                Assert.True(result = "Cancel" || result = "Ok")
        )

    [<Theory>]
    [<InlineData("int32")>]
    [<InlineData("timespan")>]
    member _.AsyncSleepThrowsOnNegativeDueTimes(sleepType) =
        async2 {
            try
                do! match sleepType with
                    | "int32"    -> Async2.Sleep(-100)
                    | "timespan" -> Async2.Sleep(-100.0 |> TimeSpan.FromMilliseconds)
                    | unknown    -> failwith $"Unknown {unknown}"
                failwith "Expected ArgumentOutOfRangeException"
            with
            | :? ArgumentOutOfRangeException -> ()
        } |> asyncWaitImm

    [<Fact>]
    member _.AsyncSleepInfinitely() =
        ignoreSynchCtx (fun () ->
            let computation = Async2.Sleep(Timeout.Infinite)
            let result = TaskCompletionSource<string>()
            use cts = new CancellationTokenSource(TimeSpan.FromSeconds 1.0) // there's a long way from 1 sec to infinity, but it'll have to do.
            Async2.StartWithContinuations(computation,
                                            (fun _ -> result.TrySetResult("Ok")        |> ignore),
                                            (fun _ -> result.TrySetResult("Exception") |> ignore),
                                            (fun _ -> result.TrySetResult("Cancel")    |> ignore),
                                            cts.Token)
            let result = result.Task |> Async2.Await |> asyncWaitImm
            Assert.Equal("Cancel", result)
        )

    [<Fact>]
    member _.CreateTask () =
        let s = "Hello tasks!"
        let a = async2 { return s }
        let t : Task<string> = Async2.StartAsTask a
        verifyTaskCompletion t
        Assert.True(t.IsCompleted)
        Assert.Equal(s, t.Result)

    [<Fact>]
    member _.StartAsTaskCancellation () =
        let cts = new CancellationTokenSource()
        let asyncStarted = new ManualResetEventSlim(false)
        let doSpinloop () = while spinloop do ()
        let a = async2 {
            asyncStarted.Set()
            cts.CancelAfter(100)
            doSpinloop()
        }

        let t : Task<unit> = Async2.StartAsTask(a, cancellationToken = cts.Token)

        // Wait for the async2 body to actually start executing before checking timing.
        // Use a generous timeout to avoid flaky failures on loaded CI machines where the thread pool may be saturated.
        Assert.True(asyncStarted.Wait(30_000), "Async2 body did not start within 30 seconds")

        // Should not finish, we don't eagerly mark the task done just because it's been signaled to cancel.
        try
            let result = t.Wait(1000)
            Assert.False(result)
        with :? AggregateException -> Assert.Fail "Task should not finish, yet"

        spinloop <- false

        try
            let result = t.Wait(TimeSpan(hours=0,minutes=0,seconds=5))
            Assert.True(result, "Task did not finish after waiting for 5 seconds.")
        with :? AggregateException as a ->
            match a.InnerException with
            | :? TaskCanceledException -> ()
            | _ -> reraise()

        Assert.True(t.IsCompleted, "Task is not completed")


    [<Fact>]
    member _.``AwaitTask ignores Async2 cancellation`` () =
        let cts = new CancellationTokenSource()
        let tcs = TaskCompletionSource<unit>()
        let innerTcs = TaskCompletionSource<unit>()
        let a = innerTcs.Task |> Async2.AwaitTask

        Async2.StartWithContinuations(a, tcs.SetResult, tcs.SetException, ignore >> tcs.SetCanceled, cts.Token)

        cts.CancelAfter(100)
        try
            let result = tcs.Task.Wait(300)
            Assert.False(result)
        with :? AggregateException -> Assert.Fail "Should not finish, yet"

        innerTcs.SetResult ()

        try
            verifyTaskCompletion tcs.Task
        with :? AggregateException as a ->
            match a.InnerException with
            | :? TaskCanceledException -> ()
            | _ -> reraise()
        Assert.True(tcs.Task.IsCompleted, "Task is not completed")

    [<Theory; InlineData(false); InlineData(true)>]
    member _.RunSynchronouslyCancellationWithDelayedResult(newAwait: bool) =
        let cts = new CancellationTokenSource()
        let tcs = TaskCompletionSource<int>()
        let _ = cts.Token.Register(fun () -> tcs.SetResult 42)
        let a = async2 {
            cts.CancelAfter(100)
            let! result = tcs.Task |> if newAwait then Async2.Await else Async2.AwaitTask
            return result }

        let cancelled =
            try
                asyncWaitWithCtImm cts.Token a |> ignore
                false
            with :? OperationCanceledException as o -> true
                 | _ -> false

        Assert.True(cancelled, "Task is not cancelled")

    [<Fact>]
    member _.ExceptionPropagatesToTask () =
        let a = async2 {
            do raise (Exception ())
         }
        let t = Async2.StartAsTask a
        let mutable exceptionThrown = false
        try
            // waitForCompletion t
            t.Wait()
        with
            e -> exceptionThrown <- true
        Assert.True(t.IsFaulted)
        Assert.True(exceptionThrown)

    [<Fact>]
    member _.CancellationPropagatesToGroup () =
        let ewh = new ManualResetEvent(false)
        let mutable cancelled = false
        let a = async2 {
                use! holder = Async2.OnCancel (fun _ -> cancelled <- true)
                ewh.Set() |> Assert.True
                while true do ()
            }
        let cts = new CancellationTokenSource()
        let token = cts.Token
        let t = Async2.StartAsTask(a, cancellationToken=token)
//        printfn "%A" t.Status
        ewh.WaitOne() |> Assert.True
        cts.Cancel()
//        printfn "%A" t.Status
        let mutable exceptionThrown = false
        try
            t.Wait()
        with e -> exceptionThrown <- true
        Assert.True(exceptionThrown)
        Assert.True(t.IsCanceled)
        Assert.True(cancelled)

    [<Fact>]
    member _.CreateImmediateAsTask () =
        let s = "Hello tasks!"
        let a = async2 { return s }
        let t : Task<string> = Async2.StartImmediateAsTask a
        verifyTaskCompletion t
        Assert.True(t.IsCompleted)
        Assert.Equal(s, t.Result)

    [<Fact>]
    member _.StartImmediateAsTask () =
        let s = "Hello tasks!"
        let a = async2 { return s }
        let t = Async2.StartImmediateAsTask a
        verifyTaskCompletion t
        Assert.True(t.IsCompleted)
        Assert.Equal(s, t.Result)


    [<Fact>]
    member _.ExceptionPropagatesToImmediateTask () =
        let a = async2 {
            do raise (Exception ())
         }
        let t = Async2.StartImmediateAsTask a
        let mutable exceptionThrown = false
        try
            t.Wait()
        with
            e -> exceptionThrown <- true
        Assert.True(t.IsFaulted)
        Assert.True(exceptionThrown)

    [<Fact>]
    member _.CancellationPropagatesToGroupImmediate () =
        let ewh = new ManualResetEvent(false)
        let mutable cancelled = false
        let a = async2 {
                use! holder = Async2.OnCancel (fun _ -> cancelled <- true)
                ewh.Set() |> Assert.True
                while true do
                    do! Async2.Sleep 100
            }
        let cts = new CancellationTokenSource()
        let token = cts.Token
        let t =
            Async2.StartImmediateAsTask(a, cancellationToken=token)
        ewh.WaitOne() |> Assert.True
        cts.Cancel()
        let mutable exceptionThrown = false
        try
            t.Wait()
        with e -> exceptionThrown <- true
        Assert.True(exceptionThrown)
        Assert.True(t.IsCanceled)
        Assert.True(cancelled)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.TaskAsyncValue(newAwait: bool) =
        let s = "Test"
        use t = Task.Factory.StartNew(Func<_>(fun () -> s))
        let a = async2 {
            let! s1 = t |> if newAwait then Async2.Await else Async2.AwaitTask
            return s = s1
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.AwaitTaskCancellation(newAwait: bool) =
        let a = async2 {
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            tcs.SetCanceled()
            try
                do! tcs.Task |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with :? OperationCanceledException -> return true
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.AwaitCompletedTask() =
        let a = async2 {
            let threadIdBefore = Thread.CurrentThread.ManagedThreadId
            do! Async2.AwaitTask Task.CompletedTask
            let threadIdAfter = Thread.CurrentThread.ManagedThreadId
            return threadIdBefore = threadIdAfter
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.AwaitTaskCancellationUntyped(newAwait: bool) =
        let a = async2 {
            let tcs = System.Threading.Tasks.TaskCompletionSource<unit>()
            tcs.SetCanceled()
            try
                do! tcs.Task :> Task |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with :? OperationCanceledException -> return true
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.TaskAsyncValueException(newAwait: bool) =
        use t = Task.Factory.StartNew(Func<unit>(fun () -> raise <| Exception()))
        let a = async2 {
            try let! v = t |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with e -> return true
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.TaskAsyncValueCancellation(newAwait: bool) =
        use ewh = new ManualResetEvent(false)
        let cts = new CancellationTokenSource()
        let token = cts.Token
        use t : Task<unit> = Task.Factory.StartNew(Func<unit>(fun () -> while not token.IsCancellationRequested do ()), token)
        let cancelled = ref true
        let a = async2 {
            try
                use! _holder = Async2.OnCancel(fun _ -> ewh.Set() |> ignore)
                let! v = t |> if newAwait then Async2.Await else Async2.AwaitTask
                return v
            // A canceled task yields TaskCanceledException via the exception continuation
            with
               :? TaskCanceledException ->
                  ewh.Set() |> ignore // this is ok
        }
        let t1 = Async2.StartAsTask a
        cts.Cancel()
        ewh.WaitOne(10000) |> ignore
        // Don't leave unobserved background tasks, because they can crash the test run.
        t1.Wait()

    [<Theory; InlineData(false); InlineData(true)>]
    member _.NonGenericTaskAsyncValue(newAwait: bool) =
        let mutable hasBeenCalled = false
        use t = Task.Factory.StartNew(Action(fun () -> hasBeenCalled <- true))
        let a = async2 {
            do! t |> if newAwait then Async2.Await else Async2.AwaitTask
            return true
        }
        Assert.True(asyncWaitImm a && hasBeenCalled)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.NonGenericTaskAsyncValueException(newAwait: bool) =
        use t = Task.Factory.StartNew(Action(fun () -> raise <| Exception()))
        let a = async2 {
            try
                let! v = t |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with e -> return true
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.NonGenericTaskAsyncValueCancellation(newAwait: bool) =
        use ewh = new ManualResetEvent(false)
        let cts = new CancellationTokenSource()
        let token = cts.Token
        use t = Task.Factory.StartNew(Action(fun () -> while not token.IsCancellationRequested do ()), token)
        let a = async2 {
            try
                use! _holder = Async2.OnCancel(fun _ -> ewh.Set() |> ignore)
                let! v = t |> if newAwait then Async2.Await else Async2.AwaitTask
                return v
            // A canceled task yields TaskCanceledException via the exception continuation
            with
               :? TaskCanceledException ->
                  ewh.Set() |> ignore // this is ok
        }
        let t1 = Async2.StartAsTask a
        cts.Cancel()
        ewh.WaitOne(10000) |> ignore
        t1

    [<Fact>]
    member _.CancellationExceptionThrown () =
        use ewh = new ManualResetEventSlim(false)
        let cts = new CancellationTokenSource()
        let token = cts.Token
        let mutable hasThrown = false
        token.Register(fun () -> ewh.Set()) |> ignore
        let a = async2 {
            try
                while true do token.ThrowIfCancellationRequested()
            with _ -> hasThrown <- true
        }
        Async2.Start(a, token)
        cts.Cancel()
        ewh.Wait(10000) |> ignore
        Assert.False hasThrown

    [<Theory; InlineData(false); InlineData(true)>]
    member _.NoStackOverflowOnRecursion(newAwait: bool) =
        let mutable hasThrown = false
        let rec loop (x: int) = async2 {
            do! Task.CompletedTask |> if newAwait then Async2.Await else Async2.AwaitTask
            Console.WriteLine (if x = 10000 then failwith "finish" else x)
            return! loop(x+1)
        }

        try asyncWait false (loop 0)
            hasThrown <- false
        with Failure "finish" ->
            hasThrown <- true
        Assert.True hasThrown

    // Both AwaitTask and Await ignore the ambient cancellation token while waiting
    // (Same goes for the typed variants)
    [<Theory; InlineData(false); InlineData(true)>]
    member _.``Both AwaitTask and Await ignore ambient cancellation while waiting``(newAwait) =
        let cts = new CancellationTokenSource()
        let tcs = TaskCompletionSource<unit>()  // task that never completes
        let res = TaskCompletionSource<bool>()

        let a = async2 {
            try do! tcs.Task |> if newAwait then Async2.Await else Async2.AwaitTask
                res.TrySetResult true |> ignore
            with _ -> res.TrySetResult false |> ignore
        }

        Async2.Start(a, cts.Token)
        // NOTE we only cancel during the Await/AwaitTask - the initial check would throw if we canceled before the Start()
        cts.CancelAfter 100

        // AwaitTask should NOT honor the ambient CT trigger
        let taskCompleted = res.Task.Wait 500
        Assert.False(taskCompleted, "Await/AwaitTask should not have responded to ambient CT cancellation")
        tcs.TrySetResult() |> ignore // clean up
        res.Task.Wait()

    (* Task awaiters surface the first exception from a faulted task's AggregateException. *)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.``Await and AwaitTask(Task<'T>) multiple exceptions surface the first``(newAwait) =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException [ ArgumentException "a" :> exn; InvalidOperationException "b" :> exn ]
        let a = async2 {
            try
                let! _ = tcs.Task |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with :? ArgumentException as error -> return error.Message = "a"
        }
        Assert.True(asyncWaitImm a)

    [<Theory; InlineData(false); InlineData(true)>]
    member _.``Await and AwaitTask(Task) multiple exceptions surface the first``(newAwait) =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException [| ArgumentException "a" :> exn; InvalidOperationException "b" |]
        let a = async2 {
            try
                do! tcs.Task |> if newAwait then Async2.Await else Async2.AwaitTask
                return false
            with :? ArgumentException as error -> return error.Message = "a"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``AwaitTask(Task) unwraps a single exception``() =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try do! Async2.AwaitTask tcs.Task
                return false
            with :? ArgumentException as error -> return error.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(Task) unwraps a single exception``() =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try do! Async2.Await tcs.Task
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``AwaitTask(Task<'T>) unwraps a single exception``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try let! _ = Async2.AwaitTask tcs.Task
                return false
            with :? ArgumentException as error -> return error.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(Task<'T>) unwraps a single exception``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try let! _ = Async2.Await tcs.Task
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    (* Await(Task/Task<'T>) overloads happy path *)

    [<Fact>]
    member _.``Await(Task<'T>) happy path``() =
        let a = async2 {
            let! v = Async2.Await(Task.FromResult(42))
            return v = 42
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(Task) happy path``() =
        let a = async2 {
            do! Async2.Await(Task.CompletedTask)
            return true
        }
        Assert.True(asyncWaitImm a)

    (* StartTaskImmediate(Task/Task<'T>) *)

    [<Fact>]
    member _.``StartTaskImmediate(Task<'T>) flows result``() =
        let a = async2 {
            let! v = Async2.StartTaskImmediate(fun _ct -> Task.result 42)
            return v = 42
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(Task) happy path``() =
        let mutable called = false
        let a = async2 {
            do! Async2.StartTaskImmediate(fun _ct -> task { called <- true })
        }
        asyncWaitImm a
        Assert.True called

    [<Fact>]
    member _.``StartTaskImmediate flows CancellationToken``() =
        let cts = new CancellationTokenSource()
        let mutable capturedCt = CancellationToken.None
        let a = async2 {
            do! Async2.StartTaskImmediate(fun ct -> task { capturedCt <- ct })
        }
        asyncWaitWithCtImm cts.Token a
        Assert.Equal(cts.Token, capturedCt)

    [<Fact>]
    member _.``StartTaskImmediate(Task<'T>) exception unwraps``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try let! _ = Async2.StartTaskImmediate(fun _ct -> tcs.Task)
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(Task) exception unwraps``() =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try do! Async2.StartTaskImmediate(fun _ct -> tcs.Task :> Task)
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(Task<'T>) cancellation raises TaskCanceledException``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetCanceled()
        let a = async2 {
            try let! _ = Async2.StartTaskImmediate(fun _ct -> tcs.Task)
                return false
            with :? TaskCanceledException -> return true
        }
        Assert.True(asyncWaitImm a)

#if !NETFRAMEWORK
    (* Await(ValueTask and ValueTask<'T>) overloads coverage of mainline behaviors *)

    [<Fact>]
    member _.``Await(ValueTask) happy path``() =
        let a = async2 {
            do! Async2.Await(ValueTask())
            return true
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(ValueTask<'T>) happy path``() =
        let a = async2 {
            let! v = Async2.Await(ValueTask<int>(42))
            return v = 42
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(ValueTask) exception unwraps``() =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException(ArgumentException "original")
        let task = ValueTask(tcs.Task :> Task)
        let a = async2 {
            try do! Async2.Await task
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``Await(ValueTask<'T>) exception unwraps``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try let! _ = Async2.Await(ValueTask<int>(tcs.Task))
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    (* StartTaskImmediate(ValueTask/ValueTask<'T>) *)

    [<Fact>]
    member _.``StartTaskImmediate(ValueTask<'T>) flows result``() =
        let a = async2 {
            let! v = Async2.StartTaskImmediate(fun _ct -> ValueTask<int>(42))
            return v = 42
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(ValueTask) happy path``() =
        let a = async2 {
            do! Async2.StartTaskImmediate(fun _ct -> ValueTask())
            return true
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(ValueTask<'T>) exception unwraps``() =
        let tcs = TaskCompletionSource<int>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try
                let! _ = Async2.StartTaskImmediate(fun _ct -> ValueTask<int>(tcs.Task))
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)

    [<Fact>]
    member _.``StartTaskImmediate(ValueTask) exception unwraps``() =
        let tcs = TaskCompletionSource<unit>()
        tcs.SetException(ArgumentException "original")
        let a = async2 {
            try
                do! Async2.StartTaskImmediate(fun _ct -> ValueTask(tcs.Task :> Task))
                return false
            with :? ArgumentException as ae -> return ae.Message = "original"
        }
        Assert.True(asyncWaitImm a)
#endif

[<Collection(nameof FSharp.Test.NotThreadSafeResourceCollection)>]
type AsyncTypeCancelDefaultToken() =

    [<Fact>]
    member _.CancellationPropagatesToTask () =
        let ewh = new ManualResetEvent(false)
        let a = async2 {
                ewh.Set() |> Assert.True
                while true do ()
            }
        let t = Async2.StartAsTask a
        ewh.WaitOne() |> Assert.True
        Async2.CancelDefaultToken ()
        let mutable exceptionThrown = false
        try
            verifyTaskCompletion t
        with e -> exceptionThrown <- true
        Assert.True(exceptionThrown)
        Assert.True(t.IsCanceled)

    [<Fact>]
    member _.CancellationPropagatesToImmediateTask () =
        let a = async2 {
                while true do
                    do! Async2.Sleep 100
            }
        let t = Async2.StartImmediateAsTask a
        Async2.CancelDefaultToken ()
        let mutable exceptionThrown = false
        try
            t.Wait()
        with e -> exceptionThrown <- true
        Assert.True(exceptionThrown)
        Assert.True(t.IsCanceled)

module AsyncAwaitTaskLikeTests =

    // Minimal custom task-like type wrapping Task<'T>
    type MyTask<'T>(inner: Task<'T>) =
        member _.GetAwaiter() = inner.GetAwaiter()

    // Minimal custom unit-returning task-like
    type MyUnitTask(inner: Task) =
        member _.GetAwaiter() = inner.GetAwaiter()

    [<Fact>]
    let ``Await(task-like) happy path with result``() =
        let a =
            async2 {
                let! v = Async2.Await(MyTask(Task.FromResult 42))
                return v
            }
        Assert.Equal(42, asyncWaitImm a)


    [<Fact>]
    let ``Await(task-like) happy path unit``() =
        async2 {
            do! Async2.Await(MyUnitTask(Task.CompletedTask))
        }
        |> asyncWaitImm

    [<Fact>]
    let ``Await(task-like) deferred completion``() =
        let tcs = TaskCompletionSource<int>()
        let t =
            async2 {
                let! v = Async2.Await(MyTask(tcs.Task))
                return v
            }
            |> Async2.StartAsTask
        Assert.False(t.IsCompleted, "Should not be done before TCS is set")
        tcs.SetResult 7
        verifyTaskCompletion t
        Assert.Equal(7, t.Result)

    [<Fact>]
    let ``Await(task-like) deferred completion preserves AsyncLocal ExecutionContext``() =
        let asyncLocal = AsyncLocal<string>()
        let tcs = TaskCompletionSource<unit>()

        let t =
            Async2.StartImmediateAsTask(async2 {
                asyncLocal.Value <- "trace-id"
                do! Async2.Await(MyUnitTask(tcs.Task))
                return asyncLocal.Value // should yield trace-id, *unless ExecutionContext did not propagate*
            })
        Assert.False(t.IsCompleted, "Should not be done before TCS is set")

        // This should not pollute the continuation observed
        asyncLocal.Value <- "root-context"
        let completion =
            Task.Run(fun () ->
                asyncLocal.Value <- "completing-context" // if ExecutionContext is not propagated correctly to the continuation, it will see this
                tcs.SetResult())

        verifyTaskCompletion completion
        verifyTaskCompletion t
        Assert.Equal("trace-id", t.Result) // Validate the chaining worked correctly
        Assert.Equal("root-context", asyncLocal.Value) // Root level context should be preserved

    [<Fact>]
    let ``Await(task-like) exception propagation``() =
        let tcs = TaskCompletionSource<int>()
        let a =
            async2 {
                try let! _ = Async2.Await(MyTask(tcs.Task))
                    return false
                with :? InvalidOperationException as e ->
                    return e.Message = "boom"
            }
        tcs.SetException(InvalidOperationException "boom")
        Assert.True(asyncWaitImm a)

    [<Fact>]
    let ``Await(YieldAwaitable) yields and resumes``() =
        // Task.Yield() returns a YieldAwaitable which is a struct — exercises the struct-awaiter path.
        let mutable before, after = false, false
        async2 {
            before <- true
            do! Async2.Await(Task.Yield())
            after <- true
        }
        |> asyncWaitImm
        Assert.True(before && after)

    [<Fact>]
    let ``Await(ConfiguredTaskAwaitable) from ConfigureAwait``() =
        // task.ConfigureAwait(false) returns a ConfiguredTaskAwaitable — a common real-world task-like.
        let result =
            async2 {
                let! v = Async2.Await(Task.FromResult(42).ConfigureAwait(false))
                return v
            }
            |> asyncWaitImm
        Assert.Equal(42, result)

module AsyncStartTaskImmediateTaskLikeTests =

    [<Fact>]
    let ``StartTaskImmediate(YieldAwaitable factory) yields and resumes``() =
        // Task.Yield() returns a struct YieldAwaitable — exercises the struct-awaiter SRTP path.
        let mutable before, after = false, false

        async2 {
            before <- true
            do! Async2.StartTaskImmediate(fun _ -> Task.Yield())
            after <- true
        }
        |> asyncWaitImm
        Assert.True(before && after)

    [<Fact>]
    let ``StartTaskImmediate(ConfiguredTaskAwaitable factory) returns result``() =
        // ConfigureAwait(false) returns a ConfiguredTaskAwaitable — a common real-world task-like.
        let result =
            async2 {
                return! Async2.StartTaskImmediate(fun _ -> Task.FromResult(42).ConfigureAwait(false))
            }
            |> asyncWaitImm
        Assert.Equal(42, result)

    [<Fact>]
    let ``StartTaskImmediate flows CancellationToken``() =
        // The factory receives the ambient cancellation token from the enclosing async2.
        let mutable capturedCt = CancellationToken.None
        use cts = new CancellationTokenSource()

        let a = async2 {
            do! Async2.StartTaskImmediate(fun ct ->
                    capturedCt <- ct
                    Task.CompletedTask.ConfigureAwait(false))
        }
        asyncWaitWithCtImm cts.Token a

        Assert.Equal(cts.Token, capturedCt)

module AsyncAwaitStackTraceTests =

    open System.Runtime.CompilerServices

    // Minimal wrapper to route through the SRTP overload instead of the specific Task<'T> overload.
    // Task<'T>, Task, ValueTask<'T>, and ValueTask all have higher-priority intrinsic overloads.
    type TaskWrapper<'T>(inner: Task<'T>) =
        member _.GetAwaiter() = inner.GetAwaiter()

    // Plain function — provides a stable named frame at the outermost throw site.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let throwAtLevel1 () : unit = invalidOp "boom"

    // Level-1 task: thin wrapper around the direct throw.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let level1Task () : Task<unit> = task { throwAtLevel1 () }

    // Level-2 task: introduces a real async2 await boundary between levels 1 and 2.
    [<MethodImpl(MethodImplOptions.NoInlining)>]
    let level2Task () : Task<unit> = task { do! level1Task () }

    // Run via StartImmediateAsTask + .Wait() and return the inner exception.
    // Using StartImmediateAsTask (not RunSynchronously) ensures that the async2-layer
    // exception machinery goes through TaskCompletionSource.SetException, which preserves
    // the stack trace rather than rethrowing synchronously and/or potentially truncating it.
    let runAndCaptureException (computation: Async2<unit>) : exn =
        // TODO swap in usage of Async2.RunSynchronouslyImmediate and add characterization of the stack trace behavior there as well
        let t = Async2.StartImmediateAsTask computation
        let ae = Assert.Throws<AggregateException>(fun () -> t.Wait())
        ae.InnerException

    // Template assertion: levels 1 and 2 must be traceable in the stack trace
    // regardless of which Async2.Await overload is used.
    let checkTrace (e: exn) =
        let trace = e.StackTrace
        // Print the trace to help diagnose missing frames.
        printn "EDI trace ===="
        printfn "%s" trace
        printn "==== EDI trace"
        Assert.NotNull(trace)
        Assert.Contains("throwAtLevel1", trace)
        Assert.Contains("level1Task", trace)
        Assert.Contains("level2Task", trace)

    // --- Tests per overload ---
    // The common skeleton is: build a 3-level chain (throwAtLevel1 → level1Task → level2Task),
    // wrap the outermost level in an async2 block using Async2.Await, run via
    // StartImmediateAsTask + .Wait(), and assert on the resulting exception's stack trace.

    [<Fact>]
    let ``Await Task-of-T: all three levels visible in stack trace`` () =
        let e = runAndCaptureException (async2 { do! Async2.Await(level2Task()) })
        checkTrace e

    [<Fact>]
    let ``Await Task (non-generic): all three levels visible in stack trace`` () =
        let e = runAndCaptureException (async2 { do! Async2.Await(level2Task() :> Task) })
        checkTrace e
        // Same behavior as the Task<'T> overload — see comment there.

#if !NETFRAMEWORK
    [<Fact>]
    let ``Await ValueTask-of-T: all three levels visible in stack trace`` () =
        // The ValueTask is backed by a faulted task, so the same task-level frames remain visible.
        let e = runAndCaptureException (async2 { do! Async2.Await(ValueTask<unit>(level2Task())) })
        checkTrace e

    [<Fact>]
    let ``Await ValueTask (non-generic): all three levels visible in stack trace`` () =
        // The non-generic await path includes its Async2 wrapper frames.
        let e = runAndCaptureException (async2 { do! Async2.Await(ValueTask(level2Task() :> Task)) })

        checkTrace e
#endif

    [<Fact>]
    let ``Await task-like via SRTP overload: all three levels visible in stack trace`` () =
        let e = runAndCaptureException (async2 { do! Async2.Await(TaskWrapper(level2Task())) })

        checkTrace e