// Copyright (c) Microsoft Corporation.  All Rights Reserved.  See License.txt in the project root for license information.

// Tests for camelCase functions in module Async2
module FSharp.Core.UnitTests.Control.AsyncModuleFunctionsTests

open System
open Microsoft.FSharp.Control
open System.Threading
open System.Threading.Tasks
open Xunit

#if NETFRAMEWORK // Polyfill for netstandard2.0
let cancelWithToken (tcs: TaskCompletionSource<'T>) =
    tcs.SetCanceled() // No CT overload available
    CancellationToken.None // so exception won't reference one
#else
let cancelWithToken (tcs: TaskCompletionSource<'T>) =
    let ct = CancellationToken true
    tcs.SetCanceled ct
    ct
#endif

let asyncWait (a: Async2<'T>): 'T = Async2.RunSynchronouslyImmediate a
let asyncWaitWithCt (ct: CancellationToken) (a: Async2<'T>): 'T = Async2.RunSynchronouslyImmediate(a, cancellationToken = ct)

[<Fact>]
let ``Async2.result wraps value`` () =
    let actual = Async2.result 42 |> asyncWait
    Assert.Equal(42, actual)


[<Fact>]
let ``Async2.map transforms value`` () =
    let actual = Async2.result 21 |> Async2.map (fun x -> x * 2) |> asyncWait
    Assert.Equal(42, actual)

[<Fact>]
let ``Async2.map propagates incoming exception`` () =
    let a = async2 { return failwith "boom" : int } |> Async2.map (fun x -> x * 2)
    let e = Assert.Throws<exn>(fun () -> a |> asyncWait |> ignore)
    Assert.Equal("boom", e.Message)

[<Fact>]
let ``Async2.map propagates mapper exception as Fault`` () =
    let a = Async2.result () |> Async2.map (fun () -> failwith "boom")
    let e = Assert.Throws<exn>(fun () -> a |> asyncWait |> ignore)
    Assert.Equal("boom", e.Message)

[<Fact>]
let ``Async2.map propagates Cancellation (sync)`` () =
    let ct = CancellationToken true
    let a = Async2.result 2 |> Async2.map (fun x -> x * 2)
    let e = Assert.Throws<OperationCanceledException>(fun () -> a |> asyncWaitWithCt ct |> ignore)
    Assert.Equal(ct, e.CancellationToken)

[<Fact>]
let ``Async2.map propagates Cancellation (async2)`` () =
    let mutable mapperWasCalled = false
    let cts = new CancellationTokenSource()
    let a =
        async2 {
            do! Async2.Sleep 5000 }
        |> Async2.map (fun () -> async2 { mapperWasCalled <- true })
    let t = Async2.StartAsTask(a, cancellationToken = cts.Token)
    cts.Cancel()
    let e = Assert.ThrowsAsync<OperationCanceledException>(fun () -> t).Result
    //Assert.NotEqual(cts.Token, e.CancellationToken) // no longer
    Assert.False mapperWasCalled


[<Fact>]
let ``Async2.bind threads value`` () =
    let actual =
        Async2.result 21
        |> Async2.bind (fun x -> Async2.result (x * 2))
        |> asyncWait
    Assert.Equal(42, actual)

[<Fact>]
let ``Async2.bind propagates incoming exception (sync)`` () =
    let a = async2 { return failwith "boom" } |> Async2.bind Async2.result
    let e = Assert.Throws<exn>(fun () -> a |> asyncWait |> ignore)
    Assert.Equal("boom", e.Message)

[<Fact>]
let ``Async2.bind propagates binder exception as Fault (async)`` () =
    let a = Async2.result 5 |> Async2.bind (fun x -> async2 { failwith $"boom {x}"})
    let e = Assert.Throws<exn>(fun () -> asyncWait a)
    Assert.Equal("boom 5", e.Message)

[<Fact>]
let ``Async2.bind propagates Cancellation (sync)`` () =
    let ct = CancellationToken true
    let a = Async2.result 2 |> Async2.bind Async2.result
    let e = Assert.Throws<OperationCanceledException>(fun () -> a |> asyncWaitWithCt ct |> ignore)
    Assert.Equal(ct, e.CancellationToken)

[<Fact>]
let ``Async2.bind propagates Cancellation (async)`` () =
    let cts = new CancellationTokenSource()
    let mutable binderWasCalled = false
    let a =
        async2 {
            do! Async2.Sleep 5000 }
        |> Async2.bind (fun () -> async2 { binderWasCalled <- true })
    let t = Async2.StartAsTask(a, cancellationToken = cts.Token)
    cts.Cancel()
    let e = Assert.ThrowsAsync<OperationCanceledException>(fun () -> t).Result
    //Assert.NotEqual(cts.Token, e.CancellationToken) //no longer
    Assert.False binderWasCalled


[<Fact>]
let ``Async2.ignore discards result (sync)`` () =
    let actual = Async2.result 42 |> Async2.ignore<int> |> asyncWait
    Assert.Equal((), actual)

[<Fact>]
let ``Async2.ignore discards result (async2)`` () =
    let tcs = TaskCompletionSource<int>()
    let t = async2 { return! tcs.Task |> Async2.AwaitTask } |> Async2.ignore<int> |> Async2.StartAsTask
    tcs.SetResult 42
    Assert.Equal((), t.Result)

[<Fact>]
let ``Async2.ignore propagates incoming exception (sync)`` () =
    let a = async2 { return failwith "boom" : int } |> Async2.ignore<int>
    let e = Assert.Throws<exn>(fun () -> a |> asyncWait)
    Assert.Equal("boom", e.Message)

[<Fact>]
let ``Async2.ignore propagates incoming exception (async2)`` () =
    let tcs = TaskCompletionSource<int>()
    let t = async2 { return! tcs.Task |> Async2.AwaitTask } |> Async2.ignore<int> |> Async2.StartAsTask
    tcs.SetException(Exception "boom")
    let e = Assert.ThrowsAsync<Exception>(fun () -> t).Result
    Assert.Equal("boom", e.Message)

[<Fact>]
let ``Async2.ignore propagates Cancellation (sync)`` () =
    let ct = CancellationToken true
    let a = Async2.result 2 |> Async2.ignore<int>
    let e = Assert.Throws<OperationCanceledException>(fun () -> a |> asyncWaitWithCt ct)
    Assert.Equal(ct, e.CancellationToken)

[<Fact>]
let ``Async2.ignore propagates Cancellation (async2)`` () =
    let mutable cancellationFailed = false
    let cts = new CancellationTokenSource()
    let a =
        async2 {
            do! Async2.Sleep 5000
            cancellationFailed <- true
            return 42 }
        |> Async2.ignore<int>
    let t = Async2.StartAsTask(a, cancellationToken = cts.Token)
    cts.Cancel()
    let e = Assert.ThrowsAsync<OperationCanceledException>(fun () -> t).Result
    //Assert.NotEqual(cts.Token, e.CancellationToken) // no longer
    Assert.False cancellationFailed


[<Fact>]
let ``Async2.catchWith passes through success (sync)`` () =
    let source = Async2.result 42
    let a = source |> Async2.catchWith (fun _ -> -1)
    Assert.Equal(42, asyncWait a)

[<Fact>]
let ``Async2.catchWith passes through success (async2)`` () = async2 {
    let tcs = TaskCompletionSource<int>()
    let! a = async2 { return! tcs.Task |> Async2.AwaitTask } |> Async2.catchWith (fun _ -> -1) |> Async2.StartChild
    tcs.SetResult 42
    let! res = a
    Assert.Equal(42, res) }

[<Fact>]
let ``Async2.catchWith recovers from exception (sync)`` () = async2 {
    let! actual =
        async2 { return failwith "boom" : int }
        |> Async2.catchWith (fun e -> Assert.Equal("boom", e.Message); -1)
    Assert.Equal(-1, actual) }

[<Fact>]
let ``Async2.catchWith recovers from exception (async2)`` () = async2 {
    let tcs = TaskCompletionSource<int>()
    let! a = async2 { return! tcs.Task |> Async2.AwaitTask } |> Async2.catchWith (fun _ -> -1) |> Async2.StartChild
    tcs.SetException(Exception "boom")
    let! result = a
    Assert.Equal(-1, result) }

[<Fact>]
let ``Async2.catchWith propagates Cancellation (sync)`` () =
    let mutable cancellationFailed = false
    let ct = CancellationToken true
    let a = async2 {
            do! Async2.Sleep 5000
            cancellationFailed <- true
            return 42 }
            |> Async2.catchWith (fun _ -> -1)
    let e = Assert.Throws<OperationCanceledException>(fun () -> a |> asyncWaitWithCt ct |> ignore)
    Assert.Equal(ct, e.CancellationToken)
    Assert.False cancellationFailed

[<Fact>]
let ``Async2.catchWith propagates Cancellation (async2)`` () =
    let mutable cancellationFailed = false
    let cts = new CancellationTokenSource()
    let a =
        async2 {
            do! Async2.Sleep 5000
            cancellationFailed <- true
            return 42 }
        |> Async2.catchWith (fun _ -> -1)
    let t = Async2.StartAsTask(a, cancellationToken = cts.Token)
    cts.Cancel()
    let e = Assert.ThrowsAsync<OperationCanceledException>(fun () -> t).Result
    ignore e
    //Assert.NotEqual(cts.Token, e.CancellationToken) //no longer


[<Fact>]
let ``Async2.catch returns Ok on success (sync)`` () =
    let actual = Async2.result 42 |> Async2.catch |> asyncWait
    Assert.Equal(Ok 42, actual)

[<Fact>]
let ``Async2.catch returns Ok on success (async2)`` () : unit =
    let tcs = TaskCompletionSource<int>()
    let t = async2 { return! tcs.Task |> Async2.AwaitTask } |> Async2.catch |> Async2.StartAsTask
    tcs.SetResult 42
    Assert.Equal(Ok 42, t.Result)

[<Fact>]
let ``Async2.catch returns Error on exception`` () =
    let a = async2 { return failwith "boom" : int } |> Async2.catch
    match a |> asyncWait with
    | Error ex -> Assert.Equal("boom", ex.Message)
    | Ok _ -> failwith "unexpected success"

[<Fact>]
let ``Async2.catch returns Error on exception (async2)`` () : unit =
    let a = async2 {
            do! Async2.Sleep 1
            return failwith "boom" } |> Async2.catch
    match a |> asyncWait with
    | Error ex -> Assert.Equal("boom", ex.Message)
    | Ok _ -> failwith "unexpected success"

[<Fact>]
let ``Async2.catch propagates Cancellation (sync)`` () =
    let ct = CancellationToken true
    let a = async2 {
            do! Async2.Sleep 5000 } |> Async2.catch
    let e = Assert.Throws<OperationCanceledException>(fun () -> a |> asyncWaitWithCt ct |> ignore)
    Assert.Equal(ct, e.CancellationToken)

[<Fact>]
let ``Async2.catch propagates Cancellation (async2)`` () =
    let cts = new CancellationTokenSource()
    let a = async2 {
            do! Async2.Sleep 5000 } |> Async2.catch
    let t = Async2.StartAsTask(a, cancellationToken = cts.Token)
    cts.Cancel()
    let e = Assert.ThrowsAsync<OperationCanceledException>(fun () -> t).Result
    //Assert.NotEqual(cts.Token, e.CancellationToken) // no longer
    ignore e

[<Fact>]
let ``Async2.empty returns unit`` () =
    let actual = Async2.empty |> asyncWait
    Assert.Equal((), actual)
    

[<Fact>]
let ``Async2.sequentialDo runs all tasks in order and returns unit`` () =
    let order = ResizeArray()
    let computations = [for i in 1..5 do async2 { order.Add i }]
    Async2.sequentialDo computations |> asyncWait |> ignore
    Assert.Equal<int seq>([ 1; 2; 3; 4; 5 ], order)

[<Fact>]
let ``Async2.sequentialDo runs computations one at a time`` () =
    let mutable concurrent = 0
    let mutable maxConcurrent = 0
    let computations =
        [for _ in 1..5 ->
            async2 {
                let n = Interlocked.Increment &concurrent
                if n > maxConcurrent then maxConcurrent <- n
                do! Async2.Sleep 1
                Interlocked.Decrement &concurrent |> ignore
            }]
    Async2.sequentialDo computations |> asyncWait |> ignore
    Assert.Equal(1, maxConcurrent)


[<Fact>]
let ``Async2.parallelLimit runs all computations`` () =
    let results =
        [for i in 1..5 do async2 { return i * i }]
        |> Async2.parallelLimit 2
        |> Async2.RunSynchronouslyImmediate
    Assert.True([| 1; 4; 9; 16; 25 |] = results)

[<Fact>]
let ``Async2.parallelLimit limits concurrency`` () =
    let mutable concurrent = 0
    let mutable maxConcurrent = 0
    let lockObj = obj()
    let a =
        [for _ in 1..10 do
            async2 {
                let n =
                    lock lockObj (fun () ->
                        concurrent <- concurrent + 1
                        if concurrent > maxConcurrent then maxConcurrent <- concurrent
                        concurrent)
                do! Async2.Sleep 1
                lock lockObj (fun () -> concurrent <- concurrent - 1) |> ignore
                return n
            }]
        |> Async2.parallelLimit 3
    a |> Async2.RunSynchronouslyImmediate |> ignore
    Assert.True(maxConcurrent <= 3, $"max concurrent was {maxConcurrent}, expected <= 3")

[<Fact>]
let ``Async2.parallelLimit with multiple failures yields single exception, not AggregateException`` () : Async2<unit> =
    async2 {
        use cts = new CancellationTokenSource()
        let firstStarted, secondStarted = TaskCompletionSource<unit>(), TaskCompletionSource<unit>()
        let releaseBoth = TaskCompletionSource<unit>()

        let! sut =
            Async2.parallelLimit 2 [ 
                async2 {
                    firstStarted.SetResult ()
                    do! releaseBoth.Task |> Async2.Await
                    return invalidOp "boom1"
                }
                async2 {
                    secondStarted.SetResult ()
                    do! releaseBoth.Task |> Async2.Await
                    return invalidArg "a" "boom2"
                }
            ]
            |> Async2.StartChild
        let! _ = Task.WhenAll(firstStarted.Task, secondStarted.Task) |> Async2.Await
        releaseBoth.SetResult ()

        match! Async2.catch sut with
        | Error (:? InvalidOperationException) | Error (:? ArgumentException) -> ()  // either sibling may win
        | Error (:? AggregateException) -> failwith "should be a single exception, not an AggregateException"
        | x -> failwith $"unexpected %A{x}"
    }

[<Fact>]
let ``Async2.catch does not Unwrap an AggregateException with a single inner`` () =
    let sut = async2 { return raise (AggregateException("boom", exn "inner" )) } |> Async2.catch
    match sut |> Async2.RunSynchronouslyImmediate with
    | Error (:? AggregateException as ex) ->
        Assert.Equal(1, ex.InnerExceptions.Count)
        // on net48, Message renders as "boom", on others it's "boom (inner)"
        Assert.True(ex.Message.StartsWith "boom", ex.Message) 
    | x -> failwith $"unexpected %A{x}"

[<Fact>]
let ``Async2.parallelDoLimit runs all computations and returns unit`` () =
    let mutable count = 0
    seq {
        for i in 1..5 do
            async2 { Interlocked.Increment &count |> ignore } }
    |> Async2.parallelDoLimit 2
    |> Async2.RunSynchronouslyImmediate
    Assert.Equal(5, count)
