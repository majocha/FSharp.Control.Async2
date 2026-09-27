// Ported from FSharp.Control.AsyncSeq (see src\FSharp.Control.AsyncSeq2\ASYNCSEQ-LICENSE.txt).
module FSharp.Core.UnitTests.Control.AsyncSeq2UpstreamTests

open Xunit
open Microsoft.FSharp.Control
open System
open System.Threading
open System.Threading.Channels

type AsyncOps = AsyncOps with
  static member unit : Async2<unit> = async2 { return () }
  static member never = async2 { do! Async2.Sleep(-1) }
  static member timeoutMs (timeoutMs:int) (a:Async2<'a>) =
    Async2.StartTaskImmediate(fun ct -> task {
      use linked = CancellationTokenSource.CreateLinkedTokenSource ct
      linked.CancelAfter timeoutMs
      return! Async2.StartAsTask(a, cancellationToken=linked.Token)
    })

module AsyncSeq2Helpers =
  [<GeneralizableValue>]
  let never<'a> : AsyncSeq2<'a> = asyncSeq2 {
    do! AsyncOps.never
    yield invalidOp "" }


let DEFAULT_TIMEOUT_MS = 500

let randomDelayMs (minMs:int) (maxMs:int) (s:AsyncSeq2<'a>) =
  let rand = new Random(int DateTime.Now.Ticks)
  let randSleep = async2 { do! Async2.Sleep(rand.Next(minMs, maxMs)) }
  AsyncSeq2.zipWith (fun _ a -> a) (AsyncSeq2.replicateInfiniteAsync (fun () -> randSleep)) s

let randomDelayDefault (s:AsyncSeq2<'a>) =
  randomDelayMs 0 10 s

let randomDelayMax m (s:AsyncSeq2<'a>) =
  randomDelayMs 0 m s

let catch (f:'a -> 'b) : 'a -> Choice<'b, exn> =
  fun a ->
    try f a |> Choice1Of2
    with ex -> ex |> Choice2Of2

let rec IsCancellationExn (e:exn) =
  match e with
  | :? OperationCanceledException -> true
  | :? TimeoutException -> true
  | :? AggregateException as x -> x.InnerExceptions |> Seq.filter (IsCancellationExn) |> Seq.isEmpty |> not
  | _ -> false

let AreCancellationExns (e1:exn) (e2:exn) =
  IsCancellationExn e1 && IsCancellationExn e2

/// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
let EQ (a:AsyncSeq2<'a>) (b:AsyncSeq2<'a>) =
  let exp = a |> AsyncSeq2.toListSynchronously
  let act = b |> AsyncSeq2.toListSynchronously
  if (exp = act) then true
  else
    printfn "expected=%A" exp
    printfn "actual=%A" act
    false

let runTimeout (timeoutMs:int) (a:Async2<'a>) : 'a =
  Async2.RunSynchronously (a, timeoutMs)

let runTest a = runTimeout 1000 a

let rec disaggregate (exn : exn) : exn =
  match exn with
  | :? AggregateException as agg ->
    match Seq.tryExactlyOne agg.InnerExceptions with
    | Some inner -> disaggregate inner
    | None -> exn
  | _ -> exn

type Assert =
  static member AreEqual (expected:'T, actual:'T) =
    Xunit.Assert.True((expected = actual), sprintf "expected=%A actual=%A" expected actual)

  static member True(value:bool, ?message:string) =
    Xunit.Assert.True(value, defaultArg message "")

  static member False(value:bool, ?message:string) =
    Xunit.Assert.False(value, defaultArg message "")
  static member IsTrue(value:bool, ?message:string) =
    Xunit.Assert.True(value, defaultArg message "")
  static member IsFalse(value:bool, ?message:string) =
    Xunit.Assert.False(value, defaultArg message "")
  static member Fail(?message:string) = Xunit.Assert.Fail(defaultArg message "Assertion failed")
  static member Throws<'T when 'T :> exn>(action:unit -> unit) =
    Xunit.Assert.Throws<'T>(Action(action))
  static member Catch<'T when 'T :> exn>(action:unit -> unit) =
    Xunit.Assert.ThrowsAny<'T>(Action(action))
  static member DoesNotThrow(action:unit -> unit) = action ()
  static member Less(expected:'T, actual:'T) = Xunit.Assert.True(expected < actual)
  static member LessOrEqual(expected:'T, actual:'T, ?message:string) =
    Xunit.Assert.True(expected <= actual, defaultArg message "")
  static member Greater(expected:'T, actual:'T) = Xunit.Assert.True(expected > actual)

  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>) =
    Assert.AreEqual (expected, actual, timeout=DEFAULT_TIMEOUT_MS, exnEq=(fun _ _ -> true), message=null)

  /// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>, message:string) =
    Assert.AreEqual (expected, actual, timeout=DEFAULT_TIMEOUT_MS, exnEq=(fun _ _ -> true), message=message)

  /// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>, exnEq) =
    Assert.AreEqual (expected, actual, timeout=DEFAULT_TIMEOUT_MS, exnEq=exnEq, message=null)

  /// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>, timeout) =
    Assert.AreEqual (expected, actual, timeout=timeout, exnEq=(fun _ _ -> true), message=null)

  /// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>, timeout, exnEq) =
    Assert.AreEqual (expected, actual, timeout=timeout, exnEq=exnEq, message=null)

  /// Determines equality of two async sequences by convering them to lists, ignoring side-effects.
  /// Exceptions are caught and compared for equality.
  /// Timeouts ensure liveness.
  static member AreEqual (expected:AsyncSeq2<'a>, actual:AsyncSeq2<'a>, timeout, exnEq:exn -> exn -> bool, message:string) =
    let expected = expected |> AsyncSeq2.toListAsync |> AsyncOps.timeoutMs timeout |> Async2.Catch
    let expected = Async2.RunSynchronously (expected)
    let actual = actual |> AsyncSeq2.toListAsync |> AsyncOps.timeoutMs timeout |> Async2.Catch
    let actual = Async2.RunSynchronously (actual)
    let message =
      if message = null then sprintf "expected=%A actual=%A" expected actual
      else sprintf "message=%s expected=%A actual=%A" message expected actual
    match expected,actual with
    | Choice1Of2 exp, Choice1Of2 act ->
      Assert.True((exp = act), message)
    | Choice2Of2 exp, Choice2Of2 act ->
      Assert.True((exnEq exp act), message)
    | _ ->
      Assert.Fail(message)

  static member AreEqual (expected:unit -> 'a, actual:unit -> 'a) =
    let expected = (catch expected) ()
    let actual = (catch actual) ()
    let message = sprintf "expected=%A actual=%A" expected actual
    match expected,actual with
    | Choice1Of2 exp, Choice1Of2 act ->
      Assert.True((exp = act), message)
    | Choice2Of2 exp, Choice2Of2 act ->
      ()
    | _ ->
      Assert.Fail(message)

[<Fact>]
let ``AsyncSeq2Helpers.never should equal itself`` () =
  Assert.AreEqual(AsyncSeq2Helpers.never<int>, AsyncSeq2Helpers.never<int>, timeout=100, exnEq=AreCancellationExns)

[<Fact>]
let ``AsyncSeq2.toArraySynchronously``() =
  let s = asyncSeq2 {
    yield 1
    yield 2
    yield 3
  }
  let a = s |> AsyncSeq2.toArraySynchronously
  Assert.True(([|1;2;3|] = a))


[<Fact>]
let ``AsyncSeq2.toListSynchronously``() =
  let s = asyncSeq2 {
    yield 1
    yield 2
    yield 3
  }
  let a = s |> AsyncSeq2.toListSynchronously
  Assert.True(([1;2;3] = a))


[<Fact>]
let ``asyncSeq2 yield! seq works``() =
  let items = seq { 1; 2; 3 }
  let s = asyncSeq2 {
    yield! items
  }
  let a = s |> AsyncSeq2.toListSynchronously
  Assert.True(([1;2;3] = a))

[<Fact>]
let ``asyncSeq2 yield! seq combines with other yields``() =
  let items = seq { 2; 3 }
  let s = asyncSeq2 {
    yield 1
    yield! items
    yield 4
  }
  let a = s |> AsyncSeq2.toListSynchronously
  Assert.True(([1;2;3;4] = a))

[<Fact>]
let ``asyncSeq2 yield! list works``() =
  let items = [1; 2; 3]
  let s = asyncSeq2 {
    yield! items
  }
  let a = s |> AsyncSeq2.toListSynchronously
  Assert.True(([1;2;3] = a))

[<Fact>]
let ``asyncSeq2 yield! list combines with other yields``() =
  let items = [2; 3]
  let s = asyncSeq2 {
    yield 1
    yield! items
    yield 4
  }
  let a = s |> AsyncSeq2.toListSynchronously
  Assert.True(([1;2;3;4] = a))

[<Fact>]
let ``AsyncSeq2.concatSeq works``() =
  let ls = [ [1;2] ; [3;4] ]
  let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.concatSeq
  let expected = ls |> List.concat |> AsyncSeq2.ofSeq
  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.sum works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.sum |> Async2.RunSynchronously
      let expected = ls |> List.sum
      Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.min returns minimum element``() =
  for i in 1 .. 10 do
      let ls = [ 1 .. i ] |> List.rev
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.min |> Async2.RunSynchronously
      Assert.AreEqual(1, actual)

[<Fact>]
let ``AsyncSeq2.min raises on empty sequence``() =
  Assert.Throws<System.ArgumentException>(fun () ->
      (AsyncSeq2.empty<int> ()) |> AsyncSeq2.min |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.max returns maximum element``() =
  for i in 1 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.max |> Async2.RunSynchronously
      Assert.AreEqual(i, actual)

[<Fact>]
let ``AsyncSeq2.max raises on empty sequence``() =
  Assert.Throws<System.ArgumentException>(fun () ->
      (AsyncSeq2.empty<int> ()) |> AsyncSeq2.max |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.minBy returns element with minimum projected value``() =
  let ls = [ ("b", 2); ("a", 1); ("c", 3) ]
  let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.minBy snd |> Async2.RunSynchronously
  Assert.AreEqual(("a", 1), actual)

[<Fact>]
let ``AsyncSeq2.maxBy returns element with maximum projected value``() =
  let ls = [ ("b", 2); ("a", 1); ("c", 3) ]
  let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.maxBy snd |> Async2.RunSynchronously
  Assert.AreEqual(("c", 3), actual)

[<Fact>]
let ``AsyncSeq2.minByAsync uses async projection``() =
  let ls = [ 3; 1; 4; 1; 5; 9 ]
  let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.minByAsync (fun x -> (fun value -> async2 { return value }) x) |> Async2.RunSynchronously
  Assert.AreEqual(1, actual)

[<Fact>]
let ``AsyncSeq2.maxByAsync uses async projection``() =
  let ls = [ 3; 1; 4; 1; 5; 9 ]
  let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.maxByAsync (fun x -> (fun value -> async2 { return value }) x) |> Async2.RunSynchronously
  Assert.AreEqual(9, actual)

[<Fact>]
let ``AsyncSeq2.sumBy works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.sumBy float |> Async2.RunSynchronously
      let expected = ls |> List.sumBy float
      Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.sumByAsync works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.sumByAsync (float >> (fun value -> async2 { return value })) |> Async2.RunSynchronously
      let expected = ls |> List.sumBy float
      Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.average works``() =
  for i in 1 .. 10 do
      let ls = [ 1.0 .. float i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.average |> Async2.RunSynchronously
      let expected = ls |> List.average
      Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.average raises on empty sequence``() =
  Assert.Throws<System.ArgumentException>(fun () ->
      (AsyncSeq2.empty<float> ()) |> AsyncSeq2.average |> Async2.RunSynchronously |> ignore
  ) |> ignore

[<Fact>]
let ``AsyncSeq2.averageBy works``() =
  for i in 1 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.averageBy float |> Async2.RunSynchronously
      let expected = ls |> List.averageBy float
      Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.averageByAsync works``() =
  for i in 1 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.averageByAsync (float >> (fun value -> async2 { return value })) |> Async2.RunSynchronously
      let expected = ls |> List.averageBy float
      Assert.AreEqual(expected, actual)


[<Fact>]
let ``AsyncSeq2.length works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.length |> Async2.RunSynchronously |> int32
      let expected = ls |> List.length
      Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.contains works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.contains j |> Async2.RunSynchronously
          let expected = ls |> List.exists (fun x -> x = j)
          Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.tryPick works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.tryPick (fun x -> if x = j then Some (string (x+1)) else None) |> Async2.RunSynchronously
          let expected = ls |> Seq.tryPick (fun x -> if x = j then Some (string (x+1)) else None)
          Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.pick works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let chooser x = if x = j then Some (string (x+1)) else None
          let actual () = AsyncSeq2.ofSeq ls |> AsyncSeq2.pick chooser |> Async2.RunSynchronously
          let expected () = ls |> Seq.pick chooser
          Assert.AreEqual(actual, expected)

[<Fact>]
let ``AsyncSeq2.tryFind works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.tryFind (fun x -> x = j) |> Async2.RunSynchronously
          let expected = ls |> Seq.tryFind (fun x -> x = j)
          Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.exists works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.exists (fun x -> x = j) |> Async2.RunSynchronously
          let expected = ls |> Seq.exists (fun x -> x = j)
          Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.forall works``() =
  for i in 0 .. 10 do
      let ls = [ 1 .. i ]
      for j in [0;i;i+1] do
          let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.forall (fun x -> x = j) |> Async2.RunSynchronously
          let expected = ls |> Seq.forall (fun x -> x = j)
          Assert.True((expected = actual))
//[<Fact>]
//let ``AsyncSeq2.cache works``() =
//  for n in 0 .. 10 do
//          let ls = [ for i in 1 .. n do for j in 1 .. i do yield i ]
//          let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.cache
//          let expected = ls |> AsyncSeq2.ofSeq
//          Assert.True(EQ expected actual)

let shouldEqual expected actual msg =
  if expected <> actual then
    printfn "EXPECTED=%A" expected
    printfn "ACTUAL=%A" actual
    match msg with
    | Some msg -> Assert.Fail msg
    | None -> Assert.Fail ()

[<Fact>]
let ``AsyncSeq2.cache should work``() =
  for N in [0;1;2;3;100] do
    let expected = List.init N id
    let effects = ref 0
    let s = asyncSeq2 {
      for item in expected do
        yield item
        do! Async2.Sleep 1
        incr effects }
    let cached = s |> AsyncSeq2.cache
    let actual1,actual2 =
      (cached, cached)
      ||> AsyncSeq2.zipParallel
      |> AsyncSeq2.toListSynchronously
      |> List.unzip
    shouldEqual expected actual1 (Some "cached sequence1 was different")
    shouldEqual expected actual2 (Some "cached sequence2 was different")
    shouldEqual expected.Length !effects (Some "iterating cached sequence resulted in multiple iterations of source")

[<Fact>]
let ``AsyncSeq2.cache does not slow down late consumers``() =
    let src =
        AsyncSeq2.initInfiniteAsync (fun _ -> Async2.Sleep 10)
        |> AsyncSeq2.cache
    let consume initialDelay amount =
        async2 {
            do! Async2.Sleep (initialDelay:int)
            let timing = System.Diagnostics.Stopwatch.StartNew()
            let! _ =
                src
                |> AsyncSeq2.truncate amount
                |> AsyncSeq2.length
            return timing.Elapsed.TotalSeconds
        }
    let times =
        Async2.Parallel [
            // The first to start will take 10s to consume 10 items
            consume 0 10
            // The second should take no time to consume 5 items, starting 5s later, as the first five items have already been cached.
            consume 50 5
        ]
        |> Async2.RunSynchronously
    Assert.LessOrEqual(abs(times.[0] - 0.1), 0.1, "Sanity check: lead consumer should take ~100ms")
    Assert.LessOrEqual(times.[1], 0.1, "Test purpose: follower should only read cached items")

[<Fact>]
let ``AsyncSeq2.unfoldAsync``() =
  let gen s =
    if s < 3 then (s,s + 1) |> Some
    else None
  let expected = Seq.unfold gen 0 |> AsyncSeq2.ofSeq
  let actual = AsyncSeq2.unfoldAsync (gen >> (fun value -> async2 { return value })) 0
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.unfold``() =
  let n = 3
  let chooser x = if x % 2 = 0 then Some x else None
  let gen s =
    if s < n then (s,s + 1) |> Some
    else None
  let expected = Seq.unfold gen 0 |> Seq.choose chooser |> AsyncSeq2.ofSeq
  let actual = AsyncSeq2.unfold gen 0 |> AsyncSeq2.choose chooser
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.unfold choose``() =
  let gen s =
    if s < 3 then (s,s + 1) |> Some
    else None
  let expected = Seq.unfold gen 0 |> AsyncSeq2.ofSeq
  let actual = AsyncSeq2.unfold gen 0
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.interleaveChoice``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq [1;2;3]
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([Choice1Of2 "a" ; Choice2Of2 1 ; Choice1Of2 "b" ; Choice2Of2 2 ; Choice1Of2 "c" ; Choice2Of2 3] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveChoice second smaller``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq [1]
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([Choice1Of2 "a" ; Choice2Of2 1 ; Choice1Of2 "b" ; Choice1Of2 "c" ] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveChoice second empty``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq []
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([Choice1Of2 "a" ; Choice1Of2 "b" ; Choice1Of2 "c" ] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveChoice both empty``() =
  let s1 = AsyncSeq2.ofSeq<int> []
  let s2 = AsyncSeq2.ofSeq<int> []
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([ ] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveChoice first smaller``() =
  let s1 = AsyncSeq2.ofSeq ["a"]
  let s2 = AsyncSeq2.ofSeq [1;2;3]
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([Choice1Of2 "a" ; Choice2Of2 1 ; Choice2Of2 2 ; Choice2Of2 3] = merged)



[<Fact>]
let ``AsyncSeq2.interleaveChoice first empty``() =
  let s1 = AsyncSeq2.ofSeq []
  let s2 = AsyncSeq2.ofSeq [1;2;3]
  let merged = AsyncSeq2.interleaveChoice s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([Choice2Of2 1 ; Choice2Of2 2 ; Choice2Of2 3] = merged)


[<Fact>]
let ``AsyncSeq2.interleave``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq ["1";"2";"3"]
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True(["a" ; "1" ; "b" ; "2" ; "c" ; "3"] = merged)

[<Fact>]
let ``AsyncSeq2.interleave second smaller``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq ["1"]
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True(["a" ; "1" ; "b" ; "c" ] = merged)

[<Fact>]
let ``AsyncSeq2.interleave second empty``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let s2 = AsyncSeq2.ofSeq []
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True(["a" ; "b" ; "c" ] = merged)

[<Fact>]
let ``AsyncSeq2.interleave both empty``() =
  let s1 = AsyncSeq2.ofSeq<int> []
  let s2 = AsyncSeq2.ofSeq<int> []
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([ ] = merged)

[<Fact>]
let ``AsyncSeq2.interleave first smaller``() =
  let s1 = AsyncSeq2.ofSeq ["a"]
  let s2 = AsyncSeq2.ofSeq ["1";"2";"3"]
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True(["a" ; "1" ; "2" ; "3"] = merged)



[<Fact>]
let ``AsyncSeq2.interleave first empty``() =
  let s1 = AsyncSeq2.ofSeq []
  let s2 = AsyncSeq2.ofSeq [1;2;3]
  let merged = AsyncSeq2.interleave s1 s2 |> AsyncSeq2.toListSynchronously
  Assert.True([1 ; 2 ; 3] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveMany empty``() =
  let merged = AsyncSeq2.interleaveMany [] |> AsyncSeq2.toListSynchronously
  Assert.True(List.isEmpty merged)

[<Fact>]
let ``AsyncSeq2.interleaveMany 1``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b";"c"]
  let merged = AsyncSeq2.interleaveMany [s1] |> AsyncSeq2.toListSynchronously
  Assert.True(["a" ; "b" ; "c" ] = merged)

[<Fact>]
let ``AsyncSeq2.interleaveMany 3``() =
  let s1 = AsyncSeq2.ofSeq ["a";"b"]
  let s2 = AsyncSeq2.ofSeq ["i";"j";"k";"l"]
  let s3 = AsyncSeq2.ofSeq ["x";"y";"z"]
  let merged = AsyncSeq2.interleaveMany [s1;s2;s3] |> AsyncSeq2.toListSynchronously
  Assert.True(["a"; "x"; "i"; "y"; "b"; "z"; "j"; "k"; "l"] = merged)


[<Fact>]
let ``AsyncSeq2.chunkBySize``() =
  let s = asyncSeq2 {
    yield 1
    yield 2
    yield 3
    yield 4
    yield 5
  }
  let s' = s |> AsyncSeq2.chunkBySize 2 |> AsyncSeq2.toListSynchronously
  Assert.True(([[|1;2|];[|3;4|];[|5|]] = s'))

[<Fact>]
let ``AsyncSeq2.chunkBySize various sizes``() =
  for sz in 0 .. 10 do
      let s = asyncSeq2 {
        for i in 1 .. sz do
           yield i
      }
      let s' = s |> AsyncSeq2.chunkBySize 1 |> AsyncSeq2.toListSynchronously
      Assert.True(([for i in 1 .. sz -> [|i|]] = s'))

[<Fact>]
let ``AsyncSeq2.chunkBySize empty``() =
  let s = (AsyncSeq2.empty<int> ())
  let s' = s |> AsyncSeq2.chunkBySize 2 |> AsyncSeq2.toListSynchronously
  Assert.True(([] = s'))


[<Fact>]
let ``AsyncSeq2.bufferByTimeAndCount``() =
  let s = asyncSeq2 {
    yield 1
    yield 2
    yield 3
    do! Async2.Sleep 250
    yield 4
    yield 5
  }
  let actual = AsyncSeq2.bufferByCountAndTime 2 50 s |> AsyncSeq2.toListSynchronously
  Assert.True((actual = [ [|1;2|] ; [|3|] ; [|4;5|] ]))

[<Fact>]
let ``AsyncSeq2.bufferByCountAndTime various sizes``() =
  for sz in 0 .. 10 do
      let s = asyncSeq2 {
        for i in 1 .. sz do
           yield i
      }
      let s' = s |> AsyncSeq2.bufferByCountAndTime 1 1 |> AsyncSeq2.toListSynchronously
      Assert.True(([for i in 1 .. sz -> [|i|]] = s'))

[<Fact>]
let ``AsyncSeq2.bufferByTimeAndCount empty``() =
  let s = (AsyncSeq2.empty<int> ())
  let actual = AsyncSeq2.bufferByCountAndTime 2 10 s |> AsyncSeq2.toListSynchronously
  Assert.True((actual = []))

//[<Fact>]
//let ``AsyncSeq2.bufferByTime`` () =
//
//  let Y = Choice1Of2
//  let S = Choice2Of2
//
//  let timeMs = 500
//
//  let inp0 = [ ]
//  let exp0 = [ ]
//
//  let inp1 = [ Y 1 ; Y 2 ; S timeMs ; Y 3 ; Y 4 ; S timeMs ; Y 5 ; Y 6 ]
//  let exp1 = [ [1;2] ; [3;4] ; [5;6] ]
//
////  let inp2 : Choice<int, int> list = [ S 500 ]
////  let exp2 : int list list = [ [] ; [] ; [] ; []  ]
//
//  let toSeq (xs:Choice<int, int> list) = asyncSeq2 {
//    for x in xs do
//      match x with
//      | Choice1Of2 v -> yield v
//      | Choice2Of2 s -> do! Async2.Sleep s }
//
//  for (inp,exp) in [ (inp0,exp0) ; (inp1,exp1) ] do
//
//    let actual =
//      toSeq inp
//      |> AsyncSeq2.bufferByTime (timeMs - 5)
//      |> AsyncSeq2.map List.ofArray
//      |> AsyncSeq2.toListSynchronously
//
//    //let ls = toSeq inp |> AsyncSeq2.toListSynchronously
//    //let actualLs = actual |> List.concat
//
//    Assert.True ((actual = exp))

// WARNING: Too timing sensitive
//let rec prependToAll (a:'a) (ls:'a list) : 'a list =
//  match ls with
//  | [] -> []
//  | hd::tl -> a::hd::prependToAll a tl
//
//let rec intersperse (a:'a) (ls:'a list) : 'a list =
//  match ls with
//  | [] -> []
//  | hd::tl -> hd::prependToAll a tl
//
//let intercalate (l:'a list) (xs:'a list list) : 'a list =
//  intersperse l xs |> List.concat
//
//let batch (size:int) (ls:'a list) : 'a list list =
//  let rec go batch ls =
//    match ls with
//    | [] -> [List.rev batch]
//    | _ when List.length batch = size -> (List.rev batch)::go [] ls
//    | hd::tl -> go (hd::batch) tl
//  go [] ls
//
//[<Fact>]
//let ``AsyncSeq2.bufferByTime2`` () =
//
//  let Y = Choice1Of2
//  let S = Choice2Of2
//  let sleepMs = 100
//
//  let toSeq (xs:Choice<int, int> list) = asyncSeq2 {
//    for x in xs do
//      match x with
//      | Choice1Of2 v -> yield v
//      | Choice2Of2 s -> do! Async2.Sleep s }
//
//  for (size,batchSize) in [ (0,0) ; (10,2) ; (100,2) ] do
//
//    let expected =
//      List.init size id
//      |> batch batchSize
//
//    let actual =
//      expected
//      |> List.map (List.map Y)
//      |> intercalate [S sleepMs]
//      |> toSeq
//      |> AsyncSeq2.bufferByTime sleepMs
//      |> AsyncSeq2.map List.ofArray
//      |> AsyncSeq2.toListSynchronously
//
//    Assert.True ((actual = expected))

[<Fact>]
let ``AsyncSeq2.while do CE is possible`` () =
  let mutable i = 0
  let mutable foo = true
  let something =
    async2 {
      i <- i + 1
      foo <- i < 3
      do! Async2.Sleep 10
    }
  let actual =
    asyncSeq2 {
      yield "a"

      while foo do
        do! something

      yield "b"
      yield "c"
    }
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously

  Assert.AreEqual([ "a"; "b"; "c" ], actual)

[<Fact>]
let ``AsyncSeq2.bufferByCountAndTime should not block`` () =
  let op =
    asyncSeq2 {
      while true do
      do! Async2.Sleep 10
      yield 0
    }
    |> AsyncSeq2.bufferByCountAndTime 10 100
    |> AsyncSeq2.take 3
    |> AsyncSeq2.iter (ignore)

  // should return immediately
  // while a blocking call would take > 3sec
  let watch = System.Diagnostics.Stopwatch.StartNew()
  let cts = new CancellationTokenSource()
  Async2.StartWithContinuations(op, ignore, ignore, ignore, cts.Token)
  watch.Stop()
  cts.Cancel(false)
  Assert.Less (watch.ElapsedMilliseconds, 100L)

[<Fact>]
let ``AsyncSeq2.bufferByTime should not block`` () =
  let op =
    asyncSeq2 {
      while true do
      do! Async2.Sleep 10
      yield 0
    }
    |> AsyncSeq2.bufferByTime 100
    |> AsyncSeq2.take 3
    |> AsyncSeq2.iter (ignore)

  // should return immediately
  // while a blocking call would take > 3sec
  let watch = System.Diagnostics.Stopwatch.StartNew()
  let cts = new CancellationTokenSource()
  Async2.StartWithContinuations(op, ignore, ignore, ignore, cts.Token)
  watch.Stop()
  cts.Cancel(false)
  Assert.Less (watch.ElapsedMilliseconds, 100L)

//  let s = asyncSeq2 {
//    yield 1
//    yield 2
//    do! Async2.Sleep 10
//    yield 3
//    yield 4
//    do! Async2.Sleep 10
//    yield 5
//    yield 6
//  }

  //let actual =
  //  s
  //  |> AsyncSeq2.bufferByTime 100
  //  |> AsyncSeq2.map (List.ofArray)
  //  |> AsyncSeq2.toListSynchronously

  //let expected = [ [1;2] ; [3;4] ; [5;6] ]

  //Assert.True ((actual = expected))

[<Fact>]
let ``try finally works no exception``() =
  let x = ref 0
  let s = asyncSeq2 {
    try yield 1
    finally x := x.Value + 3
  }
  Assert.True(x.Value = 0)
  let s1 = s |> AsyncSeq2.toListSynchronously
  Assert.True(x.Value = 3)
  let s2 = s |> AsyncSeq2.toListSynchronously
  Assert.True(x.Value = 6)

[<Fact>]
let ``try finally works exception``() =
  let x = ref 0
  let s = asyncSeq2 {
    try
      try yield 1
          failwith "fffail"
      finally x := x.Value + 1
    finally x := x.Value + 2
  }
  Assert.True(x.Value = 0)
  let s1 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s1 = []))
  Assert.True(x.Value = 3)
  let s2 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s2 = []))
  Assert.True(x.Value = 6)

[<Fact>]
let ``try with works exception``() =
  let x = ref 0
  let s = asyncSeq2 {
    try failwith "ffail"
    with e -> x := x.Value + 3
  }
  Assert.True(x.Value = 0)
  let s1 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s1 = []))
  Assert.True(x.Value = 3)
  let s2 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s2 = []))
  Assert.True(x.Value = 6)

[<Fact>]
let ``try with works no exception``() =
  let x = ref 0
  let s = asyncSeq2 {
    try yield 1
    with e -> x := x.Value + 3
  }
  Assert.True(x.Value = 0)
  let s1 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s1 = [1]))
  Assert.True(x.Value = 0)
  let s2 = try s |> AsyncSeq2.toListSynchronously with _ -> []
  Assert.True((s2 = [1]))
  Assert.True(x.Value = 0)

[<Fact>]
let ``AsyncSeq2.zip``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          let a = la |> AsyncSeq2.ofSeq
          let b = lb |> AsyncSeq2.ofSeq
          let actual = AsyncSeq2.zip a b
          let expected = Seq.zip la lb |> AsyncSeq2.ofSeq
          Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.zipWithAsync``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          let a = la |> AsyncSeq2.ofSeq
          let b = lb |> AsyncSeq2.ofSeq
          let actual = AsyncSeq2.zipWithAsync (fun a b -> a + b |> (fun value -> async2 { return value })) a b
          let expected = Seq.zip la lb |> Seq.map ((<||) (+)) |> AsyncSeq2.ofSeq
          Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.zipWithAsyncParallel``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          let a = la |> AsyncSeq2.ofSeq
          let b = lb |> AsyncSeq2.ofSeq
          let actual = AsyncSeq2.zipWithAsyncParallel (fun a b -> a + b |> (fun value -> async2 { return value })) a b
          let expected = Seq.zip la lb |> Seq.map ((<||) (+)) |> AsyncSeq2.ofSeq
          Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.zip3``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          for lc in [ []; [1]; [1;2;3;4;5] ] do
              let a = la |> AsyncSeq2.ofSeq
              let b = lb |> AsyncSeq2.ofSeq
              let c = lc |> AsyncSeq2.ofSeq
              let actual = AsyncSeq2.zip3 a b c
              let expected = Seq.zip3 la lb lc |> AsyncSeq2.ofSeq
              Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.zipWith3``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          for lc in [ []; [1]; [1;2;3;4;5] ] do
              let a = la |> AsyncSeq2.ofSeq
              let b = lb |> AsyncSeq2.ofSeq
              let c = lc |> AsyncSeq2.ofSeq
              let actual = AsyncSeq2.zipWith3 (fun a b c -> a + b + c) a b c
              let expected = Seq.zip3 la lb lc |> Seq.map (fun (a,b,c) -> a+b+c) |> AsyncSeq2.ofSeq
              Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.zipWithAsync3``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          for lc in [ []; [1]; [1;2;3;4;5] ] do
              let a = la |> AsyncSeq2.ofSeq
              let b = lb |> AsyncSeq2.ofSeq
              let c = lc |> AsyncSeq2.ofSeq
              let actual = AsyncSeq2.zipWithAsync3 (fun a b c -> a + b + c |> (fun value -> async2 { return value })) a b c
              let expected = Seq.zip3 la lb lc |> Seq.map (fun (a,b,c) -> a+b+c) |> AsyncSeq2.ofSeq
              Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.append works``() =
  for la in [ []; [1]; [1;2;3;4;5] ] do
     for lb in [ []; [1]; [1;2;3;4;5] ] do
          let a = la |> AsyncSeq2.ofSeq
          let b = lb |> AsyncSeq2.ofSeq
          let actual = AsyncSeq2.append a b
          let expected = List.append la lb |> AsyncSeq2.ofSeq
          Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skipWhileAsync``() =
  for ls in [ []; [1]; [3]; [1;2;3;4;5] ] do
      let p i = i <= 2
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.skipWhileAsync (p >> (fun value -> async2 { return value }))
      let expected = ls |> Seq.skipWhile p |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.takeWhileAsync``() =
  for ls in [ []; [1]; [1;2;3;4;5] ] do
      let p i = i < 4
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.takeWhileAsync (p >> (fun value -> async2 { return value }))
      let expected = ls |> Seq.takeWhile p |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.takeWhileInclusive``() =
  for ls in [ []; [1]; [4]; [4;5]; [1;2;3;4;5] ] do
      let p i = i < 4
      let pInclusive i = i <= 4
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.takeWhileInclusive p
      let expected = ls |> Seq.filter(pInclusive) |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.take 3 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 3
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.take c
  let expected = ls |> Seq.take c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.take 0 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 0
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.take c
  let expected = ls |> Seq.take c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.take 5 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 5
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.take c
  let expected = ls |> Seq.take c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skip 3 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 3
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.skip c
  let expected = ls |> Seq.skip c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skip 0 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 0
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.skip c
  let expected = ls |> Seq.skip c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skip 5 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 5
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.skip c
  let expected = ls |> Seq.skip c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.skip 1 of 5``() =
  let ls = [1;2;3;4;5]
  let c = 1
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.skip c
  let expected = ls |> Seq.skip c |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)



[<Fact>]
let ``AsyncSeq2.threadStateAsync``() =
  let ls = [1;2;3;4;5]
  let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.threadStateAsync (fun i a -> (fun value -> async2 { return value })(i + a, i + 1)) 0
  let expected = [1;3;5;7;9] |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.scanAsync``() =
  for ls in [ []; [1]; [3]; [1;2;3;4;5] ] do
      let f i a = i + a
      let z = 0
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.scanAsync (fun i a -> f i a |> (fun value -> async2 { return value })) z
      let expected = ls |> List.scan f z |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.scan``() =
  for ls in [ []; [1]; [3]; [1;2;3;4;5] ] do
      let f i a = i + a
      let z = 0
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.scan (fun i a -> f i a) z
      let expected = ls |> List.scan f z |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.foldAsync``() =
  for ls in [ []; [1]; [3]; [1;2;3;4;5] ] do
      let f i a = i + a
      let z = 0
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.foldAsync (fun i a -> f i a |> (fun value -> async2 { return value })) z |> Async2.RunSynchronously
      let expected = ls |> Seq.fold f z
      Assert.True((expected = actual))


[<Fact>]
let ``AsyncSeq2.filterAsync``() =
  for ls in [ []; [1]; [4]; [1;2;3;4;5] ] do
      let p i = i > 3
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.filterAsync (p >> (fun value -> async2 { return value }))
      let expected = ls |> Seq.filter p |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.filter``() =
  for ls in [ []; [1]; [4]; [1;2;3;4;5] ] do
      let p i = i > 3
      let actual = ls |> AsyncSeq2.ofSeq |> AsyncSeq2.filter p
      let expected = ls |> Seq.filter p |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.merge``() =
  let ls1 = [1;2;3;4;5]
  let ls2 = [6;7;8;9;10]
  let actual = AsyncSeq2.merge (AsyncSeq2.ofSeq ls1) (AsyncSeq2.ofSeq ls2) |> AsyncSeq2.toListSynchronously |> Set.ofList
  let expected = ls1 @ ls2 |> Set.ofList
  Assert.True((expected = actual))

[<Fact>]
let ``AsyncSeq2.mergeChoice``() =
  let ls1 = [1;2;3;4;5]
  let ls2 = [6.;7.;8.;9.;10.]
  let actual = AsyncSeq2.mergeChoice (AsyncSeq2.ofSeq ls1) (AsyncSeq2.ofSeq ls2) |> AsyncSeq2.toListSynchronously |> Set.ofList
  let expected = (List.map Choice1Of2 ls1) @ (List.map Choice2Of2 ls2) |> Set.ofList
  Assert.True((expected = actual))


[<Fact>]
let ``AsyncSeq2.merge should be fair``() =
  let s1 = asyncSeq2 {
    do! Async2.Sleep 1
    yield 1
  }
  let s2 = asyncSeq2 {
    yield 2
  }
  let actual = AsyncSeq2.merge s1 s2
  let expected = [2;1] |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.merge should be fair 2``() =
  let s1 = asyncSeq2 {
    yield 1
  }
  let s2 = asyncSeq2 {
    do! Async2.Sleep 1
    yield 2
  }
  let actual = AsyncSeq2.merge s1 s2
  let expected = [1;2] |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.replicate``() =
  let c = 10
  let x = "hello"
  let actual = AsyncSeq2.replicate 100 x |> AsyncSeq2.take c
  let expected = List.replicate c x |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.replicateInfinite``() =
  let c = 10
  let x = "hello"
  let actual = AsyncSeq2.replicateInfinite x |> AsyncSeq2.take c
  let expected = List.replicate c x |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.init``() =
  for c in [0; 1; 100] do
      let actual = AsyncSeq2.init c string
      let expected = List.init c string |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.initInfinite``() =
  for c in [0; 1; 100] do
      let actual = AsyncSeq2.initInfinite string  |> AsyncSeq2.take c
      let expected = List.init c string |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.collect works``() =
  for c in [0; 1; 10] do
      let actual = AsyncSeq2.collect (fun i -> AsyncSeq2.ofSeq [ 0 .. i]) (AsyncSeq2.ofSeq [ 0 .. c ])
      let expected = [ for i in 0 .. c do yield! [ 0 .. i ] ] |> AsyncSeq2.ofSeq
      Assert.AreEqual(expected, actual)


[<Fact>]
let ``AsyncSeq2.initInfinite scales``() =
    AsyncSeq2.initInfinite string  |> AsyncSeq2.take 100 |> AsyncSeq2.iter ignore |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.initAsync``() =
  for c in [0; 1; 100] do
      let actual = AsyncSeq2.initAsync c (string >> (fun value -> async2 { return value }))
      let expected = List.init c string |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.initInfiniteAsync``() =
  for c in [0; 1; 100] do
      let actual = AsyncSeq2.initInfiniteAsync (string >> (fun value -> async2 { return value })) |> AsyncSeq2.take c
      let expected = List.init c string |> AsyncSeq2.ofSeq
      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.traverseOptionAsync``() =
  let seen = ResizeArray<_>()
  let s = [1;2;3;4;5] |> AsyncSeq2.ofSeq
  let f i =
    seen.Add i
    if i < 2 then Some i |> (fun value -> async2 { return value })
    else None |> (fun value -> async2 { return value })
  let r = AsyncSeq2.traverseOptionAsync f s |> Async2.RunSynchronously
  match r with
  | Some _ -> Assert.Fail()
  | None -> Assert.True(([1;2] = (seen |> List.ofSeq)))

[<Fact>]
let ``AsyncSeq2.traverseChoiceAsync``() =
  let seen = ResizeArray<_>()
  let s = [1;2;3;4;5] |> AsyncSeq2.ofSeq
  let f i =
    seen.Add i
    if i < 2 then Choice1Of2 i |> (fun value -> async2 { return value })
    else Choice2Of2 "oh no" |> (fun value -> async2 { return value })
  let r = AsyncSeq2.traverseChoiceAsync f s |> Async2.RunSynchronously
  match r with
  | Choice1Of2 _ -> Assert.Fail()
  | Choice2Of2 e ->
    Assert.AreEqual("oh no", e)
    Assert.True(([1;2] = (seen |> List.ofSeq)))

[<Fact>]
let ``AsyncSeq2.traverseOptionAsync returns Some sequence when all elements succeed``() =
  let s = [1;2;3] |> AsyncSeq2.ofSeq
  let f i = Some (i * 10) |> (fun value -> async2 { return value })
  let r = AsyncSeq2.traverseOptionAsync f s |> Async2.RunSynchronously
  match r with
  | None -> Assert.Fail("Expected Some")
  | Some result ->
    let values = result |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
    Assert.AreEqual([10;20;30], values)

[<Fact>]
let ``AsyncSeq2.traverseOptionAsync does not read past failing element``() =
  let readCount = ref 0
  let s = asyncSeq2 {
    for i in 1..10 do
      incr readCount
      yield i
  }
  let f i = (if i <= 3 then Some i else None) |> (fun value -> async2 { return value })
  let _r = AsyncSeq2.traverseOptionAsync f s |> Async2.RunSynchronously
  // f returns None on element 4; only elements 1..4 should be read from source
  Assert.AreEqual(4, readCount.Value)

[<Fact>]
let ``AsyncSeq2.traverseChoiceAsync returns Choice1Of2 sequence when all elements succeed``() =
  let s = [1;2;3] |> AsyncSeq2.ofSeq
  let f i = Choice1Of2 (i * 10) |> (fun value -> async2 { return value })
  let r = AsyncSeq2.traverseChoiceAsync f s |> Async2.RunSynchronously
  match r with
  | Choice2Of2 _ -> Assert.Fail("Expected Choice1Of2")
  | Choice1Of2 result ->
    let values = result |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
    Assert.AreEqual([10;20;30], values)

[<Fact>]
let ``AsyncSeq2.traverseChoiceAsync does not read past failing element``() =
  let readCount = ref 0
  let s = asyncSeq2 {
    for i in 1..10 do
      incr readCount
      yield i
  }
  let f i = (if i <= 3 then Choice1Of2 i else Choice2Of2 "stop") |> (fun value -> async2 { return value })
  let _r = AsyncSeq2.traverseChoiceAsync f s |> Async2.RunSynchronously
  // f returns Choice2Of2 on element 4; only elements 1..4 should be read from source
  Assert.AreEqual(4, readCount.Value)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq does not hung forever and rethrows exception``() =
  let s = asyncSeq2 {
      yield 1
      failwith "error"
  }
  Assert.Throws<Exception>(fun _ -> s |> AsyncSeq2.toBlockingSeq |> Seq.toList |> ignore) |> ignore
  try
      let _ = s |> AsyncSeq2.toBlockingSeq |> Seq.toList
      ()
  with e ->
      Assert.AreEqual(e.Message, "error")


[<Fact>]
let ``AsyncSeq2.distinctUntilChangedWithAsync``() =
  let ls = [1;1;2;2;3;4;5;1]
  let s = ls |> AsyncSeq2.ofSeq
  let c a b =
    if a = b then true |> (fun value -> async2 { return value })
    else false |> (fun value -> async2 { return value })
  let actual = s |> AsyncSeq2.distinctUntilChangedWithAsync c
  let expected = [1;2;3;4;5;1] |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.takeUntil should complete immediately with completed signal``() =
  let s = asyncSeq2 {
    do! Async2.Sleep 1
    yield 1
    yield 2
  }
  let actual = AsyncSeq2.takeUntilSignal AsyncOps.unit s
  Assert.True(EQ (AsyncSeq2.empty ()) actual)


[<Fact>]
let ``AsyncSeq2.takeUntil should take entire sequence with never signal``() =
  let expected = [1;2;3;4] |> AsyncSeq2.ofSeq
  let actual = expected |> AsyncSeq2.takeUntilSignal AsyncOps.never
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.singleton works``() =
  let expected = [1] |> AsyncSeq2.ofSeq
  let actual = AsyncSeq2.singleton 1
  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skipUntil should not skip with completed signal``() =
  let expected = [1;2;3;4] |> AsyncSeq2.ofSeq
  let actual =
      asyncSeq2 {
          do! Async2.Sleep 10
          yield! expected
      }
      |> AsyncSeq2.skipUntilSignal AsyncOps.unit

  Assert.True(EQ expected actual)


[<Fact>]
let ``AsyncSeq2.skipUntil should skip everything with never signal``() =
  let actual = [1;2;3;4] |> AsyncSeq2.ofSeq |> AsyncSeq2.skipUntilSignal AsyncOps.never
  Assert.True(EQ (AsyncSeq2.empty ()) actual)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work length 1``() =
  let s = asyncSeq2 { yield 1 } |> AsyncSeq2.toBlockingSeq  |> Seq.toList
  Assert.True((s = [1]))

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work length 0``() =
  let s = asyncSeq2 { () } |> AsyncSeq2.toBlockingSeq  |> Seq.toList
  Assert.True((s = []))

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work length 2 with sleep``() =
  let s =
    asyncSeq2 {
      yield 1
      do! Async2.Sleep 1
      yield 2
    }
    |> AsyncSeq2.toBlockingSeq
    |> Seq.toList
  Assert.True((s = [1; 2]))

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work length 1 with fail``() =
  let s =
      asyncSeq2 {
        yield 1
        failwith "fail"
      }
      |> AsyncSeq2.toBlockingSeq
      |> Seq.truncate 1
      |> Seq.toList
  Assert.True((s = [1]))

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work length 0 with fail``() =
  let s =
      asyncSeq2 { failwith "fail"  }
      |> AsyncSeq2.toBlockingSeq
      |> Seq.truncate 0
      |> Seq.toList
  Assert.True((s = []))

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should be cancellable``() =
  let cancelCount = ref 0
  let aseq =
      asyncSeq2 {
          use! a = Async2.OnCancel(fun x -> incr cancelCount)
          while true do
              yield 1
              do! Async2.Sleep 1
    }

  let asSeq = aseq |> AsyncSeq2.toBlockingSeq
  let enum = asSeq.GetEnumerator()
  Assert.AreEqual(cancelCount.Value, 0)
  let canMoveNext = enum.MoveNext()
  Assert.AreEqual(canMoveNext, true)
  Assert.AreEqual(cancelCount.Value, 0)
  enum.Dispose()
  System.Threading.Thread.Sleep(100) // wait for task cancellation to be effective
  Assert.AreEqual(cancelCount.Value, 1)

[<Fact>]
let ``AsyncSeq2.while should allow do at end``() =
  let s1 = asyncSeq2 {
    while false do
        yield 1
        do! Async2.Sleep 1
  }
  Assert.True(true)

let observe vs err =
    let discarded = ref false
    { new IObservable<'U> with
            member x.Subscribe(observer) =
                for v in vs do
                   observer.OnNext v
                if err then
                   observer.OnError (Failure "fail")
                observer.OnCompleted()
                { new IDisposable with member __.Dispose() = discarded := true }  },
    (fun _ -> discarded.Value)

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (empty)``() =
  let src, discarded = observe [] false
  Assert.True(src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.toListSynchronously = [])
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (singleton)``() =
  let src, discarded = observe [1] false
  Assert.True(src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.toListSynchronously = [1])
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (ten)``() =
  let src, discarded = observe [1..10] false
  Assert.True(src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.toListSynchronously = [1..10])
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (empty, fail)``() =
  let src, discarded = observe [] true
  Assert.True(try (src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.toListSynchronously |> ignore); false with _ -> true)
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (one, fail)``() =
  let src, discarded = observe [1] true
  Assert.True(try (src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.toListSynchronously |> ignore); false with _ -> true)
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.ofObservableBuffered should work (one, take)``() =
  let src, discarded = observe [1] true
  Assert.True(src |> AsyncSeq2.ofObservableBuffered |> AsyncSeq2.take 1 |> AsyncSeq2.toListSynchronously = [1])
  Assert.True(discarded())

[<Fact>]
let ``AsyncSeq2.getIterator should work``() =
  let s1 = [1..2] |> AsyncSeq2.ofSeq
  let i = s1.GetAsyncEnumerator(System.Threading.CancellationToken.None)
  try
    let move () = i.MoveNextAsync().AsTask() |> Async2.AwaitTask |> Async2.RunSynchronously
    Assert.True(move(), "expected first element")
    Assert.AreEqual(i.Current, 1)
    Assert.True(move(), "expected second element")
    Assert.AreEqual(i.Current, 2)
    Assert.False(move(), "expected end of sequence")
  finally
    i.DisposeAsync() |> ignore



[<Fact>]
let ``asyncSeq.For should delay``() =
  let (s:seq<int>) =
     { new System.Collections.Generic.IEnumerable<int> with
           member x.GetEnumerator() = failwith "fail"
       interface System.Collections.IEnumerable with
           member x.GetEnumerator() = failwith "fail"  }
  Assert.DoesNotThrow(fun _ -> asyncSeq2.For(s, (fun _ -> Seq.empty)) |> ignore)

[<Fact>]
let ``Async2.mergeAll should work``() =
    for n in 0 .. 10 do
        let expected =
            [ for i in 1 .. n do
                    for j in 0 .. i do
                      yield i
                 ]
            |> List.sort
        let actual =
            [ for i in 1 .. n ->
                asyncSeq2 {
                    for j in 0 .. i do
                      do! Async2.Sleep 1;
                      yield i
                } ]
            |> AsyncSeq2.mergeAll
            |> AsyncSeq2.toListSynchronously
            |> List.sort
        Assert.True((actual = expected), sprintf "mergeAll test at n = %d" n)


[<Fact>]
let ``Async2.mergeAll should perform well``() =
    let mergeTest n =
        [ for i in 1 .. n ->
            asyncSeq2 {
              do! Async2.Sleep 500
              yield i
            } ]
        |> AsyncSeq2.mergeAll
        |> AsyncSeq2.toListSynchronously

    Assert.DoesNotThrow(fun _ -> mergeTest 500 |> ignore)

[<Fact>]
let ``Async2.mergeAll should be fair``() =
  let s1 = asyncSeq2 {
    do! Async2.Sleep 300
    yield 1
  }
  let s2 = asyncSeq2 {
    do! Async2.Sleep 100
    yield 2
  }
  let s3 = asyncSeq2 {
    yield 3
  }
  let actual = AsyncSeq2.mergeAll [s1; s2; s3]
  let expected = [3;2;1] |> AsyncSeq2.ofSeq
  Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.mergeAll propagates a task failure``() =
    for n in 0 .. 10 do
      Assert.Throws<Exception>(fun _ ->
          [ for i in 0 .. n ->
                    asyncSeq2 {
                      yield 1
                      if (i % 4) = 0 then failwith "fail"
                      yield 2
                    } ]
          |> AsyncSeq2.mergeAll
          |> AsyncSeq2.toListSynchronously
          |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.merge propagates a task failure``() =
      for i in 0 .. 1 do
        Assert.Throws<Exception>(fun _ ->
          (asyncSeq2 {
             yield 1
             if i % 2 = 0 then failwith "fail"
             yield 2
           },
           asyncSeq2 {
             yield 1
             if i % 2 = 1 then failwith "fail"
             yield 2
           })
          ||> AsyncSeq2.merge
          |> AsyncSeq2.toListSynchronously
          |> ignore) |> ignore


[<Fact>]
let ``AsyncSeq2.mergeChoice propagates a task failure``() =
      for i in 0 .. 1 do
        Assert.Throws<Exception>(fun _ ->
          (asyncSeq2 {
             yield 1
             if i % 2 = 0 then failwith "fail"
             yield 2
           },
           asyncSeq2 {
             yield 1
             if i % 2 = 1 then failwith "fail"
             yield 2
           })
          ||> AsyncSeq2.mergeChoice
          |> AsyncSeq2.toListSynchronously
          |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.interleave should fail with Exception if a task fails``() =
      for i in 0 .. 1 do
        Assert.Throws<Exception>(fun _ ->
          (asyncSeq2 {
             yield 1
             if i % 2 = 0 then failwith "fail"
             yield 2
           },
           asyncSeq2 {
             yield 1
             if i % 2 = 1 then failwith "fail"
             yield 2
           })
          ||> AsyncSeq2.interleave
          |> AsyncSeq2.toListSynchronously
          |> ignore) |> ignore


let perfTest1 n =
    let empty = async2 { return () }
    Seq.init n id
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.iterAsync (fun _ -> empty )
    |> Async2.RunSynchronously

// n                            NEW        1.15.0
//perfTest1 1000 -             0.001        0.004
//perfTest1 2000 -
//perfTest1 3000 -
//perfTest1 4000 -
//perfTest1 5000 -
//perfTest1 6000 -             0.006        0.020
//perfTest1 10000 -            0.012
//perfTest1 100000 -           0.129        0.260
//perfTest1 1000000 -          0.708        2.345


let perfTest2 n =
    Seq.init n id
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.toArraySynchronously

// n                        NEW
//perfTest2 1000            0.038
//perfTest2 2000            0.001
//perfTest2 3000            0.004
//perfTest2 4000
//perfTest2 5000
//perfTest2 10000           0.007
//perfTest2 100000          0.076
//perfTest2 1000000         0.663


// This was the original ofSeq implementation.  It is now faster than before this perf testing
// took place, but is still slower than the bespoke ofSeq implementation (which effectively "knows"
// that a single yield happens for each iteration of the loop).
let perfTest3 n =
    let ofSeq2 (source : seq<'T>) = asyncSeq2 {
        for el in source do
          yield el }

    Seq.init n id
    |> ofSeq2
    |> AsyncSeq2.toArraySynchronously


// n                        NEW         1.15.0
//perfTest3 1000            0.003
//perfTest3 2000
//perfTest3 3000
//perfTest3 4000
//perfTest3 5000           0.009
//perfTest3 10000           0.015
//perfTest3 100000          0.155
//perfTest3 1000000         1.500      3.480

let perfTest4 n =
    Seq.init n id
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.map id
    |> AsyncSeq2.filter (fun x -> x % 2 = 0)
    |> AsyncSeq2.toArraySynchronously

// n                        NEW         1.15.0
//perfTest4 1000
//perfTest4 2000
//perfTest4 3000
//perfTest4 4000
//perfTest4 5000
//perfTest4 10000
//perfTest4 100000          0.362       0.442
//perfTest4 1000000         3.533       4.656


[<Fact>]
let ``AsyncSeq2.unfoldAsync should be iterable in finite resources``() =
    let generator state =
        async2 {
            if state < 10000 then
                return Some ((), state + 1)
            else
                return None
        }

    AsyncSeq2.unfoldAsync generator 0
    |> AsyncSeq2.iter ignore
    |> Async2.RunSynchronously



[<Fact>]
let ``AsyncSeq2.mapi should work`` () =
  for i in 0..100 do
    let ls = List.init i (fun x -> x + 100)
    let expected =
      ls
      |> List.mapi (fun i x -> sprintf "%i_%i" i x)
    let actual =
      ls
      |> AsyncSeq2.ofSeq
      |> AsyncSeq2.mapi (fun i x -> sprintf "%i_%i" i x)
      |> AsyncSeq2.toListSynchronously
    Assert.AreEqual(expected, actual)


[<Fact>]
let ``AsyncSeq2.take should work``() =
  let s = asyncSeq2 {
    yield ["a",1] |> Map.ofList
  }
  let ss = s |> AsyncSeq2.take 1
  let ls = ss |> AsyncSeq2.toListSynchronously
  ()

[<Fact>]
let ``AsyncSeq2.truncate should work like take``() =
  let s = asyncSeq2 {
    yield ["a",1] |> Map.ofList
  }
  let expected = s |> AsyncSeq2.take 1
  let actual =  s |> AsyncSeq2.truncate 1
  Assert.AreEqual(expected, actual)


[<Fact>]
let ``AsyncSeq2.mapAsyncParallel should maintain order`` () =
  for i in 0..100 do
    let ls = List.init i id
    let expected =
      ls
      |> AsyncSeq2.ofSeq
      |> AsyncSeq2.mapAsync ((fun value -> async2 { return value }))
    let actual =
      ls
      |> AsyncSeq2.ofSeq
      |> AsyncSeq2.mapAsyncParallel ((fun value -> async2 { return value }))
    Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.mapAsyncParallel should propagate exception`` () =

  for N in [100] do

    let fail = N / 2

    let res =
      Seq.init N id
      |> AsyncSeq2.ofSeq
      |> Microsoft.FSharp.Control.AsyncSeq2.mapAsyncParallel (fun i -> async2 {
        if i = fail then
          return failwith  "error"
        return i })
      |> Microsoft.FSharp.Control.AsyncSeq2.mapAsyncParallel (ignore >> (fun value -> async2 { return value }))
      |> AsyncSeq2.iter ignore
      |> Async2.Catch
      |> runTest

    match res with
    | Choice2Of2 _ -> ()
    | Choice1Of2 _ -> Assert.Fail ("error expected")


//[<Fact>]
let ``AsyncSeq2.mapParallelAsync should be parallel`` () =
  let parallelism = 3
  let barrier = new Threading.Barrier(parallelism)
  let s = AsyncSeq2.init parallelism id
  let expected =
    s |> AsyncSeq2.map id
  let actual =
    s
    |> AsyncSeq2.mapAsyncParallel (fun i -> async2 { barrier.SignalAndWait () ; return i }) // can deadlock
  Assert.AreEqual(expected, actual, timeout=200)


[<Fact>]
let ``AsyncSeq2.iterAsyncParallel should propagate exception`` () =

  for N in [100] do

    let fail = N / 2

    let res =
      Seq.init N id
      |> AsyncSeq2.ofSeq
      |> Microsoft.FSharp.Control.AsyncSeq2.mapAsyncParallel (fun i -> async2 {
        if i = fail then
          return failwith  "error"
        return i })
//      |> AsyncSeq2.iterAsyncParallel (fun i -> async2 {
//        if i = fail then
//          return failwith "error"
//        else () })
      //|> AsyncSeq2.iter ignore
      |> AsyncSeq2.iterAsyncParallel ((fun value -> async2 { return value }) >> Async2.Ignore)
      |> Async2.Catch
      |> runTest

    match res with
    | Choice2Of2 _ -> ()
    | Choice1Of2 _ -> Assert.Fail ("error expected")

[<Fact>]
let ``AsyncSeq2.iterAsyncParallel should cancel and not block forever when run in parallel with another exception-throwing Async`` () =

    let handle x = async2 {
           do! Async2.Sleep 5
    }

    let fakeAsync = async2 {
           do! Async2.Sleep 50
           return "fakeAsync"
    }

    let makeAsyncSeqBatch () =
           let rec loop() = asyncSeq2 {
               let! batch =  fakeAsync |> Async2.Catch
               match batch with
               | Choice1Of2 batch ->
                 if (Seq.isEmpty batch) then
                   do! Async2.Sleep 50
                   yield! loop()
                 else
                   yield batch
                   yield! loop()
               | Choice2Of2 err ->
                    printfn "Problem getting batch: %A" err
           }

           loop()

    let x = makeAsyncSeqBatch () |> AsyncSeq2.concatSeq |> AsyncSeq2.iterAsyncParallel handle
    let exAsync = async2 {
           do! Async2.Sleep 2000
           failwith "error"
    }

    let t = [x; exAsync] |> Async2.Parallel |> Async2.Ignore |> Async2.StartAsTask

    // should fail after 2 seconds
    Assert.Throws<AggregateException>(fun _ -> t.Wait(4000) |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.iterAsyncParallelThrottled should propagate handler exception`` () =

  let res =
    AsyncSeq2.init 100 id
    |> AsyncSeq2.iterAsyncParallelThrottled 10 (fun i -> async2 { if i = 50 then return failwith "oh no" else return () })
    |> Async2.Catch
    |> (fun x -> Async2.RunSynchronously (x, timeout = 10000))

  match res with
  | Choice2Of2 _ -> ()
  | Choice1Of2 _ -> Assert.Fail ("error expected")

[<Fact>]
let ``AsyncSeq2.iterAsyncParallelThrottled should propagate sequence exception`` () =

  let res =
    asyncSeq2 {
      yield 1
      yield 2
      yield 3
      failwith "oh no"
    }
    |> AsyncSeq2.iterAsyncParallelThrottled 10 ((fun value -> async2 { return value }) >> Async2.Ignore)
    |> Async2.Catch
    |> (fun x -> Async2.RunSynchronously (x, timeout = 10000))

  match res with
  | Choice2Of2 _ -> ()
  | Choice1Of2 _ -> Assert.Fail ("error expected")


[<Fact>]
let ``AsyncSeq2.iterAsyncParallelThrottled should throttle`` () =

  let count = ref 0
  let parallelism = 10

  let res =
    AsyncSeq2.init 100 id
    |> AsyncSeq2.iterAsyncParallelThrottled parallelism (fun i -> async2 {
      let c = Interlocked.Increment count
      if c > parallelism then
        return failwith "oh no"
      do! Async2.Sleep 1
      Interlocked.Decrement count |> ignore
      return () })
    |> Async2.RunSynchronously
  ()

[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallel should produce all results`` () =
  let input = [1; 2; 3; 4; 5]
  let expected = [2; 4; 6; 8; 10] |> Set.ofList

  let actual =
    input
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.mapAsyncUnorderedParallel (fun x -> async2 {
      do! Async2.Sleep(10)
      return x * 2
    })
    |> AsyncSeq2.toListAsync
    |> runTest
    |> Set.ofList

  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallel should propagate exceptions`` () =
  let input = [1; 2; 3; 4; 5]

  let res =
    input
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.mapAsyncUnorderedParallel (fun x -> async2 {
      if x = 3 then failwith "test exception"
      return x * 2
    })
    |> AsyncSeq2.toListAsync
    |> Async2.Catch
    |> runTest

  match res with
  | Choice2Of2 _ -> () // Expected exception
  | Choice1Of2 _ -> Assert.Fail("Expected exception but none was thrown")

[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallel should not preserve order`` () =
  // Test that results can come in different order than input
  let input = [1; 2; 3; 4; 5]
  let results = System.Collections.Generic.List<int>()

  input
  |> AsyncSeq2.ofSeq
  |> AsyncSeq2.mapAsyncUnorderedParallel (fun x -> async2 {
    // Longer delay for smaller numbers to encourage reordering
    do! Async2.Sleep(60 - x * 10)
    results.Add(x)
    return x
  })
  |> AsyncSeq2.iter ignore
  |> runTest

  let resultOrder = results |> List.ofSeq
  // With unordered parallel processing and varying delays,
  // we expect some reordering (though not guaranteed in all environments)
  let isReordered = resultOrder <> [1; 2; 3; 4; 5]

  // This test passes regardless of ordering since reordering depends on timing
  // The main validation is that all results are present
  let allPresent = (Set.ofList resultOrder) = (Set.ofList input)
  Assert.IsTrue(allPresent, "All input elements should be present in results")


[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallelThrottled should produce all results`` () =
  let input = [1; 2; 3; 4; 5]
  let expected = [2; 4; 6; 8; 10] |> Set.ofList

  let actual =
    input
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.mapAsyncUnorderedParallelThrottled 3 (fun x -> async2 {
      do! Async2.Sleep(10)
      return x * 2
    })
    |> AsyncSeq2.toListAsync
    |> runTest
    |> Set.ofList

  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallelThrottled should propagate handler exception`` () =
  let res =
    AsyncSeq2.init 100 id
    |> AsyncSeq2.mapAsyncUnorderedParallelThrottled 10 (fun i -> async2 {
      if i = 50 then return failwith "oh no"
      else return i * 2
    })
    |> AsyncSeq2.toListAsync
    |> Async2.Catch
    |> (fun x -> Async2.RunSynchronously (x, timeout = 10000))

  match res with
  | Choice2Of2 _ -> ()
  | Choice1Of2 _ -> Assert.Fail ("error expected")

[<Fact>]
let ``AsyncSeq2.mapAsyncUnorderedParallelThrottled should throttle`` () =
  let count = ref 0
  let parallelism = 5

  let result =
    AsyncSeq2.init 50 id
    |> AsyncSeq2.mapAsyncUnorderedParallelThrottled parallelism (fun i -> async2 {
      let c = Interlocked.Increment count
      if c > parallelism then
        return failwith (sprintf "concurrency exceeded: %d > %d" c parallelism)
      do! Async2.Sleep 5
      Interlocked.Decrement count |> ignore
      return i * 2 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously

  Assert.AreEqual(50, result.Length)

[<Fact>]
let ``AsyncSeq2.mapAsyncParallelThrottled should maintain order`` () =
  let ls = List.init 100 id
  let result =
    ls
    |> AsyncSeq2.ofList
    |> AsyncSeq2.mapAsyncParallelThrottled 5 (fun i -> async2 {
      do! Async2.Sleep (100 - i)
      return i * 2 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual(ls |> List.map ((*) 2), result)

[<Fact>]
let ``AsyncSeq2.mapAsyncParallelThrottled should propagate exception`` () =
  let result =
    AsyncSeq2.init 50 id
    |> AsyncSeq2.mapAsyncParallelThrottled 5 (fun i -> async2 {
      if i = 25 then return failwith "test error"
      return i })
    |> AsyncSeq2.toListAsync
    |> Async2.Catch
    |> Async2.RunSynchronously
  match result with
  | Choice2Of2 _ -> ()
  | Choice1Of2 _ -> Assert.Fail("Expected exception")

[<Fact>]
let ``AsyncSeq2.mapAsyncParallelThrottled should throttle`` () =
  let count = ref 0
  let parallelism = 5

  let result =
    AsyncSeq2.init 50 id
    |> AsyncSeq2.mapAsyncParallelThrottled parallelism (fun i -> async2 {
      let c = Interlocked.Increment count
      if c > parallelism then
        return failwith (sprintf "concurrency exceeded: %d > %d" c parallelism)
      do! Async2.Sleep 5
      Interlocked.Decrement count |> ignore
      return i * 2 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously

  Assert.AreEqual(50, result.Length)
  Assert.AreEqual([ 0..49 ] |> List.map ((*) 2), result)

//[<Fact>]
//let ``AsyncSeq2.mapParallelAsyncBounded should maintain order`` () =
//  let ls = List.init 500 id
//  let expected =
//    ls
//    |> AsyncSeq2.ofSeq
//    |> AsyncSeq2.mapAsync ((fun value -> async2 { return value }))
//  let actual =
//    ls
//    |> AsyncSeq2.ofSeq
//    |> AsyncSeq2.mapAsyncParallelBounded 10 ((fun value -> async2 { return value }))
//  Assert.AreEqual(expected, actual, timeout=200)



[<Fact>]
let ``AsyncSeq2Src.should work`` () =
  for n in 0..100 do
    let items = List.init n id
    let src = AsyncSeq2Src.create ()
    let actual = src |> AsyncSeq2Src.toAsyncSeq
    for item in items do
      src |> AsyncSeq2Src.put item
    src |> AsyncSeq2Src.close
    let expected = items |> AsyncSeq2.ofSeq
    Assert.AreEqual (expected, actual)

[<Fact>]
let ``AsyncSeq2Src.put should yield when tapped after put`` () =
  let item = 1
  let src = AsyncSeq2Src.create ()
  src |> AsyncSeq2Src.put item
  let actual = src |> AsyncSeq2Src.toAsyncSeq
  src |> AsyncSeq2Src.close
  let expected = (AsyncSeq2.empty ())
  Assert.AreEqual (expected, actual)

[<Fact>]
let ``AsyncSeq2Src.fail should throw`` () =
  let item = 1
  let src = AsyncSeq2Src.create ()
  let actual = src |> AsyncSeq2Src.toAsyncSeq
  src |> AsyncSeq2Src.error (exn("test"))
  let expected = asyncSeq2 { raise (exn("test")) }
  Assert.AreEqual (expected, actual)


[<Fact>]
let ``AsyncSeq2.groupBy should work``() =
  for i in 0..100 do
    for j in 1..3 do
      let ls = List.init i id
      let p x = x % j
      let expected =
        ls
        |> Seq.groupBy p
        |> Seq.map (fun (key, group) -> key, Seq.toArray group)
        |> Seq.toArray
      let actual =
        ls
        |> AsyncSeq2.ofSeq
        |> AsyncSeq2.groupBy p
        |> Async2.RunSynchronously
      Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.groupBy should propagate exception and terminate all groups``() =
  Assert.Throws<Exception>(fun () ->
    asyncSeq2 { yield raise (exn("test")) }
    |> AsyncSeq2.groupBy (fun i -> i % 3)
    |> Async2.RunSynchronously
    |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.combineLatest should behave like merge after initial``() =
  for n in 0..10 do
    for m in 0..10 do
      let ls1 = List.init n id
      let ls2 = List.init m id
      let actual = AsyncSeq2.combineLatestWith (+) (AsyncSeq2.ofSeq ls1 |> randomDelayMax 2) (AsyncSeq2.ofSeq ls2 |> randomDelayMax 2)
      let values = actual |> AsyncSeq2.toListSynchronously
      if n = 0 || m = 0 then
        Assert.AreEqual([], values)
      else
        Assert.True(values.Length >= 1 && values.Length <= n + m - 1)
        Assert.AreEqual(n + m - 2, List.last values)
        Assert.True(values |> List.pairwise |> List.forall (fun (a, b) -> a <= b))

[<Fact>]
let ``AsyncSeq2.combineLatest should be never when either argument is never``() =
  let expected = AsyncSeq2Helpers.never
  let actual1 = AsyncSeq2.combineLatestWith (fun _ _ -> 0) (AsyncSeq2Helpers.never) (AsyncSeq2.singleton 1)
  let actual2 = AsyncSeq2.combineLatestWith (fun _ _ -> 0) (AsyncSeq2.singleton 1) (AsyncSeq2Helpers.never)
  Assert.AreEqual(expected, actual1, timeout=100, exnEq=AreCancellationExns)
  Assert.AreEqual(expected, actual2, timeout=100, exnEq=AreCancellationExns)

[<Fact>]
let ``Async2.ofSeqAsync should work``() =
  let asyncSequential (s:seq<Async2<'t>>) : Async2<seq<'t>> =
    async2 {
      let results = ResizeArray<'t>()
      for item in s do
        let! value = item
        results.Add value
      return results :> seq<'t>
    }

  for n in 0..10 do
    let s = Seq.init n (id >> (fun value -> async2 { return value }))
    let actual = AsyncSeq2.ofSeqAsync s
    let expected = asyncSequential s |> Async2.RunSynchronously |> AsyncSeq2.ofSeq
    Assert.True(EQ expected actual)

[<Fact>]
let ``Async2.concat should work``() =
  for n in 0..10 do
    for m in 0..10 do
      let actual =
        Seq.init m (fun _ -> Seq.init n (id >> (fun value -> async2 { return value })) |> AsyncSeq2.ofSeqAsync)
        |> AsyncSeq2.ofSeq
        |> AsyncSeq2.concat

      let expected =
        Seq.init m (fun _ -> Seq.init n id)
        |> AsyncSeq2.ofSeq
        |> AsyncSeq2.concatSeq

      Assert.True(EQ expected actual)

[<Fact>]
let ``AsyncSeq2.sort should work for``() =
  let input = [1; 3; 2; 5; 7; 4; 6] |> AsyncSeq2.ofSeq
  let expected = [|1..7|]
  let actual = input |> AsyncSeq2.sort |> Async2.RunSynchronously
  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.sortDescending should work``() =
  let input = [1; 3; 2; Int32.MaxValue; 4; 6; Int32.MinValue; 5; 7; 0] |> AsyncSeq2.ofSeq
  let expected = seq { yield Int32.MaxValue; yield! seq{ 7..-1..0 }; yield Int32.MinValue } |> Array.ofSeq
  let actual = input |> AsyncSeq2.sortDescending |> Async2.RunSynchronously
  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.sortBy should work``() =
  let fn x = Math.Abs(x-5)
  let input = [1; 2; 4; 5; 7] |> AsyncSeq2.ofSeq
  let expected = [|5; 4; 7; 2; 1|]
  let actual = input |> AsyncSeq2.sortBy fn |> Async2.RunSynchronously
  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.sortByDescending should work``() =
  let fn x = Math.Abs(x-5)
  let input = [1; 2; 4; 5; 6; 7;] |> AsyncSeq2.ofSeq
  let expected = [|1; 2; 7; 4; 6; 5;|]
  let actual = input |> AsyncSeq2.sortByDescending fn |> Async2.RunSynchronously
  Assert.AreEqual(expected, actual)

[<Fact>]
let ``async.For with AsyncSeq should work``() =
  async2 {
    let mutable results = []
    let source = asyncSeq2 {
      yield 1
      yield 2
      yield 3
    }

    do! async2 {
      for item in source do
        results <- item :: results
    }

    Assert.AreEqual([3; 2; 1], results)
  }
  |> Async2.RunSynchronously

[<Fact>]
let ``async.For with empty AsyncSeq should work``() =
  async2 {
    let mutable count = 0
    let source = (AsyncSeq2.empty ())

    do! async2 {
      for item in source do
        count <- count + 1
    }

    Assert.AreEqual(0, count)
  }
  |> Async2.RunSynchronously

[<Fact>]
let ``async.For with exception in AsyncSeq should propagate``() =
  async2 {
    let source = asyncSeq2 {
      yield 1
      failwith "test exception"
      yield 2
    }

    try
      do! async2 {
        for item in source do
          ()
      }
      Assert.Fail("Expected exception to be thrown")
    with
    | ex when ex.Message = "test exception" ->
      () // Expected
    | ex ->
      Assert.Fail($"Unexpected exception: {ex.Message}")
  }
  |> Async2.RunSynchronously

// ----------------------------------------------------------------------------
// Tests for previously uncovered modules to improve coverage

[<Fact>]
let ``AsyncSeqExtensions - async.For with AsyncSeq`` () =
  let mutable result = []
  let computation = async2 {
    for item in asyncSeq2 { yield 1; yield 2; yield 3 } do
      result <- item :: result
  }
  computation |> Async2.RunSynchronously
  Assert.AreEqual([3; 2; 1], result)

[<Fact>]
let ``AsyncSeqExtensions - async.For with empty AsyncSeq`` () =
  let mutable result = []
  let computation = async2 {
    for item in (AsyncSeq2.empty ()) do
      result <- item :: result
  }
  computation |> Async2.RunSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeqExtensions - async.For with exception in AsyncSeq`` () =
  let mutable exceptionCaught = false
  let computation = async2 {
    try
      for item in asyncSeq2 { yield 1; failwith "test error"; yield 2 } do
        ()
    with
    | ex when ex.Message = "test error" ->
        exceptionCaught <- true
  }
  computation |> Async2.RunSynchronously
  Assert.IsTrue(exceptionCaught)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work`` () =
  let asyncSeqData = asyncSeq2 {
    yield 1
    yield 2
    yield 3
  }
  let seqResult = AsyncSeq2.toBlockingSeq asyncSeqData |> Seq.toList
  Assert.AreEqual([1; 2; 3], seqResult)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq with empty AsyncSeq`` () =
  let seqResult = AsyncSeq2.toBlockingSeq (AsyncSeq2.empty ()) |> Seq.toList
  Assert.AreEqual([], seqResult)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq with exception`` () =
  let asyncSeqWithError = asyncSeq2 {
    yield 1
    failwith "test error"
    yield 2
  }
  Assert.Throws<System.Exception>(fun () ->
    AsyncSeq2.toBlockingSeq asyncSeqWithError |> Seq.toList |> ignore
  ) |> ignore

[<Fact>]
let ``AsyncSeq2.intervalMs should generate sequence with timestamps``() =
  let result =
    AsyncSeq2.intervalMs 50
    |> AsyncSeq2.take 3
    |> AsyncSeq2.toListAsync
    |> AsyncOps.timeoutMs 1000
    |> Async2.RunSynchronously

  Assert.AreEqual(3, result.Length)
  // Verify timestamps are increasing
  Assert.IsTrue(result.[1] > result.[0])
  Assert.IsTrue(result.[2] > result.[1])

[<Fact>]
let ``AsyncSeq2.intervalMs with zero period should work``() =
  let result =
    AsyncSeq2.intervalMs 0
    |> AsyncSeq2.take 2
    |> AsyncSeq2.toListAsync
    |> AsyncOps.timeoutMs 500
    |> Async2.RunSynchronously

  Assert.AreEqual(2, result.Length)

[<Fact>]
let ``AsyncSeq2.take with negative count should throw ArgumentException``() =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.take -1
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
    |> ignore
  ) |> ignore

[<Fact>]
let ``AsyncSeq2.skip with negative count should throw ArgumentException``() =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.skip -1
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
    |> ignore
  ) |> ignore

[<Fact>]
let ``AsyncSeq2.take zero should return empty sequence``() =
  let expected = []
  let actual =
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.take 0
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously

  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.skip zero should return original sequence``() =
  let expected = [1;2;3]
  let actual =
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.skip 0
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously

  Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.replicateInfinite with exception should propagate exception``() =
  let exceptionMsg = "test exception"
  let expected = System.ArgumentException(exceptionMsg)

  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.replicateInfinite (raise expected)
    |> AsyncSeq2.take 2
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
    |> ignore
  ) |> ignore

[<Fact>]
let ``AsyncSeq2.ofAsyncEnum should roundtrip successfully``() =
  let data = [ 1 .. 10 ] |> AsyncSeq2.ofSeq
  let actual = data |> AsyncSeq2.toAsyncEnum |> AsyncSeq2.ofAsyncEnum
  Assert.True(EQ data actual)

[<Fact>]
let ``AsyncSeq2.toAsyncEnum raises exception``() : unit =
  async2 {
    let exceptionMessage = "Raised inside AsyncSeq"
    let exceptionSequence =
      asyncSeq2 { yield failwith exceptionMessage; yield 1 }
      |> AsyncSeq2.toAsyncEnum
    let mutable exceptionRaised = false
    try
      use enumerator = exceptionSequence.GetAsyncEnumerator(CancellationToken.None)
      let! _ = enumerator.MoveNextAsync()
      enumerator.Current |> ignore
    with
    | ex ->
      if exceptionMessage <> ex.Message
      then Assert.Fail("Message")
      else exceptionRaised <- true
    Assert.IsTrue(exceptionRaised)
  }
  |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.ofAsyncEnum raises exception``() : unit =
  async2 {
    let exceptionMessage = "Raised inside AsyncSeq"
    let exceptionSequence =
      asyncSeq2 { yield failwith exceptionMessage; yield 1 }
      |> AsyncSeq2.toAsyncEnum
      |> AsyncSeq2.ofAsyncEnum
    let mutable exceptionRaised = false
    try
      use enumerator = exceptionSequence.GetAsyncEnumerator(CancellationToken.None)
      let! _ = enumerator.MoveNextAsync()
      Assert.Fail()
    with
    | ex ->
      if exceptionMessage <> ex.Message
      then Assert.Fail()
      else exceptionRaised <- true
    Assert.IsTrue(exceptionRaised)
  }
  |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.ofAsyncEnum can be cancelled``() : unit =
  use cts = new CancellationTokenSource()
  let mutable cancelledInvoked = false
  let mutable results = ResizeArray<_>()

  let sourceWorkflow =
    asyncSeq2 {
      use! __ = Async2.OnCancel(fun x -> cancelledInvoked <- true)
      yield 1
      yield 2
    } |> AsyncSeq2.toAsyncEnum |> AsyncSeq2.ofAsyncEnum

  let innerAsync =
    async2 {
      use enumerator = sourceWorkflow.GetAsyncEnumerator(cts.Token)
      let! hasFirst = enumerator.MoveNextAsync()
      if hasFirst then results.Add(enumerator.Current)
      cts.Cancel()
      try
        let! _ = enumerator.MoveNextAsync()
        Assert.Fail("Task should have been cancelled")
      with
      | :? OperationCanceledException -> ()
      | _ -> return Assert.Fail()
    }

  try
    Async2.RunSynchronously(innerAsync, cancellationToken = cts.Token)
    Assert.Fail()
  with
  | :? OperationCanceledException ->
    Assert.IsTrue(cancelledInvoked)
    Assert.IsTrue([ 1 ] = (results |> Seq.toList))
  | _ -> Assert.Fail()

[<Fact>]
let ``AsyncSeq2.toAsyncEnum can be cancelled``() : unit =
  async2 {
    use cts = new CancellationTokenSource()
    let mutable cancelledInvoked = false

    let sourceWorkflow =
      asyncSeq2 {
        use! __ = Async2.OnCancel(fun x -> cancelledInvoked <- true)
        yield 1
        yield 2
      }
      |> AsyncSeq2.toAsyncEnum

    let enumerator = sourceWorkflow.GetAsyncEnumerator(cts.Token)
    let! result1 = enumerator.MoveNextAsync().AsTask() |> Async2.AwaitTask
    Assert.IsTrue(result1)
    Assert.IsTrue(enumerator.Current = 1)
    cts.Cancel()
    try
      let! _ = enumerator.MoveNextAsync().AsTask() |> Async2.AwaitTask
      Assert.Fail()
    with
    | :? OperationCanceledException -> return ()
    | _ -> Assert.Fail()
    Assert.IsTrue(cancelledInvoked)
  }
  |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.toBlockingSeq should work with three values``() =
  let source = asyncSeq2 {
    yield 1
    yield 2
    yield 3
  }

  let result = AsyncSeq2.toBlockingSeq source |> Seq.toList
  Assert.AreEqual([1; 2; 3], result)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq with empty AsyncSeq should work``() =
  let source = (AsyncSeq2.empty ())
  let result = AsyncSeq2.toBlockingSeq source |> Seq.toList
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.toBlockingSeq with exception should propagate``() =
  let source = asyncSeq2 {
    yield 1
    failwith "test exception"
    yield 2
  }

  try
    let _ = AsyncSeq2.toBlockingSeq source |> Seq.toList
    Assert.Fail("Expected exception to be thrown")
  with
  | ex when ex.Message = "test exception" ->
    () // Expected
  | ex ->
    Assert.Fail($"Unexpected exception: {ex.Message}")

[<Fact>]
let ``AsyncSeq2.fold with empty sequence should return seed``() =
  let result = (AsyncSeq2.empty ())
               |> AsyncSeq2.fold (+) 10
               |> Async2.RunSynchronously
  Assert.AreEqual(10, result)

[<Fact>]
let ``AsyncSeq2.reduce sums non-empty sequence`` () =
  let result = asyncSeq2 { yield 1; yield 2; yield 3; yield 4; yield 5 }
               |> AsyncSeq2.reduce (+)
               |> Async2.RunSynchronously
  Assert.AreEqual(15, result)

[<Fact>]
let ``AsyncSeq2.reduce single element returns that element`` () =
  let result = asyncSeq2 { yield 42 }
               |> AsyncSeq2.reduce (+)
               |> Async2.RunSynchronously
  Assert.AreEqual(42, result)

[<Fact>]
let ``AsyncSeq2.reduce empty sequence raises ArgumentException`` () =
  Assert.Throws<ArgumentException>(fun () ->
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.reduce (+)
    |> Async2.RunSynchronously
    |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.reduceAsync accumulates with async function`` () =
  async2 {
    let result =
      asyncSeq2 { yield 10; yield 3; yield 2 }
      |> AsyncSeq2.reduceAsync (fun a b -> async2 { return a - b })
      |> Async2.RunSynchronously
    Assert.AreEqual(5, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.reduce matches Seq.reduce`` () =
  for ls in [ [1]; [1;2]; [3;1;4;1;5;9;2;6] ] do
    let expected = Seq.reduce (+) ls
    let actual = AsyncSeq2.ofSeq ls |> AsyncSeq2.reduce (+) |> Async2.RunSynchronously
    Assert.AreEqual(expected, actual)

[<Fact>]
let ``AsyncSeq2.ofSeq should work with large sequence``() =
  let largeSeq = seq { 1 .. 100 }
  let asyncSeq2 = AsyncSeq2.ofSeq largeSeq
  let result = asyncSeq2 |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
  Assert.AreEqual(100, result.Length)
  Assert.AreEqual(1, result.[0])
  Assert.AreEqual(100, result.[99])

[<Fact>]
let ``AsyncSeq2.mapAsync should preserve order with async transformations``() =
  let data = [1; 2; 3; 4; 5] |> AsyncSeq2.ofSeq
  let asyncTransform x = async2 {
    do! Async2.Sleep(50 - x * 10) // Shorter sleep for larger numbers
    return x * 2
  }

  let result = data
               |> AsyncSeq2.mapAsync asyncTransform
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([2; 4; 6; 8; 10], result)

[<Fact>]
let ``AsyncSeq2.mapAsync should propagate exceptions``() =
  let data = [1; 2; 3] |> AsyncSeq2.ofSeq
  let asyncTransform x = async2 {
    if x = 2 then failwith "test error"
    return x * 2
  }

  try
    data
    |> AsyncSeq2.mapAsync asyncTransform
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
    |> ignore
    Assert.Fail("Expected exception to be thrown")
  with
  | ex when ex.Message = "test error" -> () // Expected
  | ex -> Assert.Fail($"Unexpected exception: {ex.Message}")

[<Fact>]
let ``AsyncSeq2.chooseAsync should filter and transform``() =
  let data = [1; 2; 3; 4; 5] |> AsyncSeq2.ofSeq
  let asyncChoose x = async2 {
    if x % 2 = 0 then return Some (x * 10)
    else return None
  }

  let result = data
               |> AsyncSeq2.chooseAsync asyncChoose
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([20; 40], result)

[<Fact>]
let ``AsyncSeq2.filterAsync should work with async predicates``() =
  let data = [1; 2; 3; 4; 5] |> AsyncSeq2.ofSeq
  let asyncPredicate x = async2 {
    do! Async2.Sleep(1)
    return x % 2 = 1
  }

  let result = data
               |> AsyncSeq2.filterAsync asyncPredicate
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([1; 3; 5], result)

[<Fact>]
let ``AsyncSeq2.scan should work with accumulator``() =
  let data = [1; 2; 3; 4] |> AsyncSeq2.ofSeq
  let result = data
               |> AsyncSeq2.scan (+) 0
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([0; 1; 3; 6; 10], result)

[<Fact>]
let ``AsyncSeq2.scanAsync should work with async accumulator``() =
  let data = [1; 2; 3] |> AsyncSeq2.ofSeq
  let asyncFolder acc x = async2 {
    do! Async2.Sleep(1)
    return acc + x
  }
  let result = data
               |> AsyncSeq2.scanAsync asyncFolder 0
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([0; 1; 3; 6], result)

[<Fact>]
let ``AsyncSeq2.threadStateAsync should maintain state correctly``() =
  let data = [1; 2; 3; 4] |> AsyncSeq2.ofSeq
  let statefulFolder state x = async2 {
    let newState = state + 1
    let output = x * newState
    return (output, newState)
  }

  let result = data
               |> AsyncSeq2.threadStateAsync statefulFolder 0
               |> AsyncSeq2.toListAsync
               |> Async2.RunSynchronously
  Assert.AreEqual([1; 4; 9; 16], result)

[<Fact>]
let ``AsyncSeq2.lastOrDefault should return default for empty sequence``() =
  let result = (AsyncSeq2.empty ())
               |> AsyncSeq2.lastOrDefault 999
               |> Async2.RunSynchronously
  Assert.AreEqual(999, result)

[<Fact>]
let ``AsyncSeq2.lastOrDefault should return last element``() =
  let data = [1; 2; 3; 4; 5] |> AsyncSeq2.ofSeq
  let result = data
               |> AsyncSeq2.lastOrDefault 999
               |> Async2.RunSynchronously
  Assert.AreEqual(5, result)

[<Fact>]
let ``AsyncSeq2.toChannel and AsyncSeq2.fromChannel should work``() =
  let input = [1; 2; 4; 5; 6; 7; 8]
  let channel = Channel.CreateBounded(3)
  let actual =
    async2 {
      let! fillTask =
        input
        |> AsyncSeq2.ofSeq
        |> AsyncSeq2.toChannel channel.Writer
        |> Async2.StartChild

      let! toListTask =
        AsyncSeq2.fromChannel channel.Reader
        |> AsyncSeq2.toListAsync
        |> Async2.StartChild

      do! fillTask

      return! toListTask
    }
    |> Async2.RunSynchronously
  Assert.AreEqual(input, actual)

[<Fact>]
let ``AsyncSeq2.prefetch should not alter elements``() =
  let input = ["h"; "e"; "l"; "l"; "o"]
  let actual =
    input
    |> AsyncSeq2.ofSeq
    |> AsyncSeq2.prefetch
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual(input, actual)

[<Fact>]
let ``AsyncSeq2.toChannel and AsyncSeq2.fromChannel capture exns``() =
  async2 {
    let channel = Channel.CreateBounded(2)

    let! fillTask =
      asyncSeq2 {
        "a"
        "b"
        "c"
        failwith "Kaboom"
      }
      |> AsyncSeq2.toChannel channel.Writer
      |> Async2.StartChild

    let! toListTask =
      AsyncSeq2.fromChannel channel.Reader
      |> AsyncSeq2.toListAsync
      |> Async2.StartChild

    let! producer = Async2.Catch fillTask
    let! consumer = Async2.Catch toListTask
    match producer, consumer with
    | Choice2Of2 producerError, Choice2Of2 consumerError ->
      Assert.AreEqual("Kaboom", (disaggregate producerError).Message)
      Assert.AreEqual("Kaboom", (disaggregate consumerError).Message)
    | _ -> Assert.Fail("Both producer and consumer should propagate the channel error")
  }
  |> Async2.RunSynchronously

// ----------------------------------------------------------------------------
// Additional Coverage Tests targeting uncovered edge cases and branches

[<Fact>]
let ``AsyncSeq2.chunkBySize with size 1 should work`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.chunkBySize 1 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|1|]; [|2|]; [|3|]], result)

[<Fact>]
let ``AsyncSeq2.chunkBySize with empty sequence should return empty`` () =
  let result = AsyncSeq2.chunkBySize 2 (AsyncSeq2.empty ()) |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.chunkBySize with size larger than sequence should return partial`` () =
  let source = asyncSeq2 { yield 1; yield 2 }
  let result = AsyncSeq2.chunkBySize 5 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|1; 2|]], result)

[<Fact>]
let ``AsyncSeq2.pairwise with empty sequence should return empty`` () =
  let result = AsyncSeq2.pairwise (AsyncSeq2.empty ()) |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.pairwise with single element should return empty`` () =
  let source = asyncSeq2 { yield 42 }
  let result = AsyncSeq2.pairwise source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.pairwise with three elements should produce two pairs`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.pairwise source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([(1, 2); (2, 3)], result)

[<Fact>]
let ``AsyncSeq2.pairwise with many elements produces correct pairs`` () =
  let source = AsyncSeq2.ofSeq [1..10]
  let result = AsyncSeq2.pairwise source |> AsyncSeq2.toListSynchronously
  let expected = [(1,2); (2,3); (3,4); (4,5); (5,6); (6,7); (7,8); (8,9); (9,10)]
  Assert.AreEqual(expected, result)

[<Fact>]
let ``AsyncSeq2.windowed empty sequence returns empty`` () =
  let result = AsyncSeq2.windowed 3 (AsyncSeq2.empty<int> ()) |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.windowed fewer elements than window returns empty`` () =
  let source = asyncSeq2 { yield 1; yield 2 }
  let result = AsyncSeq2.windowed 3 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.windowed exact window size returns single window`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.windowed 3 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|1; 2; 3|]], result)

[<Fact>]
let ``AsyncSeq2.windowed sliding window produces correct windows`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4; yield 5 }
  let result = AsyncSeq2.windowed 3 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|1;2;3|]; [|2;3;4|]; [|3;4;5|]], result)

[<Fact>]
let ``AsyncSeq2.windowed size 1 returns each element as singleton array`` () =
  let source = asyncSeq2 { yield 10; yield 20; yield 30 }
  let result = AsyncSeq2.windowed 1 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|10|]; [|20|]; [|30|]], result)

[<Fact>]
let ``AsyncSeq2.windowed size 2 is equivalent to pairwise as arrays`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4 }
  let result = AsyncSeq2.windowed 2 source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([[|1;2|]; [|2;3|]; [|3;4|]], result)

[<Fact>]
let ``AsyncSeq2.windowed with size 0 raises ArgumentException`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.windowed 0 (asyncSeq2 { yield 1 }) |> AsyncSeq2.toListSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.distinctUntilChangedWith should work with custom equality`` () =
  let source = asyncSeq2 { yield "a"; yield "A"; yield "B"; yield "b"; yield "c" }
  let customEq (x: string) (y: string) = x.ToLower() = y.ToLower()
  let result = AsyncSeq2.distinctUntilChangedWith customEq source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual(["a"; "B"; "c"], result)

[<Fact>]
let ``AsyncSeq2.distinctUntilChangedWith with all same elements should return single`` () =
  let source = asyncSeq2 { yield 1; yield 1; yield 1 }
  let result = AsyncSeq2.distinctUntilChangedWith (=) source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([1], result)

[<Fact>]
let ``AsyncSeq2.append with both sequences having exceptions should propagate first`` () =
  async2 {
    let seq1 = asyncSeq2 { yield 1; failwith "error1" }
    let seq2 = asyncSeq2 { yield 2; failwith "error2" }
    let combined = AsyncSeq2.append seq1 seq2

    try
      let! _ = AsyncSeq2.toListAsync combined
      Assert.Fail("Expected exception to be thrown")
    with
    | ex when ex.Message = "error1" ->
      () // Expected - first sequence's error should be thrown
    | ex ->
      Assert.Fail($"Unexpected exception: {ex.Message}")
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.concat with nested exceptions should propagate properly`` () =
  async2 {
    let nested = asyncSeq2 {
      yield asyncSeq2 { yield 1; yield 2 }
      yield asyncSeq2 { failwith "nested error" }
      yield asyncSeq2 { yield 3 }
    }
    let flattened = AsyncSeq2.concat nested

    try
      let! result = AsyncSeq2.toListAsync flattened
      Assert.Fail("Expected exception to be thrown")
    with
    | ex when ex.Message = "nested error" ->
      () // Expected
    | ex ->
      Assert.Fail($"Unexpected exception: {ex.Message}")
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.choose with all None should return empty`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.choose (fun _ -> None) source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.choose with mixed Some and None should filter correctly`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4 }
  let chooser x = if x % 2 = 0 then Some (x * 2) else None
  let result = AsyncSeq2.choose chooser source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([4; 8], result)

[<Fact>]
let ``AsyncSeq2.chooseAsync with async transformation should work`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4 }
    let chooserAsync x = async2 {
      if x % 2 = 0 then return Some (x * 3) else return None
    }
    let! result = AsyncSeq2.chooseAsync chooserAsync source |> AsyncSeq2.toListAsync
    Assert.AreEqual([6; 12], result)
  } |> Async2.RunSynchronously

// Advanced Coverage Improvement Tests - targeting specific uncovered functionality

[<Fact>]
let ``AsyncSeqOp.FoldAsync with unfoldAsync should work`` () =
  async2 {
    // Create an AsyncSeq using unfoldAsync to trigger UnfoldAsyncEnumerator.FoldAsync path
    let generator state = async2 {
      if state < 5 then
        return Some (state * 2, state + 1)
      else
        return None
    }
    let source = AsyncSeq2.unfoldAsync generator 0

    // This should hit the uncovered FoldAsync method in UnfoldAsyncEnumerator
    let folder acc x = async2 { return acc + x }
    let! result = AsyncSeq2.foldAsync folder 0 source

    // Expected: sum of [0, 2, 4, 6, 8] = 20
    Assert.AreEqual(20, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeqOp.FoldAsync with empty sequence should return init`` () =
  async2 {
    let generator _ = async2 { return None }
    let source = AsyncSeq2.unfoldAsync generator 0
    let folder acc x = async2 { return acc + x }
    let! result = AsyncSeq2.foldAsync folder 42 source
    Assert.AreEqual(42, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeqOp.FoldAsync with exception in generator should propagate`` () =
  async2 {
    let generator state = async2 {
      if state = 0 then
        return Some (1, 1)
      else
        return failwith "generator error"
    }
    let source = AsyncSeq2.unfoldAsync generator 0

    try
      let folder acc x = async2 { return acc + x }
      let! _ = AsyncSeq2.foldAsync folder 0 source
      Assert.Fail("Expected exception to be thrown")
    with
    | ex when ex.Message = "generator error" ->
      () // Expected
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeqOp.FoldAsync with exception in folder should propagate`` () =
  async2 {
    let generator state = async2 {
      if state < 2 then
        return Some (state, state + 1)
      else
        return None
    }
    let source = AsyncSeq2.unfoldAsync generator 0

    try
      let folder acc x = async2 {
        if x = 1 then failwith "folder error"
        return acc + x
      }
      let! _ = AsyncSeq2.foldAsync folder 0 source
      Assert.Fail("Expected exception to be thrown")
    with
    | ex when ex.Message = "folder error" ->
      () // Expected
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.chunkBy empty sequence returns empty`` () =
  let result = AsyncSeq2.chunkBy id (AsyncSeq2.empty<int> ()) |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.chunkBy single element`` () =
  let source = asyncSeq2 { yield 42 }
  let result = AsyncSeq2.chunkBy id source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([(42, [|42|])], result)

[<Fact>]
let ``AsyncSeq2.chunkBy groups consecutive equal keys`` () =
  let source = asyncSeq2 { yield 1; yield 1; yield 2; yield 2; yield 1 }
  let result = AsyncSeq2.chunkBy id source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([(1, [|1;1|]); (2, [|2;2|]); (1, [|1|])], result)

[<Fact>]
let ``AsyncSeq2.chunkBy with projection`` () =
  let source = asyncSeq2 { yield 1; yield 3; yield 2; yield 4; yield 5 }
  let result = AsyncSeq2.chunkBy (fun x -> x % 2 = 0) source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([(false, [|1;3|]); (true, [|2;4|]); (false, [|5|])], result)

[<Fact>]
let ``AsyncSeq2.chunkByAsync groups consecutive equal keys`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 1; yield 2; yield 2 }
    let result = AsyncSeq2.chunkByAsync (fun x -> async2 { return x }) source |> AsyncSeq2.toListSynchronously
    Assert.AreEqual([(1, [|1;1|]); (2, [|2;2|])], result)
  } |> Async2.RunSynchronously



// ===== distinct / distinctBy / distinctByAsync =====

[<Fact>]
let ``AsyncSeq2.distinct removes all duplicates`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 1; yield 3; yield 2 }
  let result = AsyncSeq2.distinct source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([1; 2; 3], result)

[<Fact>]
let ``AsyncSeq2.distinct empty sequence returns empty`` () =
  let result = AsyncSeq2.distinct (AsyncSeq2.empty<int> ()) |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.distinct all unique elements returns all`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.distinct source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([1; 2; 3], result)

[<Fact>]
let ``AsyncSeq2.distinctBy removes duplicates by key`` () =
  let source = asyncSeq2 { yield (1, "a"); yield (2, "b"); yield (1, "c") }
  let result = AsyncSeq2.distinctBy fst source |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([(1, "a"); (2, "b")], result)

[<Fact>]
let ``AsyncSeq2.distinctByAsync removes duplicates by async key`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 2; yield 1; yield 3 }
    let result = AsyncSeq2.distinctByAsync (fun x -> async2 { return x % 2 }) source |> AsyncSeq2.toListSynchronously
    Assert.AreEqual([1; 2], result)
  } |> Async2.RunSynchronously

// ===== countBy / countByAsync =====

[<Fact>]
let ``AsyncSeq2.countBy counts elements by key`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 2; yield 1; yield 3; yield 2; yield 2 }
    let result = AsyncSeq2.countBy id source |> Async2.RunSynchronously
    let sorted = result |> Array.sortBy fst
    Assert.AreEqual([| (1, 2); (2, 3); (3, 1) |], sorted)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.countBy empty sequence returns empty array`` () =
  async2 {
    let result = AsyncSeq2.countBy id (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
    Assert.AreEqual([||], result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.countByAsync counts elements by async key`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4 }
    let result = AsyncSeq2.countByAsync (fun x -> async2 { return x % 2 }) source |> Async2.RunSynchronously
    let sorted = result |> Array.sortBy fst
    Assert.AreEqual([| (0, 2); (1, 2) |], sorted)
  } |> Async2.RunSynchronously

// ===== exactlyOne / tryExactlyOne =====

[<Fact>]
let ``AsyncSeq2.exactlyOne returns single element`` () =
  async2 {
    let source = asyncSeq2 { yield 42 }
    let result = AsyncSeq2.exactlyOne source |> Async2.RunSynchronously
    Assert.AreEqual(42, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.exactlyOne raises on empty sequence`` () =
  Assert.Throws<ArgumentException>(fun () ->
    AsyncSeq2.exactlyOne (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.exactlyOne raises on sequence with more than one element`` () =
  Assert.Throws<ArgumentException>(fun () ->
    asyncSeq2 { yield 1; yield 2 } |> AsyncSeq2.exactlyOne |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.tryExactlyOne returns Some for single element`` () =
  async2 {
    let source = asyncSeq2 { yield 42 }
    let result = AsyncSeq2.tryExactlyOne source |> Async2.RunSynchronously
    Assert.AreEqual(Some 42, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.tryExactlyOne returns None for empty sequence`` () =
  async2 {
    let result = AsyncSeq2.tryExactlyOne (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
    Assert.AreEqual(None, result)
  } |> Async2.RunSynchronously

[<Fact>]
let ``AsyncSeq2.tryExactlyOne returns None for sequence with more than one element`` () =
  async2 {
    let source = asyncSeq2 { yield 1; yield 2 }
    let result = AsyncSeq2.tryExactlyOne source |> Async2.RunSynchronously
    Assert.AreEqual(None, result)
  } |> Async2.RunSynchronously

// ===== head =====

[<Fact>]
let ``AsyncSeq2.head returns first element`` () =
  let source = asyncSeq2 { yield 42; yield 99 }
  let result = AsyncSeq2.head source |> Async2.RunSynchronously
  Assert.AreEqual(42, result)

[<Fact>]
let ``AsyncSeq2.head raises on empty sequence`` () =
  Assert.Throws<ArgumentException>(fun () ->
    AsyncSeq2.head (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously |> ignore) |> ignore

// ===== iteri =====

[<Fact>]
let ``AsyncSeq2.iteri calls action with correct indices`` () =
  let indices = System.Collections.Generic.List<int>()
  let values  = System.Collections.Generic.List<int>()
  asyncSeq2 { yield 10; yield 20; yield 30 }
  |> AsyncSeq2.iteri (fun i v -> indices.Add(i); values.Add(v))
  |> Async2.RunSynchronously
  Assert.AreEqual([| 0; 1; 2 |], indices |> Seq.toArray)
  Assert.AreEqual([| 10; 20; 30 |], values |> Seq.toArray)

[<Fact>]
let ``AsyncSeq2.iteri on empty sequence does nothing`` () =
  let mutable count = 0
  (AsyncSeq2.empty<int> ())
  |> AsyncSeq2.iteri (fun _ _ -> count <- count + 1)
  |> Async2.RunSynchronously
  Assert.AreEqual(0, count)

// ===== find / tryFindAsync =====

[<Fact>]
let ``AsyncSeq2.find returns matching element`` () =
  for i in 0 .. 10 do
    let ls = [ 1 .. i + 1 ]
    let result = AsyncSeq2.ofSeq ls |> AsyncSeq2.find (fun x -> x = i + 1) |> Async2.RunSynchronously
    Assert.AreEqual(i + 1, result)

[<Fact>]
let ``AsyncSeq2.find raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.find (fun x -> x = 99) |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.tryFindAsync returns Some when found`` () =
  for i in 0 .. 10 do
    let ls = [ 1 .. i + 1 ]
    let result =
      AsyncSeq2.ofSeq ls
      |> AsyncSeq2.tryFindAsync (fun x -> async2 { return x = i + 1 })
      |> Async2.RunSynchronously
    Assert.AreEqual(Some (i + 1), result)

[<Fact>]
let ``AsyncSeq2.tryFindAsync returns None when not found`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.tryFindAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

// ===== tail =====

[<Fact>]
let ``AsyncSeq2.tail skips first element`` () =
  let result =
    asyncSeq2 { yield 1; yield 2; yield 3 }
    |> AsyncSeq2.tail
    |> Async2.RunSynchronously
    |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([ 2; 3 ], result)

[<Fact>]
let ``AsyncSeq2.tail on singleton returns empty`` () =
  let result =
    asyncSeq2 { yield 42 }
    |> AsyncSeq2.tail
    |> Async2.RunSynchronously
    |> AsyncSeq2.toListSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.tail on empty raises`` () =
  Assert.Throws<ArgumentException>(fun () ->
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.tail
    |> Async2.RunSynchronously
    |> ignore) |> ignore

// ===== findAsync / existsAsync / forallAsync =====

[<Fact>]
let ``AsyncSeq2.findAsync returns matching element`` () =
  for i in 0 .. 10 do
    let ls = [ 1 .. i + 1 ]
    let result =
      AsyncSeq2.ofSeq ls
      |> AsyncSeq2.findAsync (fun x -> async2 { return x = i + 1 })
      |> Async2.RunSynchronously
    Assert.AreEqual(i + 1, result)

[<Fact>]
let ``AsyncSeq2.findAsync raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.findAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.existsAsync returns true when element found`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.existsAsync (fun x -> async2 { return x = 2 })
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

[<Fact>]
let ``AsyncSeq2.existsAsync returns false on empty sequence`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.existsAsync (fun _ -> async2 { return true })
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.existsAsync returns false when no match`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.existsAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.forallAsync returns true when all match`` () =
  let result =
    AsyncSeq2.ofSeq [ 2; 4; 6 ]
    |> AsyncSeq2.forallAsync (fun x -> async2 { return x % 2 = 0 })
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

[<Fact>]
let ``AsyncSeq2.forallAsync returns false when some do not match`` () =
  let result =
    AsyncSeq2.ofSeq [ 2; 3; 6 ]
    |> AsyncSeq2.forallAsync (fun x -> async2 { return x % 2 = 0 })
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.forallAsync returns true on empty sequence`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.forallAsync (fun _ -> async2 { return false })
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

// ===== last =====

[<Fact>]
let ``AsyncSeq2.last returns last element`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.last source |> Async2.RunSynchronously
  Assert.AreEqual(3, result)

[<Fact>]
let ``AsyncSeq2.last on singleton returns that element`` () =
  let result = AsyncSeq2.last (AsyncSeq2.singleton 42) |> Async2.RunSynchronously
  Assert.AreEqual(42, result)

[<Fact>]
let ``AsyncSeq2.last raises on empty sequence`` () =
  Assert.Throws<ArgumentException>(fun () ->
    AsyncSeq2.last (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously |> ignore) |> ignore

// ===== item =====

[<Fact>]
let ``AsyncSeq2.item returns element at index 0`` () =
  let source = asyncSeq2 { yield 10; yield 20; yield 30 }
  let result = AsyncSeq2.item 0 source |> Async2.RunSynchronously
  Assert.AreEqual(10, result)

[<Fact>]
let ``AsyncSeq2.item returns element at index 2`` () =
  let source = asyncSeq2 { yield 10; yield 20; yield 30 }
  let result = AsyncSeq2.item 2 source |> Async2.RunSynchronously
  Assert.AreEqual(30, result)

[<Fact>]
let ``AsyncSeq2.item raises when index out of bounds`` () =
  Assert.Throws<ArgumentException>(fun () ->
    AsyncSeq2.item 5 (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.item raises when index negative`` () =
  Assert.Throws<ArgumentException>(fun () ->
    AsyncSeq2.item -1 (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously |> ignore) |> ignore

// ===== tryItem =====

[<Fact>]
let ``AsyncSeq2.tryItem returns Some for valid index`` () =
  let source = asyncSeq2 { yield 10; yield 20; yield 30 }
  let result = AsyncSeq2.tryItem 1 source |> Async2.RunSynchronously
  Assert.AreEqual(Some 20, result)

[<Fact>]
let ``AsyncSeq2.tryItem returns None for out-of-bounds index`` () =
  let result = AsyncSeq2.tryItem 10 (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryItem returns None for negative index`` () =
  let result = AsyncSeq2.tryItem -1 (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryItem returns None on empty sequence`` () =
  let result = AsyncSeq2.tryItem 0 (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

// ===== isEmpty =====

[<Fact>]
let ``AsyncSeq2.isEmpty returns true for empty sequence`` () =
  let result = AsyncSeq2.isEmpty (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.True(result)

[<Fact>]
let ``AsyncSeq2.isEmpty returns false for non-empty sequence`` () =
  let source = asyncSeq2 { yield 1; yield 2 }
  let result = AsyncSeq2.isEmpty source |> Async2.RunSynchronously
  Assert.False(result)

[<Fact>]
let ``AsyncSeq2.isEmpty returns false for singleton`` () =
  let result = AsyncSeq2.isEmpty (AsyncSeq2.singleton 42) |> Async2.RunSynchronously
  Assert.False(result)

// ===== tryHead =====

[<Fact>]
let ``AsyncSeq2.tryHead returns Some for non-empty sequence`` () =
  let source = asyncSeq2 { yield 42; yield 99 }
  let result = AsyncSeq2.tryHead source |> Async2.RunSynchronously
  Assert.AreEqual(Some 42, result)

[<Fact>]
let ``AsyncSeq2.tryHead returns None for empty sequence`` () =
  let result = AsyncSeq2.tryHead (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

// ===== except =====

[<Fact>]
let ``AsyncSeq2.except removes excluded elements`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4; yield 5 }
  let result = AsyncSeq2.exceptOfSeq [2; 4] source |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 3; 5 |], result)

[<Fact>]
let ``AsyncSeq2.except with empty excluded returns all elements`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.exceptOfSeq [] source |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.except with all excluded returns empty sequence`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let result = AsyncSeq2.exceptOfSeq [1; 2; 3] source |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

// ===== findIndex / tryFindIndex / findIndexAsync / tryFindIndexAsync =====

[<Fact>]
let ``AsyncSeq2.findIndex returns index of first matching element`` () =
  for i in 0 .. 9 do
    let ls = [ 1 .. 10 ]
    let result = AsyncSeq2.ofSeq ls |> AsyncSeq2.findIndex (fun x -> x = i + 1) |> Async2.RunSynchronously
    Assert.AreEqual(i, result)

[<Fact>]
let ``AsyncSeq2.findIndex raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.findIndex (fun x -> x = 99) |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.tryFindIndex returns Some index when found`` () =
  let ls = [ 10; 20; 30; 40 ]
  let result = AsyncSeq2.ofSeq ls |> AsyncSeq2.tryFindIndex (fun x -> x = 30) |> Async2.RunSynchronously
  Assert.AreEqual(Some 2, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndex returns None when not found`` () =
  let result = AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.tryFindIndex (fun x -> x = 99) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndex returns Some 0 for first element`` () =
  let result = AsyncSeq2.ofSeq [ 5; 6; 7 ] |> AsyncSeq2.tryFindIndex (fun x -> x = 5) |> Async2.RunSynchronously
  Assert.AreEqual(Some 0, result)

[<Fact>]
let ``AsyncSeq2.findIndexAsync returns index of first matching element`` () =
  let ls = [ 1; 2; 3; 4; 5 ]
  let result =
    AsyncSeq2.ofSeq ls
    |> AsyncSeq2.findIndexAsync (fun x -> async2 { return x = 3 })
    |> Async2.RunSynchronously
  Assert.AreEqual(2, result)

[<Fact>]
let ``AsyncSeq2.findIndexAsync raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.findIndexAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.tryFindIndexAsync returns Some index when found`` () =
  let result =
    AsyncSeq2.ofSeq [ 10; 20; 30 ]
    |> AsyncSeq2.tryFindIndexAsync (fun x -> async2 { return x = 20 })
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 1, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndexAsync returns None when not found`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.tryFindIndexAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

// ===== tryFindIndexBack / findIndexBack / tryFindIndexBackAsync / findIndexBackAsync =====

[<Fact>]
let ``AsyncSeq2.tryFindIndexBack returns index of last matching element`` () =
  let result = AsyncSeq2.ofSeq [ 1; 2; 3; 2; 1 ] |> AsyncSeq2.tryFindIndexBack (fun x -> x = 2) |> Async2.RunSynchronously
  Assert.AreEqual(Some 3, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndexBack returns None when no match`` () =
  let result = AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.tryFindIndexBack (fun x -> x = 99) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndexBack returns None for empty sequence`` () =
  let result = (AsyncSeq2.empty<int> ()) |> AsyncSeq2.tryFindIndexBack (fun _ -> true) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndexBack returns index of last element when all match`` () =
  let result = AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.tryFindIndexBack (fun _ -> true) |> Async2.RunSynchronously
  Assert.AreEqual(Some 2, result)

[<Fact>]
let ``AsyncSeq2.findIndexBack returns index of last matching element`` () =
  let result = AsyncSeq2.ofSeq [ 10; 20; 30; 20; 10 ] |> AsyncSeq2.findIndexBack (fun x -> x = 20) |> Async2.RunSynchronously
  Assert.AreEqual(3, result)

[<Fact>]
let ``AsyncSeq2.findIndexBack raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.findIndexBack (fun x -> x = 99) |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.tryFindIndexBackAsync returns index of last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 2; 1 ]
    |> AsyncSeq2.tryFindIndexBackAsync (fun x -> async2 { return x = 2 })
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 3, result)

[<Fact>]
let ``AsyncSeq2.tryFindIndexBackAsync returns None when no match`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.tryFindIndexBackAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.findIndexBackAsync returns index of last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 5; 4; 3; 4; 5 ]
    |> AsyncSeq2.findIndexBackAsync (fun x -> async2 { return x = 4 })
    |> Async2.RunSynchronously
  Assert.AreEqual(3, result)

[<Fact>]
let ``AsyncSeq2.findIndexBackAsync raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.findIndexBackAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== tryFindBack / findBack / tryFindBackAsync / findBackAsync =====

[<Fact>]
let ``AsyncSeq2.tryFindBack returns last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5 ]
    |> AsyncSeq2.tryFindBack (fun x -> x % 2 = 0)
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 4, result)

[<Fact>]
let ``AsyncSeq2.tryFindBack returns None when no match`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 3; 5 ]
    |> AsyncSeq2.tryFindBack (fun x -> x % 2 = 0)
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryFindBack returns None on empty sequence`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.tryFindBack (fun _ -> true)
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryFindBack returns only element when singleton matches`` () =
  let result =
    AsyncSeq2.singleton 42
    |> AsyncSeq2.tryFindBack (fun x -> x = 42)
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 42, result)

[<Fact>]
let ``AsyncSeq2.findBack returns last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5 ]
    |> AsyncSeq2.findBack (fun x -> x < 4)
    |> Async2.RunSynchronously
  Assert.AreEqual(3, result)

[<Fact>]
let ``AsyncSeq2.findBack raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ] |> AsyncSeq2.findBack (fun x -> x = 99) |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.tryFindBackAsync returns last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5 ]
    |> AsyncSeq2.tryFindBackAsync (fun x -> async2 { return x % 2 = 0 })
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 4, result)

[<Fact>]
let ``AsyncSeq2.tryFindBackAsync returns None when no match`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 3; 5 ]
    |> AsyncSeq2.tryFindBackAsync (fun x -> async2 { return x % 2 = 0 })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.findBackAsync returns last matching element`` () =
  let result =
    AsyncSeq2.ofSeq [ 10; 20; 30; 40 ]
    |> AsyncSeq2.findBackAsync (fun x -> async2 { return x < 35 })
    |> Async2.RunSynchronously
  Assert.AreEqual(30, result)

[<Fact>]
let ``AsyncSeq2.findBackAsync raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.findBackAsync (fun x -> async2 { return x = 99 })
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== sortWith =====

[<Fact>]
let ``AsyncSeq2.sortWith sorts using custom comparer`` () =
  let source = asyncSeq2 { yield 3; yield 1; yield 4; yield 1; yield 5 }
  let result = AsyncSeq2.sortWith compare source |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 1; 3; 4; 5 |], result)

[<Fact>]
let ``AsyncSeq2.sortWith sorts descending with negated comparer`` () =
  let source = asyncSeq2 { yield 3; yield 1; yield 4; yield 1; yield 5 }
  let result = AsyncSeq2.sortWith (fun a b -> compare b a) source |> Async2.RunSynchronously
  Assert.AreEqual([| 5; 4; 3; 1; 1 |], result)

[<Fact>]
let ``AsyncSeq2.sortWith returns empty array for empty sequence`` () =
  let result = AsyncSeq2.sortWith compare (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

// ===== sortAsync / sortByAsync / sortDescendingAsync / sortByDescendingAsync / sortWithAsync =====

[<Fact>]
let ``AsyncSeq2.sortAsync sorts in ascending order`` () =
  let result = AsyncSeq2.ofSeq [ 3; 1; 4; 1; 5; 9 ] |> AsyncSeq2.sortAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 1; 3; 4; 5; 9 |], result)

[<Fact>]
let ``AsyncSeq2.sortAsync returns empty array for empty sequence`` () =
  let result = (AsyncSeq2.empty<int> ()) |> AsyncSeq2.sortAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.sortByAsync sorts by projected key`` () =
  let result =
    AsyncSeq2.ofSeq [ "banana"; "apple"; "cherry" ]
    |> AsyncSeq2.sortByAsync (fun s -> s.Length)
    |> Async2.RunSynchronously
  Assert.AreEqual([| "apple"; "banana"; "cherry" |], result)

[<Fact>]
let ``AsyncSeq2.sortDescendingAsync sorts in descending order`` () =
  let result = AsyncSeq2.ofSeq [ 3; 1; 4; 1; 5 ] |> AsyncSeq2.sortDescendingAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 5; 4; 3; 1; 1 |], result)

[<Fact>]
let ``AsyncSeq2.sortByDescendingAsync sorts by projected key descending`` () =
  let result =
    AsyncSeq2.ofSeq [ "apple"; "banana"; "fig" ]
    |> AsyncSeq2.sortByDescendingAsync (fun s -> s.Length)
    |> Async2.RunSynchronously
  Assert.AreEqual([| "banana"; "apple"; "fig" |], result)

[<Fact>]
let ``AsyncSeq2.sortWithAsync sorts using custom comparer`` () =
  let result =
    AsyncSeq2.ofSeq [ 3; 1; 4; 1; 5 ]
    |> AsyncSeq2.sortWithAsync (fun a b -> compare b a)
    |> Async2.RunSynchronously
  Assert.AreEqual([| 5; 4; 3; 1; 1 |], result)

// ── AsyncSeq2.mapFold ──────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.mapFold maps elements and accumulates state`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let results, finalState =
    AsyncSeq2.mapFold (fun acc x -> (x * 2, acc + x)) 0 source |> Async2.RunSynchronously
  Assert.AreEqual([| 2; 4; 6 |], results)
  Assert.AreEqual(6, finalState)

[<Fact>]
let ``AsyncSeq2.mapFold returns empty array and initial state for empty sequence`` () =
  let results, finalState =
    AsyncSeq2.mapFold (fun acc x -> (x, acc + x)) 99 (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.AreEqual([||], results)
  Assert.AreEqual(99, finalState)

[<Fact>]
let ``AsyncSeq2.mapFoldAsync maps elements asynchronously and accumulates state`` () =
  let source = asyncSeq2 { yield 10; yield 20; yield 30 }
  let results, finalState =
    AsyncSeq2.mapFoldAsync (fun acc x -> async2 { return (x + 1, acc + x) }) 0 source
    |> Async2.RunSynchronously
  Assert.AreEqual([| 11; 21; 31 |], results)
  Assert.AreEqual(60, finalState)

[<Fact>]
let ``AsyncSeq2.mapFoldAsync returns empty array and initial state for empty sequence`` () =
  let results, finalState =
    AsyncSeq2.mapFoldAsync (fun acc x -> async2 { return (x, acc + 1) }) 5 (AsyncSeq2.empty<int> ())
    |> Async2.RunSynchronously
  Assert.AreEqual([||], results)
  Assert.AreEqual(5, finalState)

// ── AsyncSeq2.allPairs ────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.allPairs returns cartesian product`` () =
  let s1 = asyncSeq2 { yield 1; yield 2 }
  let s2 = asyncSeq2 { yield 'a'; yield 'b'; yield 'c' }
  let result =
    AsyncSeq2.allPairs s1 s2 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual(
    [| (1,'a'); (1,'b'); (1,'c'); (2,'a'); (2,'b'); (2,'c') |],
    result)

[<Fact>]
let ``AsyncSeq2.allPairs returns empty when first source is empty`` () =
  let result =
    AsyncSeq2.allPairs (AsyncSeq2.empty<int> ()) (asyncSeq2 { yield 1; yield 2 })
    |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.allPairs returns empty when second source is empty`` () =
  let result =
    AsyncSeq2.allPairs (asyncSeq2 { yield 1; yield 2 }) (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

// ── AsyncSeq2.unzip ───────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.unzip splits pairs into two arrays`` () =
  let source = asyncSeq2 { yield (1, 'a'); yield (2, 'b'); yield (3, 'c') }
  let (lefts, rights) = AsyncSeq2.unzip source |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], lefts)
  Assert.AreEqual([| 'a'; 'b'; 'c' |], rights)

[<Fact>]
let ``AsyncSeq2.unzip empty source returns two empty arrays`` () =
  let (lefts, rights) = AsyncSeq2.unzip (AsyncSeq2.empty<int * string> ()) |> Async2.RunSynchronously
  Assert.AreEqual([||], lefts)
  Assert.AreEqual([||], rights)

[<Fact>]
let ``AsyncSeq2.unzip mirrors List.unzip`` () =
  let pairs = [ (1, 'x'); (2, 'y'); (3, 'z') ]
  let (expL, expR) = List.unzip pairs
  let (actL, actR) = AsyncSeq2.unzip (AsyncSeq2.ofList pairs) |> Async2.RunSynchronously
  Assert.AreEqual(expL |> Array.ofList, actL)
  Assert.AreEqual(expR |> Array.ofList, actR)

// ── AsyncSeq2.unzip3 ──────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.unzip3 splits triples into three arrays`` () =
  let source = asyncSeq2 { yield (1, 'a', true); yield (2, 'b', false); yield (3, 'c', true) }
  let (as1, as2, as3) = AsyncSeq2.unzip3 source |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], as1)
  Assert.AreEqual([| 'a'; 'b'; 'c' |], as2)
  Assert.AreEqual([| true; false; true |], as3)

[<Fact>]
let ``AsyncSeq2.unzip3 empty source returns three empty arrays`` () =
  let (as1, as2, as3) = AsyncSeq2.unzip3 (AsyncSeq2.empty<int * string * bool> ()) |> Async2.RunSynchronously
  Assert.AreEqual([||], as1)
  Assert.AreEqual([||], as2)
  Assert.AreEqual([||], as3)

// ── AsyncSeq2.map2 ────────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.map2 applies function pairwise`` () =
  let s1 = asyncSeq2 { yield 1; yield 2; yield 3 }
  let s2 = asyncSeq2 { yield 10; yield 20; yield 30 }
  let result = AsyncSeq2.map2 (+) s1 s2 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 11; 22; 33 |], result)

[<Fact>]
let ``AsyncSeq2.map2 stops at shorter sequence`` () =
  let s1 = asyncSeq2 { yield 1; yield 2; yield 3 }
  let s2 = asyncSeq2 { yield 10; yield 20 }
  let result = AsyncSeq2.map2 (+) s1 s2 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 11; 22 |], result)

// ── AsyncSeq2.map3 ────────────────────────────────────────────────────────────

[<Fact>]
let ``AsyncSeq2.map3 applies function to three sequences`` () =
  let s1 = asyncSeq2 { yield 1; yield 2; yield 3 }
  let s2 = asyncSeq2 { yield 10; yield 20; yield 30 }
  let s3 = asyncSeq2 { yield 100; yield 200; yield 300 }
  let result = AsyncSeq2.map3 (fun a b c -> a + b + c) s1 s2 s3 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 111; 222; 333 |], result)

[<Fact>]
let ``AsyncSeq2.map3 stops at shortest sequence`` () =
  let s1 = asyncSeq2 { yield 1; yield 2; yield 3 }
  let s2 = asyncSeq2 { yield 10; yield 20; yield 30 }
  let s3 = asyncSeq2 { yield 100 }
  let result = AsyncSeq2.map3 (fun a b c -> a + b + c) s1 s2 s3 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 111 |], result)

[<Fact>]
let ``AsyncSeq2.rev reverses a sequence`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4; yield 5 }
  let result = AsyncSeq2.rev source |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 5; 4; 3; 2; 1 |], result)

[<Fact>]
let ``AsyncSeq2.rev returns empty sequence for empty input`` () =
  let result = AsyncSeq2.rev (AsyncSeq2.empty<int> ()) |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.rev returns singleton for single element`` () =
  let result = AsyncSeq2.rev (asyncSeq2 { yield 42 }) |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 42 |], result)

[<Fact>]
let ``AsyncSeq2.splitAt splits a sequence at the given index`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3; yield 4; yield 5 }
  let first, rest = AsyncSeq2.splitAt 3 source |> Async2.RunSynchronously
  let restArr = rest |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], first)
  Assert.AreEqual([| 4; 5 |], restArr)

[<Fact>]
let ``AsyncSeq2.splitAt with count=0 returns empty array and full rest`` () =
  let source = asyncSeq2 { yield 10; yield 20 }
  let first, rest = AsyncSeq2.splitAt 0 source |> Async2.RunSynchronously
  let restArr = rest |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], first)
  Assert.AreEqual([| 10; 20 |], restArr)

[<Fact>]
let ``AsyncSeq2.splitAt with count >= length returns all elements in first and empty rest`` () =
  let source = asyncSeq2 { yield 1; yield 2; yield 3 }
  let first, rest = AsyncSeq2.splitAt 10 source |> Async2.RunSynchronously
  let restArr = rest |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], first)
  Assert.AreEqual([||], restArr)

[<Fact>]
let ``AsyncSeq2.splitAt on empty sequence returns empty first and empty rest`` () =
  let first, rest = AsyncSeq2.splitAt 3 (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  let restArr = rest |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], first)
  Assert.AreEqual([||], restArr)

[<Fact>]
let ``AsyncSeq2.splitAt with count equal to length returns all in first and empty rest`` () =
  let source = asyncSeq2 { yield 7; yield 8; yield 9 }
  let first, rest = AsyncSeq2.splitAt 3 source |> Async2.RunSynchronously
  let restArr = rest |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 7; 8; 9 |], first)
  Assert.AreEqual([||], restArr)

[<Fact>]
let ``AsyncSeq2.splitAt with negative count throws ArgumentException`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.splitAt -1 (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.splitAt disposes enumerator when source throws during collection`` () =
  let mutable disposed = false
  let source = asyncSeq2 {
    use _ = { new System.IDisposable with member _.Dispose() = disposed <- true }
    yield 1
    yield 2
    failwith "source error"
  }
  try AsyncSeq2.splitAt 10 source |> Async2.RunSynchronously |> ignore
  with _ -> ()
  Assert.IsTrue(disposed, "enumerator should be disposed after exception during collection")

// ===== removeAt =====

[<Fact>]
let ``AsyncSeq2.removeAt removes the element at the specified index`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5 ]
    |> AsyncSeq2.removeAt 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 4; 5 |], result)

[<Fact>]
let ``AsyncSeq2.removeAt removes the first element (index 0)`` () =
  let result =
    AsyncSeq2.ofSeq [ 10; 20; 30 ]
    |> AsyncSeq2.removeAt 0
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 20; 30 |], result)

[<Fact>]
let ``AsyncSeq2.removeAt removes the last element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeAt 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2 |], result)

[<Fact>]
let ``AsyncSeq2.removeAt raises ArgumentException for negative index`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeAt -1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.removeAt raises ArgumentException when index is out of range`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeAt 10
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.removeAt raises ArgumentException when index equals sequence length`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeAt 3
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== updateAt =====

[<Fact>]
let ``AsyncSeq2.updateAt replaces element at specified index`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
    |> AsyncSeq2.updateAt 1 99
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 99; 3; 4 |], result)

[<Fact>]
let ``AsyncSeq2.updateAt replaces first element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.updateAt 0 99
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 99; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.updateAt replaces last element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.updateAt 2 99
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 99 |], result)

[<Fact>]
let ``AsyncSeq2.updateAt raises ArgumentException for negative index`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.updateAt -1 0
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.updateAt raises ArgumentException when index is out of range`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.updateAt 10 99
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.updateAt raises ArgumentException when index equals sequence length`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.updateAt 3 99
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== insertAt =====

[<Fact>]
let ``AsyncSeq2.insertAt inserts element at specified index`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 4; 5 ]
    |> AsyncSeq2.insertAt 2 3
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 4; 5 |], result)

[<Fact>]
let ``AsyncSeq2.insertAt inserts at index 0 (prepend)`` () =
  let result =
    AsyncSeq2.ofSeq [ 2; 3 ]
    |> AsyncSeq2.insertAt 0 1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.insertAt appends when index equals sequence length`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertAt 3 4
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 4 |], result)

[<Fact>]
let ``AsyncSeq2.insertAt inserts into empty sequence at index 0`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.insertAt 0 42
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 42 |], result)

[<Fact>]
let ``AsyncSeq2.insertAt raises ArgumentException for negative index`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertAt -1 0
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.insertAt raises ArgumentException when index exceeds length`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertAt 5 0
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== insertManyAt =====

[<Fact>]
let ``AsyncSeq2.insertManyAt inserts multiple elements at specified index`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt 1 (AsyncSeq2.ofSeq [ 10; 20 ])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 10; 20; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.insertManyAt prepends when index is 0`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt 0 (AsyncSeq2.ofSeq [ 10; 20 ])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 10; 20; 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.insertManyAt appends when index equals sequence length`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt 3 (AsyncSeq2.ofSeq [ 10; 20 ])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 10; 20 |], result)

[<Fact>]
let ``AsyncSeq2.insertManyAt with empty values returns original sequence`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt 1 (AsyncSeq2.ofSeq [])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.insertManyAt raises ArgumentException for negative index`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt -1 (AsyncSeq2.ofSeq [ 10 ])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.insertManyAt raises ArgumentException when index exceeds length`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.insertManyAt 5 (AsyncSeq2.ofSeq [ 10 ])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

// ===== removeManyAt =====

[<Fact>]
let ``AsyncSeq2.removeManyAt removes elements at specified index and count`` () =
  let result =
    AsyncSeq2.ofSeq [ 0; 1; 2; 3; 4 ]
    |> AsyncSeq2.removeManyAt 1 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 0; 3; 4 |], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt removes from beginning`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
    |> AsyncSeq2.removeManyAt 0 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 3; 4 |], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt removes from end`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4 ]
    |> AsyncSeq2.removeManyAt 2 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2 |], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt with count 0 returns original sequence`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeManyAt 1 0
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt removes all elements`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeManyAt 0 3
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt raises ArgumentException for negative index`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeManyAt -1 1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.removeManyAt negative count leaves sequence unchanged`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeManyAt 0 -1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.removeManyAt removes to end when range exceeds sequence`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.removeManyAt 2 2
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2 |], result)

// ===== splitInto =====

[<Fact>]
let ``AsyncSeq2.splitInto splits sequence into equal chunks`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3; 4; 5; 6 ]
    |> AsyncSeq2.splitInto 3
    |> Async2.RunSynchronously
  Assert.AreEqual(3, result.Length)
  Assert.AreEqual([| 1; 2 |], result.[0])
  Assert.AreEqual([| 3; 4 |], result.[1])
  Assert.AreEqual([| 5; 6 |], result.[2])

[<Fact>]
let ``AsyncSeq2.splitInto distributes remainder to first chunks`` () =
  let result =
    AsyncSeq2.ofSeq [ 1 .. 7 ]
    |> AsyncSeq2.splitInto 3
    |> Async2.RunSynchronously
  Assert.AreEqual(3, result.Length)
  Assert.AreEqual([| 1; 2; 3 |], result.[0])
  Assert.AreEqual([| 4; 5 |], result.[1])
  Assert.AreEqual([| 6; 7 |], result.[2])

[<Fact>]
let ``AsyncSeq2.splitInto with count 1 returns single chunk`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.splitInto 1
    |> Async2.RunSynchronously
  Assert.AreEqual(1, result.Length)
  Assert.AreEqual([| 1; 2; 3 |], result.[0])

[<Fact>]
let ``AsyncSeq2.splitInto with count greater than length returns one chunk per element`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.splitInto 10
    |> Async2.RunSynchronously
  Assert.AreEqual(3, result.Length)
  Assert.AreEqual([| 1 |], result.[0])
  Assert.AreEqual([| 2 |], result.[1])
  Assert.AreEqual([| 3 |], result.[2])

[<Fact>]
let ``AsyncSeq2.splitInto with empty sequence returns empty array`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.splitInto 3
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.splitInto raises ArgumentException when count is zero`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.splitInto 0
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.take more than length raises and truncate returns all elements`` () =
  let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
  Assert.Throws<ArgumentException>(fun () ->
    source |> AsyncSeq2.take 10 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously |> ignore) |> ignore
  let result =
    source
    |> AsyncSeq2.truncate 10
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.take raises ArgumentException for negative count`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.take -1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.take from infinite sequence`` () =
  let result =
    AsyncSeq2.replicateInfinite 7
    |> AsyncSeq2.take 5
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 7; 7; 7; 7; 7 |], result)

[<Fact>]
let ``AsyncSeq2.skip more than length raises and drop returns empty`` () =
  let source = AsyncSeq2.ofSeq [ 1; 2; 3 ]
  Assert.Throws<ArgumentException>(fun () ->
    source |> AsyncSeq2.skip 10 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously |> ignore) |> ignore
  let result =
    source
    |> AsyncSeq2.drop 10
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.skip raises ArgumentException for negative count`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.skip -1
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously |> ignore)
  |> ignore

[<Fact>]
let ``AsyncSeq2.take then skip roundtrip`` () =
  let source = [| 1..20 |]
  let result =
    AsyncSeq2.ofSeq source
    |> AsyncSeq2.skip 5
    |> AsyncSeq2.take 10
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 6..15 |], result)

// ===== withCancellation =====

[<Fact>]
let ``AsyncSeq2.withCancellation passes token to enumerator`` () =
  use cts = new System.Threading.CancellationTokenSource()
  let receivedToken = ref System.Threading.CancellationToken.None
  let source =
    { new System.Collections.Generic.IAsyncEnumerable<int> with
        member _.GetAsyncEnumerator(ct) =
          receivedToken.Value <- ct
          (AsyncSeq2.ofSeq [1; 2; 3]).GetAsyncEnumerator(ct) }
  source
  |> AsyncSeq2.withCancellation cts.Token
  |> AsyncSeq2.toArrayAsync
  |> Async2.RunSynchronously
  |> ignore
  Assert.AreEqual(cts.Token, receivedToken.Value)

[<Fact>]
let ``AsyncSeq2.withCancellation overrides incoming token`` () =
  use cts1 = new System.Threading.CancellationTokenSource()
  use cts2 = new System.Threading.CancellationTokenSource()
  let receivedToken = ref System.Threading.CancellationToken.None
  let source : System.Collections.Generic.IAsyncEnumerable<int> =
    { new System.Collections.Generic.IAsyncEnumerable<int> with
        member _.GetAsyncEnumerator(ct) =
          receivedToken.Value <- ct
          (AsyncSeq2.ofSeq [1; 2; 3]).GetAsyncEnumerator(ct) }
  let wrapped = source |> AsyncSeq2.withCancellation cts1.Token
  // Enumerate with cts2's token - withCancellation should still pass cts1's token
  let e = wrapped.GetAsyncEnumerator(cts2.Token)
  e.MoveNextAsync().AsTask() |> Async2.AwaitTask |> Async2.RunSynchronously |> ignore
  e.DisposeAsync() |> ignore
  Assert.AreEqual(cts1.Token, receivedToken.Value)

[<Fact>]
let ``AsyncSeq2.withCancellation preserves sequence values`` () =
  use cts = new System.Threading.CancellationTokenSource()
  let result =
    AsyncSeq2.ofSeq [1; 2; 3; 4; 5]
    |> AsyncSeq2.withCancellation cts.Token
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 4; 5 |], result)

[<Fact>]
let ``AsyncSeq2.withCancellation with cancelled token raises OperationCanceledException`` () =
  use cts = new System.Threading.CancellationTokenSource()
  cts.Cancel()
  Assert.Catch<System.OperationCanceledException>(fun () ->
    AsyncSeq2.ofSeq [1; 2; 3]
    |> AsyncSeq2.withCancellation cts.Token
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
    |> ignore)
  |> ignore

// ===== indexed =====

[<Fact>]
let ``AsyncSeq2.indexed pairs elements with indices`` () =
  let result =
    AsyncSeq2.ofSeq [ "a"; "b"; "c" ]
    |> AsyncSeq2.indexed
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| (0, "a"); (1, "b"); (2, "c") |], result)

[<Fact>]
let ``AsyncSeq2.indexed on empty sequence returns empty`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.indexed
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.indexed index starts at zero for singleton`` () =
  let result =
    AsyncSeq2.singleton 42
    |> AsyncSeq2.indexed
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| (0, 42) |], result)

[<Fact>]
let ``AsyncSeq2.indexed produces consecutive indices`` () =
  let n = 100
  let result =
    AsyncSeq2.init n id
    |> AsyncSeq2.indexed
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  let indices = result |> Array.map fst
  Assert.AreEqual(Array.init n id, indices)

// ===== iteriAsync =====

[<Fact>]
let ``AsyncSeq2.iteriAsync calls action with correct indices and values`` () =
  let log = ResizeArray<int * int>()
  AsyncSeq2.ofSeq [ 10; 20; 30 ]
  |> AsyncSeq2.iteriAsync (fun i v -> async2 { log.Add(i, v) })
  |> Async2.RunSynchronously
  Assert.AreEqual([ (0, 10); (1, 20); (2, 30) ], log |> Seq.toList)

[<Fact>]
let ``AsyncSeq2.iteriAsync on empty sequence does not call action`` () =
  let mutable callCount = 0
  (AsyncSeq2.empty<int> ())
  |> AsyncSeq2.iteriAsync (fun _ _ -> async2 { callCount <- callCount + 1 })
  |> Async2.RunSynchronously
  Assert.AreEqual(0, callCount)

[<Fact>]
let ``AsyncSeq2.iteriAsync index is zero-based`` () =
  let indices = ResizeArray<int>()
  AsyncSeq2.ofSeq [ "x"; "y"; "z" ]
  |> AsyncSeq2.iteriAsync (fun i _ -> async2 { indices.Add(i) })
  |> Async2.RunSynchronously
  Assert.AreEqual([ 0; 1; 2 ], indices |> Seq.toList)

// ===== tryLast =====

[<Fact>]
let ``AsyncSeq2.tryLast returns Some last element for non-empty sequence`` () =
  let result =
    AsyncSeq2.ofSeq [ 1; 2; 3 ]
    |> AsyncSeq2.tryLast
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 3, result)

[<Fact>]
let ``AsyncSeq2.tryLast returns None for empty sequence`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.tryLast
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryLast returns Some for singleton sequence`` () =
  let result =
    AsyncSeq2.singleton 99
    |> AsyncSeq2.tryLast
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 99, result)

// ===== replicateUntilNoneAsync =====

[<Fact>]
let ``AsyncSeq2.replicateUntilNoneAsync generates elements until None`` () =
  let mutable counter = 0
  let gen = async2 {
    counter <- counter + 1
    if counter <= 3 then return Some counter
    else return None
  }
  let result =
    AsyncSeq2.replicateUntilNoneAsync (fun () -> gen)
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], result)

[<Fact>]
let ``AsyncSeq2.replicateUntilNoneAsync returns empty for immediate None`` () =
  let result =
    AsyncSeq2.replicateUntilNoneAsync (fun () -> async2 { return None })
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.replicateUntilNoneAsync returns single element then stops`` () =
  let mutable called = false
  let gen = async2 {
    if not called then
      called <- true
      return Some 42
    else
      return None
  }
  let result =
    AsyncSeq2.replicateUntilNoneAsync (fun () -> gen)
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 42 |], result)

// ===== reduceAsync edge case =====

[<Fact>]
let ``AsyncSeq2.reduceAsync raises ArgumentException on empty sequence`` () =
  Assert.Throws<System.ArgumentException>(fun () ->
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.reduceAsync (fun a b -> async2 { return a + b })
    |> Async2.RunSynchronously
    |> ignore)
  |> ignore
// ── Design parity with FSharp.Control.TaskSeq (issue #277, batch 2) ─────────

[<Fact>]
let ``AsyncSeq2.tryTail returns None for empty`` () =
  let result = (AsyncSeq2.empty<int> ()) |> AsyncSeq2.tryTail |> Async2.RunSynchronously
  Assert.IsTrue(result.IsNone)

[<Fact>]
let ``AsyncSeq2.tryTail returns Some tail for singleton`` () =
  let result = AsyncSeq2.ofSeq [42] |> AsyncSeq2.tryTail |> Async2.RunSynchronously
  Assert.IsTrue(result.IsSome)
  let tail = result.Value |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
  Assert.AreEqual([], tail)

[<Fact>]
let ``AsyncSeq2.tryTail returns all-but-first elements`` () =
  let result = AsyncSeq2.ofSeq [1;2;3;4;5] |> AsyncSeq2.tryTail |> Async2.RunSynchronously
  Assert.IsTrue(result.IsSome)
  let tail = result.Value |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
  Assert.AreEqual([2;3;4;5], tail)

[<Fact>]
let ``AsyncSeq2.tryTail disposes enumerator when source throws on first MoveNext`` () =
  let mutable disposed = false
  // Use a pre-failed task so the exception occurs during MoveNext() (async), not during GetEnumerator()
  let failedTask = System.Threading.Tasks.Task.FromException<unit>(System.Exception("source error"))
  let source = asyncSeq2 {
    use _ = { new System.IDisposable with member _.Dispose() = disposed <- true }
    let! _ = failedTask |> Async2.AwaitTask
    yield 1
  }
  try AsyncSeq2.tryTail source |> Async2.RunSynchronously |> ignore
  with _ -> ()
  Assert.IsTrue(disposed, "enumerator should be disposed after exception on first MoveNext")

[<Fact>]
let ``AsyncSeq2.where is alias for filter`` () =
  let result =
    AsyncSeq2.ofSeq [1..10]
    |> AsyncSeq2.where (fun x -> x % 2 = 0)
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([2;4;6;8;10], result)

[<Fact>]
let ``AsyncSeq2.whereAsync is alias for filterAsync`` () =
  let result =
    AsyncSeq2.ofSeq [1..10]
    |> AsyncSeq2.whereAsync (fun x -> async2 { return x % 2 = 0 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([2;4;6;8;10], result)

[<Fact>]
let ``AsyncSeq2.lengthBy counts matching elements`` () =
  let result =
    AsyncSeq2.ofSeq [1..10]
    |> AsyncSeq2.lengthBy (fun x -> x % 2 = 0)
    |> Async2.RunSynchronously
  Assert.AreEqual(5L, result)

[<Fact>]
let ``AsyncSeq2.lengthByAsync counts matching elements`` () =
  let result =
    AsyncSeq2.ofSeq [1..10]
    |> AsyncSeq2.lengthByAsync (fun x -> async2 { return x % 3 = 0 })
    |> Async2.RunSynchronously
  Assert.AreEqual(3L, result)

[<Fact>]
let ``AsyncSeq2.lengthBy returns 0 for empty`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.lengthBy (fun _ -> true)
    |> Async2.RunSynchronously
  Assert.AreEqual(0L, result)

[<Fact>]
let ``AsyncSeq2.compareWith equal sequences returns 0`` () =
  let s = AsyncSeq2.ofSeq [1;2;3]
  let result = AsyncSeq2.compareWith compare s (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously
  Assert.AreEqual(0, result)

[<Fact>]
let ``AsyncSeq2.compareWith shorter is less than longer`` () =
  let result = AsyncSeq2.compareWith compare (AsyncSeq2.ofSeq [1;2]) (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously
  Assert.Less(result, 0)

[<Fact>]
let ``AsyncSeq2.compareWith longer is greater than shorter`` () =
  let result = AsyncSeq2.compareWith compare (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [1;2]) |> Async2.RunSynchronously
  Assert.Greater(result, 0)

[<Fact>]
let ``AsyncSeq2.compareWith lexicographic difference`` () =
  let result = AsyncSeq2.compareWith compare (AsyncSeq2.ofSeq [1;3]) (AsyncSeq2.ofSeq [1;2]) |> Async2.RunSynchronously
  Assert.Greater(result, 0)

[<Fact>]
let ``AsyncSeq2.compareWith two empty sequences returns 0`` () =
  let result = AsyncSeq2.compareWith compare (AsyncSeq2.empty<int> ()) (AsyncSeq2.empty<int> ()) |> Async2.RunSynchronously
  Assert.AreEqual(0, result)

[<Fact>]
let ``AsyncSeq2.takeWhileInclusiveAsync includes boundary element`` () =
  let result =
    AsyncSeq2.ofSeq [1;2;3;4;5]
    |> AsyncSeq2.takeWhileInclusiveAsync (fun x -> async2 { return x < 3 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([1;2;3], result)

[<Fact>]
let ``AsyncSeq2.takeWhileInclusiveAsync empty source returns empty`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.takeWhileInclusiveAsync (fun _ -> async2 { return true })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.skipWhileInclusive skips boundary element`` () =
  let result =
    AsyncSeq2.ofSeq [1;2;3;4;5]
    |> AsyncSeq2.skipWhileInclusive (fun x -> x < 3)
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  // Skips 1, 2 (while predicate holds), skips 3 (boundary, first non-match), yields 4, 5
  Assert.AreEqual([4;5], result)

[<Fact>]
let ``AsyncSeq2.skipWhileInclusive all match returns empty`` () =
  let result =
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.skipWhileInclusive (fun _ -> true)
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.skipWhileInclusiveAsync skips boundary element`` () =
  let result =
    AsyncSeq2.ofSeq [1;2;3;4;5]
    |> AsyncSeq2.skipWhileInclusiveAsync (fun x -> async2 { return x < 3 })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([4;5], result)

[<Fact>]
let ``AsyncSeq2.appendSeq appends sync sequence after async sequence`` () =
  let result =
    AsyncSeq2.appendSeq (AsyncSeq2.ofSeq [1;2;3]) [4;5;6]
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([1;2;3;4;5;6], result)

[<Fact>]
let ``AsyncSeq2.prependSeq prepends sync sequence before async sequence`` () =
  let result =
    AsyncSeq2.ofSeq [4;5;6]
    |> AsyncSeq2.prependSeq [1;2;3]
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([1;2;3;4;5;6], result)

[<Fact>]
let ``AsyncSeq2.delay defers creation until enumeration`` () =
  let created = ref false
  let s = AsyncSeq2.delay (fun () -> created.Value <- true; AsyncSeq2.ofSeq [1;2;3])
  Assert.IsFalse(created.Value)  // not created yet
  let result = s |> AsyncSeq2.toListAsync |> Async2.RunSynchronously
  Assert.IsTrue(created.Value)
  Assert.AreEqual([1;2;3], result)

[<Fact>]
let ``AsyncSeq2.delay is re-entrant`` () =
  let count = ref 0
  let s = AsyncSeq2.delay (fun () -> incr count; AsyncSeq2.ofSeq [1;2])
  s |> AsyncSeq2.toListAsync |> Async2.RunSynchronously |> ignore
  s |> AsyncSeq2.toListAsync |> Async2.RunSynchronously |> ignore
  Assert.AreEqual(2, count.Value)

[<Fact>]
let ``AsyncSeq2.collectAsync flattens async-bound inner sequences`` () =
  let result =
    AsyncSeq2.ofSeq [1;2;3]
    |> AsyncSeq2.collectAsync (fun x -> async2 { return AsyncSeq2.ofSeq [x; x*10] })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([1;10;2;20;3;30], result)

[<Fact>]
let ``AsyncSeq2.collectAsync on empty returns empty`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.collectAsync (fun x -> async2 { return AsyncSeq2.singleton x })
    |> AsyncSeq2.toListAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([], result)

[<Fact>]
let ``AsyncSeq2.partition splits by predicate`` () =
  let trues, falses =
    AsyncSeq2.ofSeq [1..10]
    |> AsyncSeq2.partition (fun x -> x % 2 = 0)
    |> Async2.RunSynchronously
  Assert.AreEqual([|2;4;6;8;10|], trues)
  Assert.AreEqual([|1;3;5;7;9|], falses)

[<Fact>]
let ``AsyncSeq2.partition empty returns two empty arrays`` () =
  let trues, falses =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.partition (fun _ -> true)
    |> Async2.RunSynchronously
  Assert.AreEqual([||], trues)
  Assert.AreEqual([||], falses)

[<Fact>]
let ``AsyncSeq2.partitionAsync splits by async predicate`` () =
  let trues, falses =
    AsyncSeq2.ofSeq [1..6]
    |> AsyncSeq2.partitionAsync (fun x -> async2 { return x % 2 = 0 })
    |> Async2.RunSynchronously
  Assert.AreEqual([|2;4;6|], trues)
  Assert.AreEqual([|1;3;5|], falses)

// ===== mapiAsync =====

[<Fact>]
let ``AsyncSeq2.mapiAsync maps elements with their int64 index`` () =
  let result =
    AsyncSeq2.ofSeq ["a"; "b"; "c"]
    |> AsyncSeq2.mapiAsync (fun i x -> async2 { return sprintf "%d:%s" i x })
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| "0:a"; "1:b"; "2:c" |], result)

[<Fact>]
let ``AsyncSeq2.mapiAsync on empty sequence returns empty`` () =
  let result =
    (AsyncSeq2.empty<string> ())
    |> AsyncSeq2.mapiAsync (fun i x -> async2 { return sprintf "%d:%s" i x })
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.mapiAsync index is int64 and starts at zero`` () =
  let indices = ResizeArray<int64>()
  AsyncSeq2.ofSeq [10; 20; 30]
  |> AsyncSeq2.mapiAsync (fun i x -> async2 { indices.Add(i); return x })
  |> AsyncSeq2.toArrayAsync
  |> Async2.RunSynchronously
  |> ignore
  Assert.AreEqual([| 0L; 1L; 2L |], indices.ToArray())

[<Fact>]
let ``AsyncSeq2.mapiAsync matches mapi for pure function`` () =
  for n in 0..20 do
    let ls = List.init n (fun x -> x + 1)
    let expected = ls |> List.mapi (fun i x -> i * x) |> List.toArray
    let actual =
      AsyncSeq2.ofSeq ls
      |> AsyncSeq2.mapiAsync (fun i x -> async2 { return int i * x })
      |> AsyncSeq2.toArrayAsync
      |> Async2.RunSynchronously
    Assert.AreEqual(expected, actual)

// ===== tryPickAsync / pickAsync =====

[<Fact>]
let ``AsyncSeq2.tryPickAsync returns Some for first matching element`` () =
  let result =
    AsyncSeq2.ofSeq [1; 2; 3; 4; 5]
    |> AsyncSeq2.tryPickAsync (fun x -> async2 { return if x > 3 then Some (x * 10) else None })
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 40, result)

[<Fact>]
let ``AsyncSeq2.tryPickAsync returns None when no element matches`` () =
  let result =
    AsyncSeq2.ofSeq [1; 2; 3]
    |> AsyncSeq2.tryPickAsync (fun x -> async2 { return if x > 99 then Some x else None })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryPickAsync returns None for empty sequence`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.tryPickAsync (fun x -> async2 { return Some x })
    |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.tryPickAsync returns first match not last`` () =
  let result =
    AsyncSeq2.ofSeq [10; 20; 30]
    |> AsyncSeq2.tryPickAsync (fun x -> async2 { return if x % 10 = 0 then Some x else None })
    |> Async2.RunSynchronously
  Assert.AreEqual(Some 10, result)

[<Fact>]
let ``AsyncSeq2.pickAsync returns value for first matching element`` () =
  let result =
    AsyncSeq2.ofSeq [1; 2; 3; 4; 5]
    |> AsyncSeq2.pickAsync (fun x -> async2 { return if x = 3 then Some "three" else None })
    |> Async2.RunSynchronously
  Assert.AreEqual("three", result)

[<Fact>]
let ``AsyncSeq2.pickAsync raises KeyNotFoundException when no match`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    AsyncSeq2.ofSeq [1; 2; 3]
    |> AsyncSeq2.pickAsync (fun x -> async2 { return if x > 99 then Some x else None })
    |> Async2.RunSynchronously
    |> ignore) |> ignore

[<Fact>]
let ``AsyncSeq2.pickAsync raises KeyNotFoundException for empty sequence`` () =
  Assert.Throws<System.Collections.Generic.KeyNotFoundException>(fun () ->
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.pickAsync (fun x -> async2 { return Some x })
    |> Async2.RunSynchronously
    |> ignore) |> ignore

// ===== groupByAsync =====

[<Fact>]
let ``AsyncSeq2.groupByAsync groups elements by async projection`` () =
  let result =
    AsyncSeq2.ofSeq [1..6]
    |> AsyncSeq2.groupByAsync (fun x -> async2 { return x % 2 })
    |> Async2.RunSynchronously
    |> Array.map (fun (key, items) -> key, Array.sort items)
    |> Array.sortBy fst
  Assert.AreEqual([| (0, [|2;4;6|]); (1, [|1;3;5|]) |], result)

[<Fact>]
let ``AsyncSeq2.groupByAsync on empty sequence returns empty`` () =
  let result =
    (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.groupByAsync (fun x -> async2 { return x % 2 })
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.groupByAsync with all-same key produces single group`` () =
  let result =
    AsyncSeq2.ofSeq [1; 2; 3]
    |> AsyncSeq2.groupByAsync (fun _ -> async2 { return "same" })
    |> Async2.RunSynchronously
  Assert.AreEqual([| ("same", [|1;2;3|]) |], result)

// ===== ofList =====

[<Fact>]
let ``AsyncSeq2.ofList returns elements in order`` () =
  let result = AsyncSeq2.ofList [1; 2; 3; 4; 5] |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 4; 5 |], result)

[<Fact>]
let ``AsyncSeq2.ofList on empty list returns empty`` () =
  let result = AsyncSeq2.ofList ([] : int list) |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.ofList produces same result as ofSeq`` () =
  let xs = [10; 20; 30]
  let fromList = AsyncSeq2.ofList xs |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  let fromSeq  = AsyncSeq2.ofSeq xs  |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual(fromSeq, fromList)

// ===== ofArray =====

[<Fact>]
let ``AsyncSeq2.ofArray returns elements in order`` () =
  let result = AsyncSeq2.ofArray [| 10; 20; 30 |] |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 10; 20; 30 |], result)

[<Fact>]
let ``AsyncSeq2.ofArray on empty array returns empty`` () =
  let result = AsyncSeq2.ofArray ([||] : int []) |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.ofArray produces same result as ofSeq`` () =
  let xs = [| 1; 2; 3; 4 |]
  let fromArray = AsyncSeq2.ofArray xs |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  let fromSeq   = AsyncSeq2.ofSeq   xs |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual(fromSeq, fromArray)

// ===== cycle =====

[<Fact>]
let ``AsyncSeq2.cycle repeats elements indefinitely`` () =
  let result =
    AsyncSeq2.cycle (AsyncSeq2.ofList [1; 2; 3])
    |> AsyncSeq2.take 7
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3; 1; 2; 3; 1 |], result)

[<Fact>]
let ``AsyncSeq2.cycle on empty sequence returns empty`` () =
  let result =
    AsyncSeq2.cycle (AsyncSeq2.empty<int> ())
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.cycle on singleton repeats single element`` () =
  let result =
    AsyncSeq2.cycle (AsyncSeq2.singleton 42)
    |> AsyncSeq2.take 5
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 42; 42; 42; 42; 42 |], result)

// ===== ofSeq: re-enumeration and empty-sequence edge cases =====

[<Fact>]
let ``AsyncSeq2.ofSeq empty returns empty`` () =
  let result = AsyncSeq2.ofSeq Seq.empty<int> |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

[<Fact>]
let ``AsyncSeq2.ofSeq can be enumerated multiple times`` () =
  let s = AsyncSeq2.ofSeq [1; 2; 3]
  let r1 = s |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  let r2 = s |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 1; 2; 3 |], r1)
  Assert.AreEqual([| 1; 2; 3 |], r2)

// ===== tryFinally: compensation runs even when downstream stops early =====

[<Fact>]
let ``asyncSeq2 use releases resource on early termination`` () =
  let disposed = ref false
  let resource = { new System.IDisposable with member _.Dispose() = disposed := true }
  let s = asyncSeq2 {
    use _r = resource
    yield 1; yield 2; yield 3 }
  s |> AsyncSeq2.take 1 |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously |> ignore
  Assert.IsTrue(disposed.Value)

// ===== tryWith: handler receives exception and yields elements =====

[<Fact>]
let ``asyncSeq2 try-with handler can yield elements`` () =
  let s = asyncSeq2 {
    try failwith "boom"
    with _ -> yield 42 }
  let result = s |> AsyncSeq2.toArrayAsync |> Async2.RunSynchronously
  Assert.AreEqual([| 42 |], result)

// ===== exists2 / exists2Async =====

[<Fact>]
let ``AsyncSeq2.exists2 returns true when a matching pair exists`` () =
  let result =
    AsyncSeq2.exists2 (=) (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [0;2;0])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

[<Fact>]
let ``AsyncSeq2.exists2 returns false when no matching pair exists`` () =
  let result =
    AsyncSeq2.exists2 (=) (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [4;5;6])
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.exists2 on empty sequences returns false`` () =
  let result =
    AsyncSeq2.exists2 (=) (AsyncSeq2.empty<int> ()) (AsyncSeq2.empty<int> ())
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.exists2 short-circuits on first match`` () =
  let count = ref 0
  let result =
    AsyncSeq2.exists2
      (fun a b -> incr count; a = b)
      (AsyncSeq2.ofSeq [1;2;3;4;5])
      (AsyncSeq2.ofSeq [0;2;0;0;0])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)
  Assert.AreEqual(2, count.Value)  // stopped after second pair

[<Fact>]
let ``AsyncSeq2.exists2 stops at shorter sequence`` () =
  let result =
    AsyncSeq2.exists2 (=) (AsyncSeq2.ofSeq [1;2]) (AsyncSeq2.ofSeq [3;4;1])
    |> Async2.RunSynchronously
  Assert.IsFalse(result)  // shorter seq ends before (1,1) can match

[<Fact>]
let ``AsyncSeq2.exists2Async returns true with async predicate`` () =
  let result =
    AsyncSeq2.exists2Async
      (fun a b -> async2 { return a = b })
      (AsyncSeq2.ofSeq [1;2;3])
      (AsyncSeq2.ofSeq [0;2;0])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

// ===== forall2 / forall2Async =====

[<Fact>]
let ``AsyncSeq2.forall2 returns true when all pairs satisfy the predicate`` () =
  let result =
    AsyncSeq2.forall2 (=) (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [1;2;3])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

[<Fact>]
let ``AsyncSeq2.forall2 returns false when a pair fails`` () =
  let result =
    AsyncSeq2.forall2 (=) (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [1;9;3])
    |> Async2.RunSynchronously
  Assert.IsFalse(result)

[<Fact>]
let ``AsyncSeq2.forall2 on empty sequences returns true`` () =
  let result =
    AsyncSeq2.forall2 (=) (AsyncSeq2.empty<int> ()) (AsyncSeq2.empty<int> ())
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

[<Fact>]
let ``AsyncSeq2.forall2 short-circuits on first failure`` () =
  let count = ref 0
  let result =
    AsyncSeq2.forall2
      (fun a b -> incr count; a = b)
      (AsyncSeq2.ofSeq [1;9;3;4;5])
      (AsyncSeq2.ofSeq [1;2;3;4;5])
    |> Async2.RunSynchronously
  Assert.IsFalse(result)
  Assert.AreEqual(2, count.Value)  // stopped after second pair

[<Fact>]
let ``AsyncSeq2.forall2 stops at shorter sequence`` () =
  let result =
    AsyncSeq2.forall2 (=) (AsyncSeq2.ofSeq [1;2]) (AsyncSeq2.ofSeq [1;2;99])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)  // stops when shorter seq ends; all checked pairs passed

[<Fact>]
let ``AsyncSeq2.forall2Async returns true with async predicate`` () =
  let result =
    AsyncSeq2.forall2Async
      (fun a b -> async2 { return a = b })
      (AsyncSeq2.ofSeq [1;2;3])
      (AsyncSeq2.ofSeq [1;2;3])
    |> Async2.RunSynchronously
  Assert.IsTrue(result)

// ===== tryFirst / firstOrDefault =====

[<Fact>]
let ``AsyncSeq2.tryFirst returns Some first element for non-empty sequence`` () =
  let result = AsyncSeq2.tryFirst (AsyncSeq2.ofSeq [1;2;3]) |> Async2.RunSynchronously
  Assert.AreEqual(Some 1, result)

[<Fact>]
let ``AsyncSeq2.tryFirst returns None for empty sequence`` () =
  let result = AsyncSeq2.tryFirst ((AsyncSeq2.empty<int> ())) |> Async2.RunSynchronously
  Assert.AreEqual(None, result)

[<Fact>]
let ``AsyncSeq2.firstOrDefault returns first element when non-empty`` () =
  let result = AsyncSeq2.firstOrDefault -1 (AsyncSeq2.ofSeq [5;6;7]) |> Async2.RunSynchronously
  Assert.AreEqual(5, result)

[<Fact>]
let ``AsyncSeq2.firstOrDefault returns default when empty`` () =
  let result = AsyncSeq2.firstOrDefault -1 ((AsyncSeq2.empty<int> ())) |> Async2.RunSynchronously
  Assert.AreEqual(-1, result)

// ===== zipWithParallel =====

[<Fact>]
let ``AsyncSeq2.zipWithParallel combines values from both sequences`` () =
  let result =
    AsyncSeq2.zipWithParallel (fun a b -> a + b) (AsyncSeq2.ofSeq [1;2;3]) (AsyncSeq2.ofSeq [10;20;30])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 11;22;33 |], result)

[<Fact>]
let ``AsyncSeq2.zipWithParallel stops at shorter sequence`` () =
  let result =
    AsyncSeq2.zipWithParallel (fun a b -> a, b) (AsyncSeq2.ofSeq [1;2]) (AsyncSeq2.ofSeq ["a";"b";"c"])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| (1,"a"); (2,"b") |], result)

// ===== combineLatestWithAsync =====

[<Fact>]
let ``AsyncSeq2.combineLatestWithAsync combines initial values then emits on each new update`` () =
  let firstCombined = System.Threading.Tasks.TaskCompletionSource<unit>(
    System.Threading.Tasks.TaskCreationOptions.RunContinuationsAsynchronously)
  let first = asyncSeq2 {
    yield 1
    do! firstCombined.Task
    yield 2
  }
  let result =
    AsyncSeq2.combineLatestWithAsync
      (fun a b -> async2 {
        firstCombined.TrySetResult(()) |> ignore
        return a + b
      })
      first
      (AsyncSeq2.ofSeq [10])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([| 11; 12 |], result)

[<Fact>]
let ``AsyncSeq2.combineLatestWithAsync produces empty sequence when either source is empty`` () =
  let result =
    AsyncSeq2.combineLatestWithAsync
      (fun a b -> async2 { return a + b })
      ((AsyncSeq2.empty<int> ()))
      (AsyncSeq2.ofSeq [1;2;3])
    |> AsyncSeq2.toArrayAsync
    |> Async2.RunSynchronously
  Assert.AreEqual([||], result)

// ===== toObservable =====

type private TestObserver<'T>(onNext, onCompleted) =
  interface IObserver<'T> with
    member _.OnNext(v) = onNext v
    member _.OnCompleted() = onCompleted ()
    member _.OnError(_e) = ()

[<Fact>]
let ``AsyncSeq2.toObservable emits all values then completes`` () =
  let received = ResizeArray<int>()
  let completedEvent = new System.Threading.ManualResetEventSlim(false)
  let observer = TestObserver<int>(received.Add, completedEvent.Set)
  use _sub = (AsyncSeq2.toObservable (AsyncSeq2.ofSeq [1;2;3])).Subscribe(observer)
  Assert.IsTrue(completedEvent.Wait(2000))
  Assert.AreEqual([| 1;2;3 |], received.ToArray())

[<Fact>]
let ``AsyncSeq2.toObservable on empty sequence emits nothing`` () =
  let received = ResizeArray<int>()
  let completedEvent = new System.Threading.ManualResetEventSlim(false)
  let observer = TestObserver<int>(received.Add, completedEvent.Set)
  use _sub = (AsyncSeq2.toObservable ((AsyncSeq2.empty<int> ()))).Subscribe(observer)
  Assert.IsTrue(completedEvent.Wait(2000))
  Assert.AreEqual([||], received.ToArray())
