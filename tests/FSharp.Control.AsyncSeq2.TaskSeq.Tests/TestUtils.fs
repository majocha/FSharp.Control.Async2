namespace AsyncSeq2.Tests

open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics
open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation
open Xunit.Abstractions

module ColdTask =
    let toArrayAsync source = AsyncSeq2.toArrayAsync source |> Async2.StartAsTask
    let toListAsync source = AsyncSeq2.toListAsync source |> Async2.StartAsTask
    let toResizeArrayAsync source = AsyncSeq2.toResizeArrayAsync source |> Async2.StartAsTask
    let toIListAsync source = AsyncSeq2.toIListAsync source |> Async2.StartAsTask
    let toChannelAsync writer source = AsyncSeq2.toChannelAsync writer source |> Async2.StartAsTask
    let iteri action source = AsyncSeq2.iteri action source |> Async2.StartAsTask
    let iteriAsync (action: int -> 'T -> Task<unit>) source =
        AsyncSeq2.iteriAsync (fun index value -> Async2.Await(action index value)) source |> Async2.StartAsTask
    let inline max source = AsyncSeq2.max source |> Async2.StartAsTask
    let inline min source = AsyncSeq2.min source |> Async2.StartAsTask
    let inline tryMax source = AsyncSeq2.tryMax source |> Async2.StartAsTask
    let inline tryMin source = AsyncSeq2.tryMin source |> Async2.StartAsTask
    let inline maxBy projection source = AsyncSeq2.maxBy projection source |> Async2.StartAsTask
    let inline minBy projection source = AsyncSeq2.minBy projection source |> Async2.StartAsTask
    let inline maxByAsync (projection: 'T -> Task<'U>) source =
        AsyncSeq2.maxByAsync (projection >> Async2.Await) source |> Async2.StartAsTask
    let inline minByAsync (projection: 'T -> Task<'U>) source =
        AsyncSeq2.minByAsync (projection >> Async2.Await) source |> Async2.StartAsTask
    let lengthOrMax count source = AsyncSeq2.lengthOrMax count source |> Async2.StartAsTask
    let lengthBy predicate source = AsyncSeq2.lengthBy predicate source |> Async2.StartAsTask
    let lengthByAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.lengthByAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let firstOrDefault value source = AsyncSeq2.firstOrDefault value source |> Async2.StartAsTask
    let lastOrDefault value source = AsyncSeq2.lastOrDefault value source |> Async2.StartAsTask
    let splitAt count source = AsyncSeq2.splitAt count source |> Async2.StartAsTask
    let tryHead source = AsyncSeq2.tryHead source |> Async2.StartAsTask
    let head source = AsyncSeq2.head source |> Async2.StartAsTask
    let tryLast source = AsyncSeq2.tryLast source |> Async2.StartAsTask
    let last source = AsyncSeq2.last source |> Async2.StartAsTask
    let tryTail source = AsyncSeq2.tryTail source |> Async2.StartAsTask
    let tail source = AsyncSeq2.tail source |> Async2.StartAsTask
    let tryItem index source = AsyncSeq2.tryItem index source |> Async2.StartAsTask
    let item index source = AsyncSeq2.item index source |> Async2.StartAsTask
    let tryExactlyOne source = AsyncSeq2.tryExactlyOne source |> Async2.StartAsTask
    let exactlyOne source = AsyncSeq2.exactlyOne source |> Async2.StartAsTask
    let forall predicate source = AsyncSeq2.forall predicate source |> Async2.StartAsTask
    let forallAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.forallAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let exists predicate source = AsyncSeq2.exists predicate source |> Async2.StartAsTask
    let existsAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.existsAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let contains item source = AsyncSeq2.contains item source |> Async2.StartAsTask
    let tryPick chooser source = AsyncSeq2.tryPick chooser source |> Async2.StartAsTask
    let tryPickAsync (chooser: 'T -> Task<'U option>) source =
        AsyncSeq2.tryPickAsync (chooser >> Async2.Await) source |> Async2.StartAsTask
    let pick chooser source = AsyncSeq2.pick chooser source |> Async2.StartAsTask
    let pickAsync (chooser: 'T -> Task<'U option>) source =
        AsyncSeq2.pickAsync (chooser >> Async2.Await) source |> Async2.StartAsTask
    let tryFind predicate source = AsyncSeq2.tryFind predicate source |> Async2.StartAsTask
    let tryFindAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.tryFindAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let find predicate source = AsyncSeq2.find predicate source |> Async2.StartAsTask
    let findAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.findAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let tryFindIndex predicate source = AsyncSeq2.tryFindIndex predicate source |> Async2.StartAsTask
    let tryFindIndexAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.tryFindIndexAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let findIndex predicate source = AsyncSeq2.findIndex predicate source |> Async2.StartAsTask
    let findIndexAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.findIndexAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let compareWith comparer first second = AsyncSeq2.compareWith comparer first second |> Async2.StartAsTask
    let compareWithAsync (comparer: 'T -> 'T -> Task<int>) first second =
        AsyncSeq2.compareWithAsync (fun a b -> Async2.Await(comparer a b)) first second |> Async2.StartAsTask
    let fold folder state source = AsyncSeq2.fold folder state source |> Async2.StartAsTask
    let foldAsync (folder: 'State -> 'T -> Task<'State>) state source =
        AsyncSeq2.foldAsync (fun acc value -> Async2.Await(folder acc value)) state source |> Async2.StartAsTask
    let foldWhile predicate folder state source = AsyncSeq2.foldWhile predicate folder state source |> Async2.StartAsTask
    let foldWhileAsync (predicate: 'State -> 'T -> Task<bool>) (folder: 'State -> 'T -> Task<'State>) state source =
        AsyncSeq2.foldWhileAsync (fun acc value -> Async2.Await(predicate acc value)) (fun acc value -> Async2.Await(folder acc value)) state source |> Async2.StartAsTask
    let reduce folder source = AsyncSeq2.reduce folder source |> Async2.StartAsTask
    let reduceAsync (folder: 'T -> 'T -> Task<'T>) source =
        AsyncSeq2.reduceAsync (fun a b -> Async2.Await(folder a b)) source |> Async2.StartAsTask
    let groupBy projection source = AsyncSeq2.groupBy projection source |> Async2.StartAsTask
    let groupByAsync (projection: 'T -> Task<'Key>) source =
        AsyncSeq2.groupByAsync (projection >> Async2.Await) source |> Async2.StartAsTask
    let countBy projection source = AsyncSeq2.countBy projection source |> Async2.StartAsTask
    let countByAsync (projection: 'T -> Task<'Key>) source =
        AsyncSeq2.countByAsync (projection >> Async2.Await) source |> Async2.StartAsTask
    let partition predicate source = AsyncSeq2.partition predicate source |> Async2.StartAsTask
    let partitionAsync (predicate: 'T -> Task<bool>) source =
        AsyncSeq2.partitionAsync (predicate >> Async2.Await) source |> Async2.StartAsTask
    let mapFold mapping state source = AsyncSeq2.mapFold mapping state source |> Async2.StartAsTask
    let mapFoldAsync (mapping: 'State -> 'T -> Task<'U * 'State>) state source =
        AsyncSeq2.mapFoldAsync (fun acc value -> Async2.Await(mapping acc value)) state source |> Async2.StartAsTask

type AsyncEnumStatus = BeforeAll | WithCurrent | AfterAll

/// Milliseconds
[<Measure>]
type ms

/// Microseconds
[<Measure>]
type µs

/// Helpers for short waits, as Task.Delay has about 15ms precision.
/// Inspired by IoT code: https://github.com/dotnet/iot/pull/235/files
module DelayHelper =

    let private rnd = Random()

    /// <summary>
    /// Delay for at least the specified <paramref name="microseconds"/>.
    /// </summary>
    /// <param name="microseconds">The number of microseconds to delay.</param>
    /// <param name="allowThreadYield">
    /// True to allow yielding the thread. If this is set to false, on single-proc systems
    /// this will prevent all other code from running.
    /// </param>
    let spinWaitDelay (microseconds: int64<µs>) (allowThreadYield: bool) =
        let start = Stopwatch.GetTimestamp()
        let minimumTicks = int64 microseconds * Stopwatch.Frequency / 1_000_000L

        // FIXME: though this is part of official IoT code, the `allowThreadYield` version is extremely slow
        // slower than would be expected from a simple SpinOnce. Though this may be caused by scenarios with
        // many tasks at once. Have to investigate. See perf smoke tests.
        if allowThreadYield then
            let spinWait = SpinWait()

            while Stopwatch.GetTimestamp() - start < minimumTicks do
                spinWait.SpinOnce(1)

        else
            while Stopwatch.GetTimestamp() - start < minimumTicks do
                Thread.SpinWait(1)

    let delayTask (µsecMin: int64<µs>) (µsecMax: int64<µs>) f = task {
        let rnd () = rnd.NextInt64(int64 µsecMin, int64 µsecMax) * 1L<µs>

        // ensure unequal running lengths and points-in-time for assigning the variable
        // DO NOT use Thead.Sleep(), it's blocking!
        // WARNING: Task.Delay only has a 15ms timer resolution!!!

        // TODO: check this! The following comment may not be correct
        // this creates a resume state, which seems more efficient than SpinWait.SpinOnce, see DelayHelper.
        let! _ = Task.Delay 0
        let delay = rnd ()

        // typical minimum accuracy of Task.Delay is 15.6ms
        // for delay-cases shorter than that, we use SpinWait
        if delay < 15_000L<µs> then
            do spinWaitDelay (rnd ()) false
        else
            do! Task.Delay(int <| float delay / 1_000.0)

        return f ()
    }

/// <summary>
/// Creates dummy backgroundTasks with a randomized delay and a mutable state,
/// to ensure we properly test whether processing is done ordered or not.
/// Default for <paramref name="µsecMin" /> and <paramref name="µsecMax" />
/// are 10,000µs and 30,000µs respectively (or 10ms and 30ms).
/// </summary>
/// <param name="µsecMin">Minimum delay in µs</param>
/// <param name="µsecMax">Maximum delay in µs</param>
type DummyTaskFactory(µsecMin: int64<µs>, µsecMax: int64<µs>) =
    let mutable x = 0

    let runTaskDelayed () = backgroundTask { return! DelayHelper.delayTask µsecMin µsecMax (fun _ -> Interlocked.Increment &x) }

    let runTaskDelayedImmutable i = backgroundTask { return! DelayHelper.delayTask µsecMin µsecMax (fun _ -> i + 1) }

    let runTaskDirect () = backgroundTask {
        Interlocked.Increment &x |> ignore
        return x
    }


    /// <summary>
    /// Creates dummy tasks with a randomized delay and a mutable state,
    /// to ensure we properly test whether processing is done ordered or not.
    /// Uses the defaults for <paramref name="µsecMin" /> and <paramref name="µsecMax" />
    /// with 1,000µs and 5,000µs respectively (or 1ms and 5ms).
    /// </summary>
    new() = DummyTaskFactory(1_000L<µs>, 5_000L<µs>)

    /// <summary>
    /// Creates dummy tasks with a randomized delay and a mutable state,
    /// to ensure we properly test whether processing is done ordered or not.
    /// Values <paramref name="msecMin" /> and <paramref name="msecMax" /> can be
    /// given in milliseconds.
    /// </summary>
    /// <param name="msecMin">Minimum delay in ms</param>
    /// <param name="msecMax">Maximum delay in ms</param>
    new(msecMin: int<ms>, msecMax: int<ms>) = new DummyTaskFactory(int64 msecMin * 1000L<µs>, int64 msecMax * 1000L<µs>)


    /// Bunch of delayed tasks that randomly have a yielding delay of 10-30ms, therefore having overlapping execution times.
    member _.CreateDelayedTasks_SideEffect total = [
        for _ in 0 .. total - 1 do
            fun () -> runTaskDelayed ()
    ]

    /// Bunch of delayed tasks that randomly have a yielding delay of 10-30ms, therefore having overlapping execution times.
    member _.CreateDelayedTasks_Immutable total = [
        for i in 0 .. total - 1 do
            fun () -> runTaskDelayedImmutable i
    ]

    /// Bunch of delayed tasks without internally using Task.Delay, therefore hot-started and immediately finished.
    member _.CreateDirectTasks_SideEffect total = [
        for _ in 0 .. total - 1 do
            fun () -> runTaskDirect ()
    ]

[<AutoOpen>]
module TestUtils =
    /// Verifies that a task sequence is empty by converting to an array and checking emptiness.
    let verifyEmpty ts =
        ts
        |> ColdTask.toArrayAsync
        |> Task.map (Array.isEmpty >> should be True)

    /// Verifies that a task sequence contains exactly one item
    let verifySingleton value ts =
        ts
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| value |])

    /// Verifies that a task sequence contains integers 1-10, by converting to an array and comparing.
    let verify1To10 ts =
        ts
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 1..10 |])

    /// Verifies that a task sequence contains integers 1-10, by converting to an array and comparing.
    let verify0To9 ts =
        ts
        |> ColdTask.toArrayAsync
        |> Task.map (should equal [| 0..9 |])

    /// Turns a sequence of integers into a string, starting with A for '1', Z for 26 etc.
    let verifyDigitsAsString expected =
        AsyncSeq2.map (char: int -> char)
        >> AsyncSeq2.map ((+) '@') // turns int(1) into char('A') etc
        >> ColdTask.toArrayAsync
        >> Task.map (String >> should equal expected)

    /// Waits using a real timer-based async delay (not spin-wait), causing an OS-level async yield point.
    /// On Windows, Task.Delay has ~15ms timer resolution, so this actually waits ~15ms.
    let longDelay () = task { do! Task.Delay 1 }

    /// Spin-waits, occasionally normal delay, between 50µs - 18,000µs
    let microDelay () = task { do! DelayHelper.delayTask 50L<µs> 18_000L<µs> (fun _ -> ()) }

    /// Consumes and returns a Task (not a Task<unit>!!!)
    let consumeTaskSeq ts = AsyncSeq2.iter ignore ts |> Async2.StartAsTask |> Task.ignore

    module Assert =
        /// Call MoveNextAsync() and check if return value is the expected value
        let moveNextAndCheck expected (enumerator: IAsyncEnumerator<_>) = task {
            let! (hasNext: bool) = enumerator.MoveNextAsync()

            if expected then
                hasNext |> should be True
            else
                hasNext |> should be False
        }

        /// Call MoveNextAsync() and check if Current has the expected value. Uses untyped 'should equal'
        let moveNextAndCheckCurrent successMoveNext expectedValue (enumerator: IAsyncEnumerator<_>) = task {
            let! (hasNext: bool) = enumerator.MoveNextAsync()

            if successMoveNext then
                hasNext |> should be True
            else
                hasNext |> should be False

            enumerator.Current |> should equal expectedValue
        }

        /// Call MoveNext() and check if Current has the expected value. Uses untyped 'should equal'
        let seqMoveNextAndCheckCurrent successMoveNext expectedValue (enumerator: IEnumerator<_>) =
            let (hasNext: bool) = enumerator.MoveNext()

            if successMoveNext then
                hasNext |> should be True
            else
                hasNext |> should be False

            enumerator.Current |> should equal expectedValue

    // TODO: figure out a way to make IXunitSerializable work with DU types
    type CustomSerializable<'T>(value: 'T) =
        let mutable _value: 'T = value

        new() = CustomSerializable(Unchecked.defaultof<'T>)

        member this.Value = _value

        interface IXunitSerializable with
            member this.Deserialize info = _value <- info.GetValue<'T>("Value")

            member this.Serialize info = info.AddValue("Value", _value)

        override this.ToString() = "Value = " + string _value

    // NOTE: using enum instead of DU because *even if* we use CustomSerializable above, the
    // VS test runner will hang, and NCrunch will (properly) show that the type does not implement
    // a default constructor. See https://github.com/xunit/xunit/issues/429, amongst others.
    type EmptyVariant =
        | CallEmpty = 10
        | Do = 11
        | DoBang = 12
        | YieldBang = 13
        | YieldBangNested = 14
        | DelayDoBang = 16
        | DelayYieldBang = 17
        | DelayYieldBangNested = 18

    type SeqImmutable =
        | Sequential_YieldBang = 100
        | Sequential_Yield = 101
        | Sequential_For = 102
        | Sequential_Combine = 103
        | Sequential_Zero = 104
        | ThreadSpinWait = 105
        | AsyncYielded = 106
        | AsyncYielded_Nested = 107

    type SeqWithSideEffect =
        | Sequential_YieldBang = 1000
        | Sequential_Yield = 1001
        | Sequential_For = 1002
        | Sequential_Combine = 1003
        | Sequential_Zero = 1004
        | ThreadSpinWait = 1005
        | AsyncYielded = 1006
        | AsyncYielded_Nested = 1007

    /// Several task generators, with artificial delays,
    /// mostly to ensure sequential async operation of side effects.
    module Gen =
        /// Joins two tasks using merely BCL methods. This approach is what you can use to
        /// properly, sequentially execute a chain of tasks in a non-blocking, non-overlapping way.
        let joinWithContinuation tasks =
            let simple (t: unit -> Task<_>) (source: unit -> Task<_>) : unit -> Task<_> =
                fun () -> source().ContinueWith((fun (_: Task) -> t ()), TaskContinuationOptions.OnlyOnRanToCompletion).Unwrap() :?> Task<_>

            let rec combine acc (tasks: (unit -> Task<_>) list) =
                match tasks with
                | [] -> acc
                | t :: tail -> combine (simple t acc) tail

            match tasks with
            | first :: rest -> combine first rest
            | [] -> failwith "oh oh, no tasks given!"

        let joinIdentityHotStarted tasks () = task { return tasks |> List.map (fun t -> t ()) }

        let joinIdentityDelayed tasks () = task { return tasks }

        let createAndJoinMultipleTasks total joiner : Task<_> =
            // the actual creation of tasks
            let tasks = DummyTaskFactory().CreateDelayedTasks_SideEffect total
            let combinedTask = joiner tasks
            // start the combined tasks
            combinedTask ()

        /// Create a bunch of dummy tasks, with varying microsecond delays.
        let sideEffectTaskSeqMicro (min: int64<µs>) max count =
            /// Set of delayed tasks in the form of `unit -> Task<int>`
            let tasks = DummyTaskFactory(min, max).CreateDelayedTasks_SideEffect count

            asyncSeq2 {
                for task in tasks do
                    // cannot use `yield!` here, as `asyncSeq2` expects it to return a seq
                    let! x = task ()
                    yield x
            }

        /// Create a bunch of dummy tasks, with varying millisecond delays.
        let sideEffectTaskSeqMs (min: int<ms>) max count =
            /// Set of delayed tasks in the form of `unit -> Task<int>`
            let tasks = DummyTaskFactory(min, max).CreateDelayedTasks_SideEffect count

            asyncSeq2 {
                for task in tasks do
                    // cannot use `yield!` here, as `asyncSeq2` expects it to return a seq
                    let! x = task ()
                    yield x
            }

        /// Create a bunch of dummy tasks, which are sequentially hot-started, WITHOUT artificial spin-wait delays.
        let sideEffectTaskSeq_Sequential count =
            // Set of non-delayed tasks in the form of `unit -> Task<int>`
            let tasks = DummyTaskFactory().CreateDirectTasks_SideEffect count

            asyncSeq2 {
                for task in tasks do
                    // cannot use `yield!` here, as `asyncSeq2` expects it to return a seq
                    let! x = task ()
                    yield x
            }

        /// Create a bunch of dummy tasks, each lasting between 1-5ms with spin-wait delays.
        let sideEffectTaskSeq = sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs>

        /// Returns any of a set of variants that each create an empty sequence in a creative way.
        /// Please extend this with more cases.
        let getEmptyVariant variant : IAsyncEnumerable<int> =
            match variant with
            | EmptyVariant.CallEmpty -> (AsyncSeq2.empty ())
            | EmptyVariant.Do -> asyncSeq2 { do ignore () }
            | EmptyVariant.DoBang -> asyncSeq2 { do! task { return () } }
            | EmptyVariant.YieldBang -> asyncSeq2 { yield! Seq.empty<int> }
            | EmptyVariant.YieldBangNested -> asyncSeq2 { yield! asyncSeq2 { do ignore () } }
            | EmptyVariant.DelayDoBang -> asyncSeq2 {
                do! microDelay ()
                do! microDelay ()
                do! longDelay ()
              }
            | EmptyVariant.DelayYieldBang -> asyncSeq2 {
                do! microDelay ()
                yield! Seq.empty<int>
                do! longDelay ()
                yield! Seq.empty<int>
                do! microDelay ()
              }

            | EmptyVariant.DelayYieldBangNested -> asyncSeq2 {
                yield! asyncSeq2 {
                    do! microDelay ()
                    yield! asyncSeq2 { do! microDelay () }
                    do! longDelay ()
                }

                yield! (AsyncSeq2.empty ())

                yield! asyncSeq2 {
                    do! microDelay ()
                    yield! asyncSeq2 { do! microDelay () }
                    do! microDelay ()
                }
              }
            | x -> failwithf "Invalid test variant: %A" x

        /// Returns a small AsyncSeq2 of 1..10
        let getSeqImmutable variant : IAsyncEnumerable<int> =
            match variant with
            | SeqImmutable.Sequential_YieldBang -> asyncSeq2 { yield! [ 1..10 ] }
            | SeqImmutable.Sequential_Yield -> asyncSeq2 {
                yield 1
                yield 2
                yield 3
                yield 4
                yield 5
                yield 6
                yield 7
                yield 8
                yield 9
                yield 10
              }

            | SeqImmutable.Sequential_For -> asyncSeq2 {
                // F# BUG? coloring disappears?
                for x = 0 to 9 do
                    yield x + 1
              }

            | SeqImmutable.Sequential_Combine -> asyncSeq2 {
                do Environment.GetEnvironmentVariable "test" |> ignore
                yield! [ 1..4 ]
                do Environment.GetEnvironmentVariable "test" |> ignore
                do Environment.GetEnvironmentVariable "test" |> ignore
                yield 5
                yield! seq { 6..10 }
              }

            | SeqImmutable.Sequential_Zero -> asyncSeq2 {
                if
                    isNull
                    <| Environment.GetEnvironmentVariable "i_do_not_exist"
                then
                    yield! [ 1..4 ]
                // absent 'else' triggers CE.Zero

                if false then
                    yield 24
                elif 10 = 12 then
                    yield 42
                // absent 'else' triggers CE.Zero

                yield! seq { 5..10 }
              }

            | SeqImmutable.ThreadSpinWait -> asyncSeq2 {
                // delay just enough with a spin-wait to occasionally cause a thread-yield
                for i in 0..9 do

                    // by returning the 'side effect seq' from the closure of the CE,
                    // the side-effect will NOT execute again
                    let! x = DelayHelper.delayTask 50L<µs> 5_000L<µs> (fun _ -> i)
                    yield x + 1
              }

            | SeqImmutable.AsyncYielded ->
                // by returning the 'side effect seq' from the closure of the CE,
                // the side-effect will NOT execute again
                asyncSeq2 { yield! sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 10 }
            | SeqImmutable.AsyncYielded_Nested ->
                // let's deeply nest the sequence, which should not cause extra side effects being executed.
                asyncSeq2 {
                    yield! asyncSeq2 {
                        yield! asyncSeq2 {
                            yield! asyncSeq2 {
                                yield! asyncSeq2 {
                                    yield! asyncSeq2 {
                                        yield! asyncSeq2 {
                                            yield! asyncSeq2 {
                                                yield! asyncSeq2 {
                                                    // by returning the 'side effect seq' from the closure of the CE,
                                                    // the side-effect will NOT execute again
                                                    yield! sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 10
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            | x -> failwithf "Invalid test variant: %A" x

        let inline inc (x: int outref) = // using byref!
            x <- x + 1
            x

        /// Returns a small AsyncSeq2 of 1..10, created with a side effect that should increase on next iteration.
        let getSeqWithSideEffect variant : IAsyncEnumerable<int> =

            // captured side-effect, should *not* be captured by GetAsyncEnumerator()
            // NOTE: this side-effect numerator DOES NOT work if the 'let mutable' were inside
            //       the asyncSeq2 CE. Just like the 'seq' CE, the mutated state will be kept
            //       and future iterations will not change it.
            let mutable i = 0

            match variant with
            | SeqWithSideEffect.Sequential_YieldBang -> asyncSeq2 { yield! List.init 10 (fun _ -> inc &i) }
            | SeqWithSideEffect.Sequential_Yield -> asyncSeq2 {
                yield inc &i // 1
                yield inc &i
                yield inc &i
                yield inc &i
                yield inc &i // 5
                yield inc &i
                yield inc &i
                yield inc &i
                yield inc &i
                yield inc &i // 10
              }

            | SeqWithSideEffect.Sequential_For -> asyncSeq2 {
                // F# BUG? coloring disappears?
                for _ = 0 to 9 do
                    i <- i + 1
                    yield i
              }

            | SeqWithSideEffect.Sequential_Combine -> asyncSeq2 {
                do i <- i + 1
                yield! [ i .. i + 4 ]
                do i <- i + 5
                yield i
                do i <- i + 1
                yield! seq { i .. i + 3 }
                i <- i + 3 // ensure we inc 'i' to 10 for a potential next iteration
              }

            | SeqWithSideEffect.Sequential_Zero -> asyncSeq2 {
                if inc &i % 10 = 1 then // always true UNLESS NOT FULLY EVALUATED!!!
                    yield! [ i .. i + 2 ]
                // absent 'else' triggers CE.Zero

                if inc &i = -24 then // never true
                    yield 24
                elif inc &i = -42 then // never true
                    yield 42
                // absent 'else' triggers CE.Zero

                yield inc &i
                yield! [ inc &i; inc &i; inc &i ]
                inc &i |> ignore
                yield i // 8
                yield! [ i + 1; i + 2 ]
                i <- i + 2 // ensure we inc 'i' to 10 for a potential next iteration
              }

            // delay just enough with a spin-wait to occasionally cause a thread-yield
            | SeqWithSideEffect.ThreadSpinWait -> sideEffectTaskSeqMicro 50L<µs> 5_000L<µs> 10
            | SeqWithSideEffect.AsyncYielded -> sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 10
            | SeqWithSideEffect.AsyncYielded_Nested ->
                // let's deeply nest the sequence, which should not cause extra side effects being executed.
                // NOTE: this list of tasks must be defined OUTSIDE the scope, otherwise, mutability on 2nd
                //       iteration will not kick in!
                let nestedTaskSeq = sideEffectTaskSeqMicro 1_000L<µs> 5_000L<µs> 10

                asyncSeq2 {
                    yield! asyncSeq2 {
                        yield! asyncSeq2 {
                            yield! asyncSeq2 {
                                yield! asyncSeq2 {
                                    yield! asyncSeq2 { yield! asyncSeq2 { yield! asyncSeq2 { yield! asyncSeq2 { yield! nestedTaskSeq } } } }
                                }
                            }
                        }
                    }
                }
            | x -> failwithf "Invalid test variant: %A" x

        /// An empty asyncSeq2 that can be used with tests for checking if the dispose method gets called.
        /// Will add 1 to the passed integer upon disposing.
        let getEmptyDisposableTaskSeq (disposed: int ref) =
            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator _ =
                    { new IAsyncEnumerator<'T> with
                        member _.MoveNextAsync() = ValueTask.False
                        member _.Current = Unchecked.defaultof<'T>
                        member _.DisposeAsync() = ValueTask(task { do disposed.Value <- disposed.Value + 1 })
                    }
            }

        /// A singleton asyncSeq2 that can be used with tests for checking if the dispose method gets called
        /// The singleton value is '42'. Will add 1 to the passed integer upon disposing.
        let getSingletonDisposableTaskSeq (disposed: int ref) =
            { new IAsyncEnumerable<int> with
                member _.GetAsyncEnumerator _ =
                    let mutable status = BeforeAll

                    { new IAsyncEnumerator<int> with
                        member _.MoveNextAsync() =
                            match status with
                            | BeforeAll ->
                                status <- WithCurrent
                                ValueTask.True
                            | WithCurrent ->
                                status <- AfterAll
                                ValueTask.False
                            | AfterAll -> ValueTask.False

                        member _.Current: int =
                            match status with
                            | WithCurrent -> 42
                            | _ -> Unchecked.defaultof<int>

                        member _.DisposeAsync() = ValueTask(task { do disposed.Value <- disposed.Value + 1 })
                    }
            }
    //
    // following types can be used with Theory & TestData
    //

    type TestEmptyVariants() as this =
        inherit TheoryData<EmptyVariant>()

        do
            this.Add EmptyVariant.CallEmpty
            this.Add EmptyVariant.Do
            this.Add EmptyVariant.DoBang
            this.Add EmptyVariant.YieldBang
            this.Add EmptyVariant.YieldBangNested
            this.Add EmptyVariant.DelayDoBang
            this.Add EmptyVariant.DelayYieldBang
            this.Add EmptyVariant.DelayYieldBangNested

    type TestImmTaskSeq() as this =
        inherit TheoryData<SeqImmutable>()

        do
            this.Add SeqImmutable.Sequential_YieldBang
            this.Add SeqImmutable.Sequential_Yield
            this.Add SeqImmutable.Sequential_For
            this.Add SeqImmutable.Sequential_Combine
            this.Add SeqImmutable.Sequential_Zero
            this.Add SeqImmutable.ThreadSpinWait
            this.Add SeqImmutable.AsyncYielded
            this.Add SeqImmutable.AsyncYielded_Nested

    type TestSideEffectTaskSeq() as this =
        inherit TheoryData<SeqWithSideEffect>()

        do
            this.Add SeqWithSideEffect.Sequential_YieldBang
            this.Add SeqWithSideEffect.Sequential_Yield
            this.Add SeqWithSideEffect.Sequential_For
            this.Add SeqWithSideEffect.Sequential_Combine
            this.Add SeqWithSideEffect.Sequential_Zero
            this.Add SeqWithSideEffect.ThreadSpinWait
            this.Add SeqWithSideEffect.AsyncYielded
            this.Add SeqWithSideEffect.AsyncYielded_Nested
