// Copyright (c) Microsoft Corporation.  All Rights Reserved.  See License.txt in the project root for license information.

namespace Microsoft.FSharp.Control

    open System
    open System.Threading
    open System.Threading.Tasks
    open System.Runtime.CompilerServices
    open System.Runtime.ExceptionServices

    open Microsoft.FSharp.Core
    open Microsoft.FSharp.Core.CompilerServices
    open Microsoft.FSharp.Control
    open Microsoft.FSharp.Collections

    /// <summary>
    /// An asynchronous computation, which, when run, will eventually produce a value  of type T, or else raises an exception.
    /// </summary>
    ///
    /// <remarks>
    ///  This type has no members. Asynchronous computations are normally specified either by using an async2 expression
    ///  or the static methods in the <see cref="T:Microsoft.FSharp.Control.FSharpAsync2`1"/> type.
    ///
    ///  See also <a href="https://learn.microsoft.com/dotnet/fsharp/language-reference/async2-expressions">F# Language Guide - Async2 Workflows</a>.
    /// </remarks>
    ///
    /// <namespacedoc><summary>
    ///   Library functionality for asynchronous programming, events and agents. See also
    ///   <a href="https://learn.microsoft.com/dotnet/fsharp/language-reference/async2-expressions">Asynchronous Programming</a>,
    ///   <a href="https://learn.microsoft.com/dotnet/fsharp/language-reference/members/events">Events</a> and
    ///   <a href="https://learn.microsoft.com/dotnet/fsharp/language-reference/lazy-expressions">Lazy Expressions</a> in the
    ///   F# Language Guide.
    /// </summary></namespacedoc>
    ///
    /// <category index="1">Async2 Programming</category>

    [<Sealed; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
    type Async2<'T>

    /// <summary>Holds static members for creating and manipulating asynchronous computations.</summary>
    ///
    /// <remarks>
    ///  See also <a href="https://learn.microsoft.com/dotnet/fsharp/language-reference/async2-expressions">F# Language Guide - Async2 Workflows</a>.
    /// </remarks>
    ///
    /// <category index="1">Async2 Programming</category>

    [<Sealed>]
    [<CompiledName("FSharpAsync2")>]
    type Async2 =

        /// <summary><p>Runs the computation and blocks the caller until it completes.</p>
        /// <p>Runs inline on the calling thread when it is a thread-pool thread with no ambient SynchronizationContext
        /// and no timeout; otherwise runs on the thread pool.</p>
        /// </summary>
        /// <remarks>
        /// <p>Note For F# interactive, F# scripts, and unit tests consider using
        /// <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.RunSynchronouslyImmediate`1"/>, which
        /// always starts on the calling thread and presents a simpler stack trace in exception cases and/or under a debugger.</p>
        /// <p>Computation runs directly on the calling thread when
        /// <see cref="P:System.Threading.SynchronizationContext.Current"/> is <c>null</c>,
        /// <see cref="P:System.Threading.Thread.IsThreadPoolThread"/> is <c>true</c>, and no timeout is specified.</p>
        /// </remarks>
        /// <param name="computation">The computation to run.</param>
        /// <param name="timeout">The number of milliseconds to wait for the result of the
        /// computation before raising a <see cref="T:System.TimeoutException"/>. If no value or -1 is provided
        /// the timeout will be <see cref="F:System.Threading.Timeout.Infinite"/>.</param>
        /// <param name="cancellationToken">The cancellation token to be associated with the computation.
        /// If omitted, <c>Async2.DefaultCancellationToken</c> is used.</param>
        /// <returns>The result of the computation. Any exception raised by the computation is propagated to the caller.</returns>
        /// <category index="0">Starting Async2 Computations</category>
        /// <example id="run-synchronously-1">
        /// <code lang="fsharp">
        /// printn "A" // runs on caller thread
        ///
        /// let result = async2 {
        ///     printn "B" // runs on a background/threadpool thread
        ///     do! Async2.Sleep(1000)
        ///     printn "C" // continuation runs on a background/threadpool thread
        ///     return 17
        /// } |> Async2.RunSynchronously
        ///
        /// printn "D" // runs on caller thread
        /// </code>
        /// <p>Prints "A", "B" immediately, then "C", "D" after 1 second.</p>
        /// <p>Yields <c>result = 17</c>.</p>
        /// </example>
        static member RunSynchronously : computation:Async2<'T> * ?timeout : int * ?cancellationToken:CancellationToken-> 'T

        /// <summary><p>Starts the asynchronous computation on the calling thread, disregarding the ambient
        /// <see cref="T:System.Threading.SynchronizationContext"/>.</p>
        /// <p>During any asynchronous continuations after the first suspension, the calling thread blocks awaiting the outcome.</p>
        /// </summary>
        /// <remarks>
        /// <p>Warning: blocks the calling thread for the duration of the computation. Calling it
        /// from a UI thread will make the UI unresponsive and risks deadlock if any continuation in the
        /// computation needs to be dispatched back to that context.</p>
        /// <p>Normally preferred to <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.RunSynchronously`1"/> for
        /// interactive use in F# scripts and F# interactive (FSI), and for unit tests as: <br/>
        /// - a breakpoint will show a clearer call stack prior to the first suspension (as opposed to it waiting for an asynchronous completion notification from another thread<br/>
        /// - the stack trace in the case of an exception will have two fewer frames.
        /// </p>
        /// <p>Does not support a timeout; see
        /// <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.RunSynchronously`1"/> if one is desired.</p>
        /// <p>Does not ensure execution takes place on a threadpool thread; see
        /// <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.RunSynchronously`1"/> or
        /// <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.SwitchToThreadPool"/> if this is required.</p>
        /// </remarks>
        /// <param name="computation">The computation to run.</param>
        /// <param name="cancellationToken">The cancellation token to be associated with the computation.
        /// If omitted, <c>Async2.DefaultCancellationToken</c> is used.</param>
        /// <returns>The result of the computation. Any exception raised by the computation is propagated to the caller.</returns>
        /// <category index="0">Starting Async2 Computations</category>
        /// <example id="run-synchronously-immediate-1">
        /// <code lang="fsharp">
        /// printn "A" // runs on calling thread
        ///
        /// let result = async2 {
        ///     printn "B" // ALSO runs on calling thread (hence immediately)
        ///     do! Async2.Sleep(1000)
        ///     printn "C" // runs in continuation context (depends on SynchronizationContext etc)
        ///     return 17
        /// } |> Async2.RunSynchronouslyImmediate
        ///
        /// printn "D" // runs on calling thread
        /// </code>
        /// <p>Prints "A", "B" immediately, then "C", "D" after 1 second.</p>
        /// <p>Yields <c>result = 17</c>.</p>
        /// </example>
        static member RunSynchronouslyImmediate : computation : Async2<'T> * ?cancellationToken : CancellationToken -> 'T

        /// <summary>Starts the asynchronous computation in the thread pool. Do not await its result.</summary>
        ///
        /// <remarks>If no cancellation token is provided then the default cancellation token is used.</remarks>
        ///
        /// <param name="computation">The computation to run asynchronously.</param>
        /// <param name="cancellationToken">The cancellation token to be associated with the computation.
        /// If one is not supplied, the default cancellation token is used.</param>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example id="start-1">
        /// <code lang="fsharp">
        /// printn "A"
        ///
        /// async2 {
        ///     printn "B"
        ///     do! Async2.Sleep(1000)
        ///     printn "C"
        /// } |> Async2.Start
        ///
        /// printn "D"
        /// </code>
        /// Prints "A", then "D", "B" quickly in any order, and then "C" in 1 second.
        /// </example>
        /// <example-tbd></example-tbd>
        static member Start : computation:Async2<unit> * ?cancellationToken:CancellationToken -> unit

        /// <summary>Executes a computation in the thread pool.</summary>
        ///
        /// <remarks>If no cancellation token is provided then the default cancellation token is used.</remarks>
        ///
        /// <returns>A <see cref="T:System.Threading.Tasks.Task`1"/> that will be completed
        /// in the corresponding state once the computation terminates (produces the result, throws exception or gets canceled)</returns>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example id="start-as-task-1">
        /// <code lang="fsharp">
        /// printn "A"
        ///
        /// let t =
        ///     async2 {
        ///         printn "B"
        ///         do! Async2.Sleep(1000)
        ///         printn "C"
        ///     } |> Async2.StartAsTask
        ///
        /// printn "D"
        /// t.Wait()
        /// printn "E"
        /// </code>
        /// Prints "A", then "D", "B" quickly in any order, then "C", "E" in 1 second.
        /// </example>
        static member StartAsTask : computation:Async2<'T> * ?taskCreationOptions:TaskCreationOptions * ?cancellationToken:CancellationToken -> Task<'T>

        /// <summary>Creates an asynchronous computation which starts the given computation as a <see cref="T:System.Threading.Tasks.Task`1"/></summary>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example-tbd></example-tbd>
        static member StartChildAsTask : computation:Async2<'T> * ?taskCreationOptions:TaskCreationOptions -> Async2<Task<'T>>

        /// <summary>Creates an asynchronous computation that executes <c>computation</c>.
        /// If this computation completes successfully then return <c>Choice1Of2</c> with the returned
        /// value. If this computation raises an exception before it completes then return <c>Choice2Of2</c>
        /// with the raised exception.</summary>
        ///
        /// <param name="computation">The input computation that returns the type T.</param>
        ///
        /// <returns>A computation that returns a choice of type T or exception.</returns>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example id="catch-example-1">
        /// <code lang="fsharp">
        /// let someRiskyBusiness() =
        /// match DateTime.Today with
        /// | dt when dt.DayOfWeek = DayOfWeek.Monday -> failwith "Not compatible with Mondays"
        /// | dt -> dt
        ///
        /// async2 { return someRiskyBusiness() }
        /// |> Async2.Catch
        /// |> Async2.RunSynchronously
        /// |> function
        ///     | Choice1Of2 result -> printfn $"Result: {result}"
        ///     | Choice2Of2 e -> printfn $"Exception: {e}"
        /// </code>
        /// Prints the returned value of someRiskyBusiness() or the exception if there is one.
        /// </example>
        static member Catch : computation:Async2<'T> -> Async2<Choice<'T,exn>>

        /// <summary>Creates an asynchronous computation that executes <c>computation</c>.
        /// If this computation is cancelled before it completes then the computation generated by
        /// running <c>compensation</c> is executed.</summary>
        ///
        /// <param name="computation">The input asynchronous computation.</param>
        /// <param name="compensation">The function to be run if the computation is cancelled.</param>
        ///
        /// <returns>An asynchronous computation that runs the compensation if the input computation
        /// is cancelled.</returns>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example id="try-cancelled-1">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 11 ]
        /// for i in primes do
        ///     Async2.TryCancelled(
        ///         async2 {
        ///             do! Async2.Sleep(i * 1000)
        ///             printfn $"{i}"
        ///         },
        ///         fun oce -> printfn $"Computation Cancelled: {i}")
        ///     |> Async2.Start
        ///
        /// Thread.Sleep(6000)
        /// Async2.CancelDefaultToken()
        /// printfn "Tasks Finished"
        /// </code>
        /// This will print "2" 2 seconds from start, "3" 3 seconds from start, "5" 5 seconds from start, cease computation
        /// and then print "Computation Cancelled: 7", "Computation Cancelled: 11" and "Tasks Finished" in any order.
        /// </example>
        static member TryCancelled : computation:Async2<'T> * compensation:(OperationCanceledException -> unit) -> Async2<'T>

        /// <summary>Generates a scoped, cooperative cancellation handler for use within an asynchronous workflow.</summary>
        ///
        /// <remarks>For example,
        ///     <c>async2 { use! holder = Async2.OnCancel interruption ... }</c>
        /// generates an asynchronous computation where, if a cancellation happens any time during
        /// the execution of the asynchronous computation in the scope of <c>holder</c>, then action
        /// <c>interruption</c> is executed on the thread that is performing the cancellation. This can
        /// be used to arrange for a computation to be asynchronously notified that a cancellation
        /// has occurred, e.g. by setting a flag, or deregistering a pending I/O action.</remarks>
        ///
        /// <param name="interruption">The function that is executed on the thread performing the
        /// cancellation.</param>
        ///
        /// <returns>An asynchronous computation that triggers the interruption if it is cancelled
        /// before being disposed.</returns>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example id="on-cancel-1">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 11 ]
        /// for i in primes do
        ///     async2 {
        ///         use! holder = Async2.OnCancel(fun () -> printfn $"Computation Cancelled: {i}")
        ///         do! Async2.Sleep(i * 1000)
        ///         printfn $"{i}"
        ///     }
        ///     |> Async2.Start
        ///
        /// Thread.Sleep(6000)
        /// Async2.CancelDefaultToken()
        /// printfn "Tasks Finished"
        /// </code>
        /// This will print "2" 2 seconds from start, "3" 3 seconds from start, "5" 5 seconds from start, cease computation
        /// and then print "Computation Cancelled: 7", "Computation Cancelled: 11" and "Tasks Finished" in any order.
        /// </example>
        static member OnCancel : interruption: (unit -> unit) -> Async2<IDisposable>

        /// <summary>Creates an asynchronous computation that returns the CancellationToken governing the execution
        /// of the computation.</summary>
        ///
        /// <remarks>In <c>async2 { let! token = Async2.CancellationToken ...}</c> token can be used to initiate other
        /// asynchronous operations that will cancel cooperatively with this workflow.</remarks>
        ///
        /// <returns>An asynchronous computation capable of retrieving the CancellationToken from a computation
        /// expression.</returns>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example-tbd></example-tbd>
        static member CancellationToken : Async2<CancellationToken>

        /// <summary>Raises the cancellation condition for the most recent set of asynchronous computations started
        /// without any specific CancellationToken. Replaces the global CancellationTokenSource with a new
        /// global token source for any asynchronous computations created after this point without any
        /// specific CancellationToken.</summary>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example id="cancel-default-token-1">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 11 ]
        ///
        /// let computations =
        ///     [ for i in primes do
        ///             async2 {
        ///                 do! Async2.Sleep(i * 1000)
        ///                 printfn $"{i}"
        ///             }
        ///     ]
        ///
        /// try
        ///     let t =
        ///         Async2.Parallel(computations, 3) |> Async2.StartAsTask
        ///
        ///     Thread.Sleep(6000)
        ///     Async2.CancelDefaultToken()
        ///     printfn $"Tasks Finished: %A{t.Result}"
        /// with
        /// | :? System.AggregateException as ae -> printfn $"Tasks Not Finished: {ae.Message}"
        /// </code>
        /// This will print "2" 2 seconds from start, "3" 3 seconds from start, "5" 5 seconds from start, cease computation and
        /// then print "Tasks Not Finished: One or more errors occurred. (A task was canceled.)".
        /// </example>
        static member CancelDefaultToken :  unit -> unit

        /// <summary>Gets the default cancellation token for executing asynchronous computations.</summary>
        ///
        /// <returns>The default CancellationToken.</returns>
        ///
        /// <category index="3">Cancellation and Exceptions</category>
        ///
        /// <example id="default-cancellation-token-1">
        /// <code lang="fsharp">
        /// Async2.DefaultCancellationToken.Register(fun () -> printfn "Computation Cancelled") |> ignore
        /// let primes = [ 2; 3; 5; 7; 11 ]
        ///
        /// for i in primes do
        ///     async2 {
        ///         do! Async2.Sleep(i * 1000)
        ///         printfn $"{i}"
        ///     }
        ///     |> Async2.Start
        ///
        /// Thread.Sleep(6000)
        /// Async2.CancelDefaultToken()
        /// printfn "Tasks Finished"
        /// </code>
        /// This will print "2" 2 seconds from start, "3" 3 seconds from start, "5" 5 seconds from start, cease computation and then
        /// print "Computation Cancelled", followed by "Tasks Finished".
        /// </example>
        static member DefaultCancellationToken : CancellationToken

        //---------- Parallelism

        /// <summary>Starts a child computation within an asynchronous workflow.
        /// This allows multiple asynchronous computations to be executed simultaneously.</summary>
        ///
        /// <remarks>This method should normally be used as the immediate
        /// right-hand-side of a <c>let!</c> binding in an F# asynchronous workflow, that is,
        /// <code lang="fsharp">
        ///     async2 { ...
        ///            let! completor1 = childComputation1 |> Async2.StartChild
        ///            let! completor2 = childComputation2 |> Async2.StartChild
        ///            ...
        ///            let! result1 = completor1
        ///            let! result2 = completor2
        ///            ... }
        /// </code>
        ///
        /// When used in this way, each use of <c>StartChild</c> starts an instance of <c>childComputation</c>
        /// and returns a completor object representing a computation to wait for the completion of the operation.
        /// When executed, the completor awaits the completion of <c>computation</c>.</remarks>
        ///
        /// <param name="computation">The computation to start.</param>
        /// <param name="millisecondsTimeout">The optional timeout value in milliseconds.</param>
        ///
        /// <returns>A computation that waits for the child computation to be completed.</returns>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example id="start-child-1">
        /// <code lang="fsharp">
        ///
        /// let computeWithTimeout timeout =
        ///     async2 {
        ///         let! completor1 =
        ///             Async2.StartChild(
        ///                 (async2 {
        ///                     do! Async2.Sleep(1000)
        ///                     return 1
        ///                  }),
        ///                 millisecondsTimeout = timeout)
        ///
        ///         let! completor2 =
        ///             Async2.StartChild(
        ///                 (async2 {
        ///                     do! Async2.Sleep(2000)
        ///                     return 2
        ///                  }),
        ///                 millisecondsTimeout = timeout)
        ///         do! Async2.Sleep 500 // Or any other async2 activity
        ///         let! v1 = completor1
        ///         let! v2 = completor2
        ///         printfn $"Result: {v1 + v2}"
        ///     } |> Async2.RunSynchronouslyImmediate
        /// </code>
        /// Will throw a <c>System.TimeoutException</c> if called with a timeout under 2000, otherwise will print "Result: 3".
        /// </example>
        static member StartChild : computation:Async2<'T> * ?millisecondsTimeout : int -> Async2<Async2<'T>>

        /// <summary>Creates an asynchronous computation that executes all the given asynchronous computations,
        /// initially queueing each as work items and using a fork/join pattern.</summary>
        ///
        /// <remarks>If all child computations succeed, an array of results is passed to the success continuation.
        ///
        /// If any child computation raises an exception, then the overall computation will trigger an
        /// exception, and cancel the others.
        ///
        /// The overall computation will respond to cancellation while executing the child computations.
        /// If cancelled, the computation will cancel any remaining child computations but will still wait
        /// for the other child computations to complete.</remarks>
        ///
        /// <param name="computations">A sequence of distinct computations to be parallelized.</param>
        ///
        /// <returns>A computation that returns an array of values from the sequence of input computations.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="parallel-1">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 10; 11 ]
        /// let t =
        ///     [ for i in primes do
        ///         async2 {
        ///             do! Async2.Sleep(System.Random().Next(1000, 2000))
        ///
        ///             if i % 2 > 0 then
        ///                 printfn $"{i}"
        ///                 return true
        ///             else
        ///                 return false
        ///         }
        ///     ]
        ///     |> Async2.Parallel
        ///     |> Async2.StartAsTask
        ///
        /// t.Wait()
        /// printfn $"%A{t.Result}"
        /// </code>
        /// This will print "3", "5", "7", "11" (in any order) in 1-2 seconds and then [| false; true; true; true; false; true |].
        /// </example>
        static member Parallel : computations:seq<Async2<'T>> -> Async2<'T array>

        /// <summary>Creates an asynchronous computation that executes all the given asynchronous computations,
        /// initially queueing each as work items and using a fork/join pattern.</summary>
        ///
        /// <remarks>If all child computations succeed, an array of results is passed to the success continuation.
        ///
        /// If any child computation raises an exception, then the overall computation will trigger an
        /// exception, and cancel the others.
        ///
        /// The overall computation will respond to cancellation while executing the child computations.
        /// If cancelled, the computation will cancel any remaining child computations but will still wait
        /// for the other child computations to complete.</remarks>
        ///
        /// <param name="computations">A sequence of distinct computations to be parallelized.</param>
        /// <param name="maxDegreeOfParallelism">The maximum degree of parallelism in the parallel execution.</param>
        ///
        /// <returns>A computation that returns an array of values from the sequence of input computations.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="parallel-2">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 10; 11 ]
        /// let computations =
        ///     [ for i in primes do
        ///         async2 {
        ///             do! Async2.Sleep(System.Random().Next(1000, 2000))
        ///
        ///             return
        ///                 if i % 2 > 0 then
        ///                     printfn $"{i}"
        ///                     true
        ///                 else
        ///                     false
        ///         } ]
        ///
        /// let t =
        ///     Async2.Parallel(computations, maxDegreeOfParallelism=3)
        ///     |> Async2.StartAsTask
        ///
        /// t.Wait()
        /// printfn $"%A{t.Result}"
        /// </code>
        /// This will print "3", "5" (in any order) in 1-2 seconds, and then "7", "11" (in any order) in 1-2 more seconds and then
        /// [| false; true; true; true; false; true |].
        /// </example>
        static member Parallel : computations:seq<Async2<'T>> * ?maxDegreeOfParallelism : int -> Async2<'T array>

        /// <summary>Creates an asynchronous computation that executes all the given asynchronous computations sequentially.</summary>
        ///
        /// <remarks>If all child computations succeed, an array of results is passed to the success continuation.
        ///
        /// If any child computation raises an exception, then the overall computation will trigger an
        /// exception, and cancel the others.
        ///
        /// The overall computation will respond to cancellation while executing the child computations.
        /// If cancelled, the computation will cancel any remaining child computations but will still wait
        /// for the other child computations to complete.</remarks>
        ///
        /// <param name="computations">A sequence of distinct computations to be run in sequence.</param>
        ///
        /// <returns>A computation that returns an array of values from the sequence of input computations.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="sequential-1">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7; 10; 11 ]
        /// let computations =
        ///     [ for i in primes do
        ///             async2 {
        ///                 do! Async2.Sleep(System.Random().Next(1000, 2000))
        ///
        ///                 if i % 2 > 0 then
        ///                     printfn $"{i}"
        ///                     return true
        ///                 else
        ///                     return false
        ///             }
        ///    ]
        ///
        /// let t =
        ///     Async2.Sequential(computations)
        ///     |> Async2.StartAsTask
        ///
        /// t.Wait()
        /// printfn $"%A{t.Result}"
        /// </code>
        /// This will print "3", "5", "7", "11" with ~1-2 seconds between them except for pauses where even numbers would be and then
        /// prints [| false; true; true; true; false; true |].
        /// </example>
        static member Sequential : computations:seq<Async2<'T>> -> Async2<'T array>

        /// <summary>
        /// Creates an asynchronous computation that executes all given asynchronous computations in parallel,
        /// returning the result of the first succeeding computation (one whose result is 'Some x').
        /// If all child computations complete with None, the parent computation also returns None.
        /// </summary>
        ///
        /// <remarks>
        /// If any child computation raises an exception, then the overall computation will trigger an
        /// exception, and cancel the others.
        ///
        /// The overall computation will respond to cancellation while executing the child computations.
        /// If cancelled, the computation will cancel any remaining child computations but will still wait
        /// for the other child computations to complete.
        /// </remarks>
        ///
        /// <param name="computations">A sequence of computations to be parallelized.</param>
        ///
        /// <returns>A computation that returns the first succeeding computation.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="choice-example-1">
        /// <code lang="fsharp">
        /// printfn "Starting"
        /// let primes = [ 2; 3; 5; 7 ]
        /// let computations =
        ///     [ for i in primes do
        ///         async2 {
        ///             do! Async2.Sleep(System.Random().Next(1000, 2000))
        ///             return if i % 2 > 0 then Some(i) else None
        ///         }
        ///     ]
        ///
        /// computations
        /// |> Async2.Choice
        /// |> Async2.RunSynchronously
        /// |> function
        ///     | Some (i) -> printfn $"{i}"
        ///     | None -> printn "No Result"
        /// </code>
        /// Prints one randomly selected odd number in 1-2 seconds. If the list is changed to all even numbers, it will
        /// instead print "No Result".
        /// </example>
        ///
        /// <example id="choice-example-2">
        /// <code lang="fsharp">
        /// let primes = [ 2; 3; 5; 7 ]
        /// let computations =
        ///     [ for i in primes do
        ///         async2 {
        ///             do! Async2.Sleep(System.Random().Next(1000, 2000))
        ///
        ///             return
        ///                 if i % 2 > 0 then
        ///                     Some(i)
        ///                 else
        ///                     failwith $"Even numbers not supported: {i}"
        ///         }
        ///     ]
        ///
        /// computations
        /// |> Async2.Choice
        /// |> Async2.RunSynchronously
        /// |> function
        ///     | Some (i) -> printfn $"{i}"
        ///     | None -> printn "No Result"
        /// </code>
        /// Will sometimes print one randomly selected odd number, sometimes throw System.Exception("Even numbers not supported: 2").
        /// </example>
        static member Choice : computations:seq<Async2<'T option>> -> Async2<'T option>

        //---------- Thread Control

        /// <summary>Creates an asynchronous computation that creates a new thread and runs
        /// its continuation in that thread.</summary>
        ///
        /// <returns>A computation that will execute on a new thread.</returns>
        ///
        /// <category index="4">Threads and Contexts</category>
        ///
        /// <example id="switch-to-new-thread">
        /// <code lang="fsharp">
        /// async2 {
        ///     do! Async2.SwitchToNewThread()
        ///     do! someLongRunningComputation()
        /// } |> Async2.StartImmediate
        /// </code>
        /// This will run someLongRunningComputation() without blocking the threads in the threadpool.
        /// </example>
        static member SwitchToNewThread : unit -> Async2<unit>

        /// <summary>Creates an asynchronous computation that queues a work item that runs
        /// its continuation.</summary>
        ///
        /// <returns>A computation that generates a new work item in the thread pool.</returns>
        ///
        /// <category index="4">Threads and Contexts</category>
        ///
        /// <example id="switch-to-thread-pool-1">
        /// <code lang="fsharp">
        /// async2 {
        ///     do! Async2.SwitchToNewThread()
        ///     do! someLongRunningComputation()
        ///     do! Async2.SwitchToThreadPool()
        ///
        ///     for i in 1 .. 10 do
        ///         do! someShortRunningComputation()
        /// } |> Async2.StartImmediate
        /// </code>
        /// This will run someLongRunningComputation() without blocking the threads in the threadpool, and then switch to the
        /// threadpool for shorter computations.
        /// </example>
        static member SwitchToThreadPool :  unit -> Async2<unit>

        /// <summary>Creates an asynchronous computation that runs
        /// its continuation using syncContext.Post. If syncContext is null
        /// then the asynchronous computation is equivalent to SwitchToThreadPool().</summary>
        ///
        /// <param name="syncContext">The synchronization context to accept the posted computation.</param>
        ///
        /// <returns>An asynchronous computation that uses the syncContext context to execute.</returns>
        ///
        /// <category index="4">Threads and Contexts</category>
        ///
        /// <example-tbd></example-tbd>
        static member SwitchToContext : syncContext: SynchronizationContext | null -> Async2<unit>

        /// <summary>Creates an asynchronous computation that captures the current
        /// success, exception and cancellation continuations. The callback must
        /// eventually call exactly one of the given continuations.</summary>
        ///
        /// <param name="callback">The function that accepts the current success, exception, and cancellation
        /// continuations.</param>
        ///
        /// <returns>An asynchronous computation that provides the callback with the current continuations.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="from-continuations-1">
        /// <code lang="fsharp">
        /// let someRiskyBusiness() =
        /// match DateTime.Today with
        /// | dt when dt.DayOfWeek = DayOfWeek.Monday -> failwith "Not compatible with Mondays"
        /// | dt -> dt
        ///
        /// let computation =
        ///     (fun (successCont, exceptionCont, cancellationCont) ->
        ///         try
        ///             someRiskyBusiness () |> successCont
        ///         with
        ///         | :? OperationCanceledException as oce -> cancellationCont oce
        ///         | e -> exceptionCont e)
        ///     |> Async2.FromContinuations
        ///
        /// Async2.StartWithContinuations(
        ///     computation,
        ///     (fun result -> printfn $"Result: {result}"),
        ///     (fun e -> printfn $"Exception: {e}"),
        ///     (fun oce -> printfn $"Cancelled: {oce}")
        ///  )
        /// </code>
        /// This anonymous function will call someRiskyBusiness() and properly use the provided continuations
        /// defined to report the outcome.
        /// </example>
        static member FromContinuations : callback:(('T -> unit) * (exn -> unit) * (OperationCanceledException -> unit) -> unit) -> Async2<'T>

        /// <summary>Creates an asynchronous computation that waits for a single invocation of a CLI
        /// event by adding a handler to the event. Once the computation completes or is
        /// cancelled, the handler is removed from the event.</summary>
        ///
        /// <remarks>The computation will respond to cancellation while waiting for the event. If a
        /// cancellation occurs, and <c>cancelAction</c> is specified, then it is executed, and
        /// the computation continues to wait for the event.
        ///
        /// If <c>cancelAction</c> is not specified, then cancellation causes the computation
        /// to cancel immediately.</remarks>
        ///
        /// <param name="event">The event to handle once.</param>
        /// <param name="cancelAction">An optional function to execute instead of cancelling when a
        /// cancellation is issued.</param>
        ///
        /// <returns>An asynchronous computation that waits for the event to be invoked.</returns>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example-tbd></example-tbd>
        static member AwaitEvent: event:IEvent<'Del,'T> * ?cancelAction : (unit -> unit) -> Async2<'T> when 'Del : delegate<'T,unit> and 'Del :> Delegate

        /// <summary>Creates an asynchronous computation that will wait on the given WaitHandle.</summary>
        ///
        /// <remarks>The computation returns true if the handle indicated a result within the given timeout.</remarks>
        ///
        /// <param name="waitHandle">The <c>WaitHandle</c> that can be signalled.</param>
        /// <param name="millisecondsTimeout">The timeout value in milliseconds.  If one is not provided
        /// then the default value of -1 corresponding to <see cref="F:System.Threading.Timeout.Infinite"/>.</param>
        ///
        /// <returns>An asynchronous computation that waits on the given <c>WaitHandle</c>.</returns>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example-tbd></example-tbd>
        static member AwaitWaitHandle: waitHandle: WaitHandle * ?millisecondsTimeout:int -> Async2<bool>

        /// <summary>Creates an asynchronous computation that will wait on the IAsyncResult.</summary>
        ///
        /// <remarks>The computation returns true if the handle indicated a result within the given timeout.</remarks>
        ///
        /// <param name="iar">The IAsyncResult to wait on.</param>
        /// <param name="millisecondsTimeout">The timeout value in milliseconds.  If one is not provided
        /// then the default value of -1 corresponding to <see cref="F:System.Threading.Timeout.Infinite"/>.</param>
        ///
        /// <returns>An asynchronous computation that waits on the given <c>IAsyncResult</c>.</returns>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example-tbd></example-tbd>
        static member AwaitIAsyncResult: iar: IAsyncResult * ?millisecondsTimeout:int -> Async2<bool>

        /// <summary>Creates an asynchronous computation that will wait asynchronously for the given task to complete, returning
        /// its result. Note exceptions are wrapped in <see cref="T:System.AggregateException"/>; for new
        /// code, prefer <c>Async2.Await</c>, which surfaces single exceptions directly.</summary>
        /// <param name="task">The task to await.</param>
        /// <remarks>
        /// <p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised. Note
        /// that the task may be governed by a different cancellation token to the overall async2 computation
        /// where the AwaitTask occurs. In practice you should normally start the task with the
        /// cancellation token returned by <c>let! ct = Async2.CancellationToken</c>, and catch
        /// any <see cref="T:System.Threading.Tasks.TaskCanceledException"/> at the point where the
        /// overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        /// <category index="2">Awaiting Results</category>
        /// <example id="awaittask-1">
        /// <code lang="fsharp">
        /// let t = Task.Run(fun () -> invalidOp "test"; 42)
        /// async2 {
        ///     try
        ///         let! _ = Async2.AwaitTask t
        ///         ()
        ///     with
        ///     | :? System.InvalidOperationException ->
        ///         printfn "unreachable" // will not match: exception is wrapped in AggregateException
        ///     | :? System.AggregateException as e ->
        ///         printfn $"Caught: {e.InnerException.Message}"
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>. The <c>InvalidOperationException</c> branch is not reached because
        /// exceptions from tasks are always wrapped in <see cref="T:System.AggregateException"/>. Contrast with <c>Async2.Await</c>.
        /// </example>
        static member AwaitTask: task: Task<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that will wait asynchronously for the given task to complete.
        /// Note exceptions are wrapped in <see cref="T:System.AggregateException"/>; for new
        /// code, prefer <c>Async2.Await</c>, which surfaces single exceptions directly.</summary>
        /// <param name="task">The task to await.</param>
        /// <remarks><p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised. Note
        /// that the task may be governed by a different cancellation token to the overall async2 computation
        /// where the AwaitTask occurs. In practice you should normally start the task with the
        /// cancellation token returned by <c>let! ct = Async2.CancellationToken</c>, and catch
        /// any <see cref="T:System.Threading.Tasks.TaskCanceledException"/> at the point where the
        /// overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        /// <category index="2">Awaiting Results</category>
        /// <example id="awaittask-2">
        /// <code lang="fsharp">
        /// let t = Task.Run(fun () -> invalidOp "test")
        /// async2 {
        ///     try
        ///         do! Async2.AwaitTask t
        ///     with
        ///     | :? System.InvalidOperationException ->
        ///         printfn "unreachable" // will not match: exception is wrapped in AggregateException
        ///     | :? System.AggregateException as e ->
        ///         printfn $"Caught: {e.InnerException.Message}"
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>. The <c>InvalidOperationException</c> branch is not reached because
        /// exceptions from tasks are always wrapped in <see cref="T:System.AggregateException"/>. Contrast with <c>Async2.Await</c>.
        /// </example>
        static member AwaitTask: task: Task -> Async2<unit>

        /// <summary>Creates an asynchronous computation that will wait for the given task to complete and return
        /// its result.</summary>
        ///
        /// <param name="task">The task to await.</param>
        ///
        /// <remarks>
        /// <p>Exceptions are surfaced directly: a task faulted with a single exception raises that
        /// exception; only <see cref="T:System.AggregateException"/>s carrying multiple inner exceptions are
        /// re-raised as-is. For the legacy behavior of uniformly presenting the raw underlying
        /// <see cref="T:System.AggregateException"/>, use <c>Async2.AwaitTask</c>.</p>
        ///
        /// <p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised.</p>
        ///
        /// <p>Note the task may be governed by a different cancellation token than the overall async2 computation;
        /// typically tasks should be wired to the ambient cancellation token obtained via
        /// <c>let! ct = Async2.CancellationToken</c>, catching <see cref="T:System.Threading.Tasks.TaskCanceledException"/>
        /// where the overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example id="await-task-1">
        /// <code lang="fsharp">
        /// let t = Task.Run(fun () -> invalidOp "test"; 42)
        /// async2 {
        ///     try
        ///         let! _ = Async2.Await t
        ///         ()
        ///     with
        ///     | :? System.InvalidOperationException as e ->
        ///         printfn $"Caught: {e.Message}"
        ///     | :? System.AggregateException ->
        ///         printfn "unreachable" // will not match: single exception is unwrapped
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>. The <c>AggregateException</c> branch is not reached because a
        /// single-inner exception is unwrapped. Contrast with <c>Async2.AwaitTask</c>.
        /// </example>
        static member Await: task: Task<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that will wait for the given task to complete.</summary>
        /// <param name="task">The task to await.</param>
        /// <remarks>
        /// <p>Exceptions are surfaced directly: a task faulted with a single exception raises that
        /// exception; only <see cref="T:System.AggregateException"/>s carrying multiple inner exceptions are
        /// re-raised as-is. For the legacy behavior of uniformly presenting the raw underlying
        /// <see cref="T:System.AggregateException"/>, use <c>Async2.AwaitTask</c>.</p>
        ///
        /// <p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised.</p>
        ///
        /// <p>Note the task may be governed by a different cancellation token than the overall async2 computation;
        /// typically tasks should be wired to the ambient cancellation token obtained via
        /// <c>let! ct = Async2.CancellationToken</c>, catching <see cref="T:System.Threading.Tasks.TaskCanceledException"/>
        /// where the overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        /// <category index="2">Awaiting Results</category>
        /// <example id="await-task-2">
        /// <code lang="fsharp">
        /// let t = Task.Run(fun () -> invalidOp "test")
        /// async2 {
        ///     try
        ///         do! Async2.Await t
        ///     with
        ///     | :? System.InvalidOperationException as e ->
        ///         printfn $"Caught: {e.Message}"
        ///     | :? System.AggregateException ->
        ///         printfn "unreachable" // will not match: single exception is unwrapped
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>. The <c>AggregateException</c> branch is not reached because a
        /// single-inner exception is unwrapped. Contrast with <c>Async2.AwaitTask</c>.
        /// </example>
        static member Await: task: Task -> Async2<unit>

#if NETSTANDARD2_1 || NET
        /// <summary>Creates an asynchronous computation that will wait for the given <c>ValueTask</c> to complete and return
        /// its result.</summary>
        /// <param name="task">The <c>ValueTask</c> to await.</param>
        /// <remarks>
        /// <p>Exceptions are surfaced directly: a task faulted with a single exception raises that
        /// exception; only <see cref="T:System.AggregateException"/>s carrying multiple inner exceptions are
        /// re-raised as-is. For the legacy behavior of uniformly presenting the raw underlying
        /// <see cref="T:System.AggregateException"/>, use <c>Async2.AwaitTask</c>.</p>
        /// 
        /// <p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised.</p>
        /// 
        /// <p>Note the task may be governed by a different cancellation token than the overall async2 computation;
        /// typically tasks should be wired to the ambient cancellation token obtained via
        /// <c>let! ct = Async2.CancellationToken</c>, catching <see cref="T:System.Threading.Tasks.TaskCanceledException"/>
        /// where the overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        /// <category index="2">Awaiting Results</category>
        /// <example id="await-valuetask-1">
        /// <code lang="fsharp">
        /// let vt = ValueTask&lt;int&gt;(Task.Run(fun () -> invalidOp "test"; 42))
        /// async2 {
        ///     try
        ///         let! _ = Async2.Await vt
        ///         ()
        ///     with
        ///     | :? System.InvalidOperationException as e ->
        ///         printfn $"Caught: {e.Message}"
        ///     | :? System.AggregateException ->
        ///         printfn "unreachable" // will not match: single exception is unwrapped
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>.
        /// </example>
        static member Await: task: ValueTask<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that will wait for the given <c>ValueTask</c> to complete.</summary>
        /// <param name="task">The <c>ValueTask</c> to await.</param>
        /// <remarks>
        /// <p>Exceptions are surfaced directly: a task faulted with a single exception raises that
        /// exception; only <see cref="T:System.AggregateException"/>s carrying multiple inner exceptions are
        /// re-raised as-is. For the legacy behavior of uniformly presenting the raw underlying
        /// <see cref="T:System.AggregateException"/>, use <c>Async2.AwaitTask</c>.</p>
        /// 
        /// <p>If the task is canceled then <see cref="T:System.Threading.Tasks.TaskCanceledException"/> is raised.</p>
        /// 
        /// <p>Note the task may be governed by a different cancellation token than the overall async2 computation;
        /// typically tasks should be wired to the ambient cancellation token obtained via
        /// <c>let! ct = Async2.CancellationToken</c>, catching <see cref="T:System.Threading.Tasks.TaskCanceledException"/>
        /// where the overall async2 is started.</p>
        /// <p>For the common case where you are running a Task within an Asynchronous Computation,
        /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
        /// so that it can be passed to the Task being started.</p>
        /// </remarks>
        /// <category index="2">Awaiting Results</category>
        /// <example id="await-valuetask-2">
        /// <code lang="fsharp">
        /// let vt = ValueTask(Task.Run(fun () -> invalidOp "test"))
        /// async2 {
        ///     try
        ///         do! Async2.Await vt
        ///     with
        ///     | :? System.InvalidOperationException as e ->
        ///         printfn $"Caught: {e.Message}"
        ///     | :? System.AggregateException ->
        ///         printfn "unreachable" // will not match: single exception is unwrapped
        /// } |> Async2.RunSynchronously
        /// </code>
        /// Prints <c>Caught: test</c>.
        /// </example>
        static member Await: task: ValueTask -> Async2<unit>
#endif

        /// <summary>Creates an asynchronous computation that passes the ambient <c>Async2.CancellationToken</c> to
        /// <c>createTask</c>, and then awaits the resulting task, returning its result.</summary>
        ///
        /// <param name="createTask">A function that accepts a <c>CancellationToken</c> and returns a <c>Task&lt;'T&gt;</c>.</param>
        ///
        /// <remarks>The cancellation token of the enclosing async2 computation is automatically passed to
        /// <c>createTask</c>, propagating cancellation naturally to the task without requiring manual token capture.
        ///
        /// The resulting task is awaited using <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.Await``1(System.Threading.Tasks.Task{``0})"/>;
        /// exception unwrapping and cancellation handling are as per that overload.
        /// </remarks>
        /// <category index="0">Starting Async2 Computations</category>
        /// <example id="startTaskImmediate-taskt-1">
        /// <code lang="fsharp">
        /// async2 {
        ///     let! text = Async2.StartTaskImmediate(fun ct -> File.ReadAllTextAsync("file.txt", ct))
        ///     printfn "Content: %s" text
        /// }
        /// </code>
        /// </example>
        static member StartTaskImmediate: createTask: (CancellationToken -> Task<'T>) -> Async2<'T>

        /// <summary>Creates an asynchronous computation that passes the ambient <c>Async2.CancellationToken</c> to
        /// <c>createTask</c>, and then awaits the resulting task.</summary>
        ///
        /// <param name="createTask">A function that accepts a <c>CancellationToken</c> and returns a <c>Task</c>.</param>
        ///
        /// <remarks>The cancellation token of the enclosing async2 computation is automatically passed to
        /// <c>createTask</c>, propagating cancellation naturally to the task without requiring manual token capture.
        ///
        /// The resulting task is awaited using <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.Await(System.Threading.Tasks.Task)"/>;
        /// exception unwrapping and cancellation handling are as per that overload.
        /// </remarks>
        /// <category index="0">Starting Async2 Computations</category>
        /// <example id="startTaskImmediate-task-1">
        /// <code lang="fsharp">
        /// async2 {
        ///     do! Async2.StartTaskImmediate(fun ct -> File.WriteAllTextAsync("file.txt", "hello", ct))
        /// }
        /// </code>
        /// </example>
        static member StartTaskImmediate: createTask: (CancellationToken -> Task) -> Async2<unit>

#if NETSTANDARD2_1 || NET
        /// <summary>Creates an asynchronous computation that passes the ambient <c>Async2.CancellationToken</c> to
        /// <c>createTask</c>, and then awaits the resulting <c>ValueTask</c>, returning its result.</summary>
        ///
        /// <param name="createTask">A function that accepts a <c>CancellationToken</c> and returns a <c>ValueTask&lt;'T&gt;</c>.</param>
        ///
        /// <remarks>The cancellation token of the enclosing async2 computation is automatically passed to
        /// <c>createTask</c>, propagating cancellation naturally to the task without requiring manual token capture.
        ///
        /// The resulting task is awaited using <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.Await``1(System.Threading.Tasks.ValueTask{``0})"/>;
        /// exception unwrapping and cancellation handling are as per that overload.
        /// </remarks>
        /// <category index="0">Starting Async2 Computations</category>
        static member StartTaskImmediate: createTask: (CancellationToken -> ValueTask<'T>) -> Async2<'T>

        /// <summary>Creates an asynchronous computation that passes the ambient <c>Async2.CancellationToken</c> to
        /// <c>createTask</c>, and then awaits the resulting <c>ValueTask</c>.</summary>
        ///
        /// <param name="createTask">A function that accepts a <c>CancellationToken</c> and returns a <c>ValueTask</c>.</param>
        ///
        /// <remarks>The cancellation token of the enclosing async2 computation is automatically passed to
        /// <c>createTask</c>, propagating cancellation naturally to the task without requiring manual token capture.
        ///
        /// The resulting task is awaited using <see cref="M:Microsoft.FSharp.Control.FSharpAsync2.Await(System.Threading.Tasks.ValueTask)"/>;
        /// exception unwrapping and cancellation handling are as per that overload.
        /// </remarks>
        /// <category index="0">Starting Async2 Computations</category>
        static member StartTaskImmediate: createTask: (CancellationToken -> ValueTask) -> Async2<unit>

#endif
        /// <summary>
        ///  Creates an asynchronous computation that will sleep for the given time. This is scheduled
        ///  using a System.Threading.Timer object. The operation will not block operating system threads
        ///  for the duration of the wait.
        /// </summary>
        ///
        /// <param name="millisecondsDueTime">The number of milliseconds to sleep.</param>
        ///
        /// <returns>An asynchronous computation that will sleep for the given time.</returns>
        ///
        /// <exception cref="T:System.ArgumentOutOfRangeException">Thrown when the due time is negative
        /// and not infinite.</exception>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example id="sleep-1">
        /// <code lang="fsharp">
        /// async2 {
        ///     printn "A"
        ///     do! Async2.Sleep(1000)
        ///     printn "B"
        /// } |> Async2.Start
        ///
        /// printn "C"
        /// </code>
        /// Prints "C" and "A" quickly in any order, and then "B" 1 second later
        /// </example>
        static member Sleep: millisecondsDueTime:int -> Async2<unit>

        /// <summary>
        ///  Creates an asynchronous computation that will sleep for the given time. This is scheduled
        ///  using a System.Threading.Timer object. The operation will not block operating system threads
        ///  for the duration of the wait.
        /// </summary>
        ///
        /// <param name="dueTime">The amount of time to sleep.</param>
        ///
        /// <returns>An asynchronous computation that will sleep for the given time.</returns>
        ///
        /// <exception cref="T:System.ArgumentOutOfRangeException">Thrown when the due time is negative.</exception>
        ///
        /// <category index="2">Awaiting Results</category>
        ///
        /// <example id="sleep-2">
        /// <code lang="fsharp">
        /// async2 {
        ///     printn "A"
        ///     do! Async2.Sleep(TimeSpan(0, 0, 1))
        ///     printn "B"
        /// } |> Async2.Start
        /// printn "C"
        /// </code>
        /// Prints "C", then "A" quickly, and then "B" 1 second later.
        /// </example>
        static member Sleep: dueTime:TimeSpan -> Async2<unit>

        /// <summary>
        ///  Creates an asynchronous computation in terms of a Begin/End pair of actions in
        ///  the style used in CLI APIs.
        /// </summary>
        ///
        /// <remarks>
        /// The computation will respond to cancellation while waiting for the completion
        /// of the operation. If a cancellation occurs, and <c>cancelAction</c> is specified, then it is
        /// executed, and the computation continues to wait for the completion of the operation.
        ///
        /// If <c>cancelAction</c> is not specified, then cancellation causes the computation
        /// to stop immediately, and subsequent invocations of the callback are ignored.</remarks>
        ///
        /// <param name="beginAction">The function initiating a traditional CLI asynchronous operation.</param>
        /// <param name="endAction">The function completing a traditional CLI asynchronous operation.</param>
        /// <param name="cancelAction">An optional function to be executed when a cancellation is requested.</param>
        ///
        /// <returns>An asynchronous computation wrapping the given Begin/End functions.</returns>
        ///
        /// <category index="5">Legacy .NET Async2 Interoperability</category>
        ///
        /// <example-tbd></example-tbd>
        static member FromBeginEnd : beginAction:(AsyncCallback * objnull -> IAsyncResult) * endAction:(IAsyncResult -> 'T) * ?cancelAction : (unit -> unit) -> Async2<'T>

        /// <summary>
        ///  Creates an asynchronous computation in terms of a Begin/End pair of actions in
        ///  the style used in .NET 2.0 APIs.
        /// </summary>
        ///
        /// <remarks>The computation will respond to cancellation while waiting for the completion
        /// of the operation. If a cancellation occurs, and <c>cancelAction</c> is specified, then it is
        /// executed, and the computation continues to wait for the completion of the operation.
        ///
        ///  If <c>cancelAction</c> is not specified, then cancellation causes the computation
        ///  to stop immediately, and subsequent invocations of the callback are ignored.
        ///</remarks>
        ///
        /// <param name="arg">The argument for the operation.</param>
        /// <param name="beginAction">The function initiating a traditional CLI asynchronous operation.</param>
        /// <param name="endAction">The function completing a traditional CLI asynchronous operation.</param>
        /// <param name="cancelAction">An optional function to be executed when a cancellation is requested.</param>
        ///
        /// <returns>An asynchronous computation wrapping the given Begin/End functions.</returns>
        ///
        /// <category index="5">Legacy .NET Async2 Interoperability</category>
        ///
        /// <example-tbd></example-tbd>
        static member FromBeginEnd : arg:'Arg1 * beginAction:('Arg1 * AsyncCallback * objnull -> IAsyncResult) * endAction:(IAsyncResult -> 'T) * ?cancelAction : (unit -> unit) -> Async2<'T>

        /// <summary>
        /// Creates an asynchronous computation in terms of a Begin/End pair of actions in
        /// the style used in .NET 2.0 APIs.</summary>
        ///
        /// <remarks>The computation will respond to cancellation while waiting for the completion
        /// of the operation. If a cancellation occurs, and <c>cancelAction</c> is specified, then it is
        /// executed, and the computation continues to wait for the completion of the operation.
        ///
        /// If <c>cancelAction</c> is not specified, then cancellation causes the computation
        /// to stop immediately, and subsequent invocations of the callback are ignored.</remarks>
        ///
        /// <param name="arg1">The first argument for the operation.</param>
        /// <param name="arg2">The second argument for the operation.</param>
        /// <param name="beginAction">The function initiating a traditional CLI asynchronous operation.</param>
        /// <param name="endAction">The function completing a traditional CLI asynchronous operation.</param>
        /// <param name="cancelAction">An optional function to be executed when a cancellation is requested.</param>
        ///
        /// <returns>An asynchronous computation wrapping the given Begin/End functions.</returns>
        ///
        /// <category index="5">Legacy .NET Async2 Interoperability</category>
        ///
        /// <example-tbd></example-tbd>
        static member FromBeginEnd : arg1:'Arg1 * arg2:'Arg2 * beginAction:('Arg1 * 'Arg2 * AsyncCallback * objnull -> IAsyncResult) * endAction:(IAsyncResult -> 'T) * ?cancelAction : (unit -> unit) -> Async2<'T>

        /// <summary>Creates an asynchronous computation in terms of a Begin/End pair of actions in
        /// the style used in .NET 2.0 APIs.</summary>
        ///
        /// <remarks>The computation will respond to cancellation while waiting for the completion
        /// of the operation. If a cancellation occurs, and <c>cancelAction</c> is specified, then it is
        /// executed, and the computation continues to wait for the completion of the operation.
        ///
        /// If <c>cancelAction</c> is not specified, then cancellation causes the computation
        /// to stop immediately, and subsequent invocations of the callback are ignored.</remarks>
        ///
        /// <param name="arg1">The first argument for the operation.</param>
        /// <param name="arg2">The second argument for the operation.</param>
        /// <param name="arg3">The third argument for the operation.</param>
        /// <param name="beginAction">The function initiating a traditional CLI asynchronous operation.</param>
        /// <param name="endAction">The function completing a traditional CLI asynchronous operation.</param>
        /// <param name="cancelAction">An optional function to be executed when a cancellation is requested.</param>
        ///
        /// <returns>An asynchronous computation wrapping the given Begin/End functions.</returns>
        ///
        /// <category index="5">Legacy .NET Async2 Interoperability</category>
        ///
        /// <example-tbd></example-tbd>
        static member FromBeginEnd : arg1:'Arg1 * arg2:'Arg2 * arg3:'Arg3 * beginAction:('Arg1 * 'Arg2 * 'Arg3 * AsyncCallback * objnull -> IAsyncResult) * endAction:(IAsyncResult -> 'T) * ?cancelAction : (unit -> unit) -> Async2<'T>

        /// <summary>Creates three functions that can be used to implement the .NET 1.0 Asynchronous
        /// Programming Model (APM) for a given asynchronous computation.</summary>
        ///
        /// <param name="computation">A function generating the asynchronous computation to split into the traditional
        /// .NET Asynchronous Programming Model.</param>
        ///
        /// <returns>A tuple of the begin, end, and cancel members.</returns>
        ///
        /// <category index="5">Legacy .NET Async2 Interoperability</category>
        ///
        /// <example-tbd></example-tbd>
        static member AsBeginEnd : computation:('Arg -> Async2<'T>) ->
                                     // The 'Begin' member
                                     ('Arg * AsyncCallback * objnull -> IAsyncResult) *
                                     // The 'End' member
                                     (IAsyncResult -> 'T) *
                                     // The 'Cancel' member
                                     (IAsyncResult -> unit)

        /// <summary>Creates an asynchronous computation that runs the given computation and ignores
        /// its result.</summary>
        ///
        /// <param name="computation">The input computation.</param>
        ///
        /// <returns>A computation that is equivalent to the input computation, but disregards the result.</returns>
        ///
        /// <category index="1">Composing Async2 Computations</category>
        ///
        /// <example id="ignore-1">
        /// <code lang="fsharp">
        /// let readFile filename numBytes =
        ///     async2 {
        ///         use file = System.IO.File.OpenRead(filename)
        ///         printfn "Reading from file %s." filename
        ///         // Throw away the data being read.
        ///         do! file.AsyncRead(numBytes) |> Async2.ignore&lt;byte[]&gt;
        ///     }
        /// readFile "example.txt" 42 |> Async2.Start
        /// </code>
        /// Reads bytes from a given file asynchronously and then ignores the result, allowing the do! to be used with functions
        /// that return an unwanted value.
        /// </example>
        static member Ignore : computation: Async2<'T> -> Async2<unit>

        /// <summary>Runs an asynchronous computation, starting immediately on the current operating system
        /// thread. Call one of the three continuations when the operation completes.</summary>
        ///
        /// <remarks>If no cancellation token is provided then the default cancellation token
        /// is used.</remarks>
        ///
        /// <param name="computation">The asynchronous computation to execute.</param>
        /// <param name="continuation">The function called on success.</param>
        /// <param name="exceptionContinuation">The function called on exception.</param>
        /// <param name="cancellationContinuation">The function called on cancellation.</param>
        /// <param name="cancellationToken">The <c>CancellationToken</c> to associate with the computation.
        /// The default is used if this parameter is not provided.</param>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example-tbd></example-tbd>
        static member StartWithContinuations:
            computation:Async2<'T> *
            continuation:('T -> unit) * exceptionContinuation:(exn -> unit) * cancellationContinuation:(OperationCanceledException -> unit) *
            ?cancellationToken:CancellationToken-> unit

        ///
        /// <example-tbd></example-tbd>
        static member internal StartWithContinuationsUsingDispatchInfo:
            computation:Async2<'T> *
            continuation:('T -> unit) * exceptionContinuation:(ExceptionDispatchInfo -> unit) * cancellationContinuation:(OperationCanceledException -> unit) *
            ?cancellationToken:CancellationToken-> unit

        /// <summary>Runs an asynchronous computation, starting immediately on the current operating system
        /// thread.</summary>
        ///
        /// <remarks>If no cancellation token is provided then the default cancellation token is used.</remarks>
        ///
        /// <param name="computation">The asynchronous computation to execute.</param>
        /// <param name="cancellationToken">The <c>CancellationToken</c> to associate with the computation.
        /// The default is used if this parameter is not provided.</param>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example id="start-immediate-1">
        /// <code lang="fsharp">
        /// printn "A"
        ///
        /// async2 {
        ///     printn "B"
        ///     do! Async2.Sleep(1000)
        ///     printn "C"
        /// } |> Async2.StartImmediate
        ///
        /// printn "D"
        /// </code>
        /// Prints "A", "B", "D" immediately, then "C" in 1 second
        /// </example>
        static member StartImmediate:
            computation:Async2<unit> * ?cancellationToken:CancellationToken-> unit

        /// <summary>Runs an asynchronous computation, starting immediately on the current operating system
        /// thread, but also returns the execution as <see cref="T:System.Threading.Tasks.Task`1"/>
        /// </summary>
        ///
        /// <remarks>If no cancellation token is provided then the default cancellation token is used.
        /// You may prefer using this method if you want to achieve a similar behavior to async2 await in C# as
        /// async2 computation starts on the current thread with an ability to return a result.
        /// </remarks>
        ///
        /// <param name="computation">The asynchronous computation to execute.</param>
        /// <param name="cancellationToken">The <c>CancellationToken</c> to associate with the computation.
        /// The default is used if this parameter is not provided.</param>
        ///
        /// <returns>A <see cref="T:System.Threading.Tasks.Task"/> that will be completed
        /// in the corresponding state once the computation terminates (produces the result, throws exception or gets canceled)</returns>
        ///
        /// <category index="0">Starting Async2 Computations</category>
        ///
        /// <example id="start-immediate-as-task-1">
        /// <code lang="fsharp">
        /// printn "A"
        ///
        /// let t =
        ///     async2 {
        ///         printn "B"
        ///         do! Async2.Sleep(1000)
        ///         printn "C"
        ///     } |> Async2.StartImmediateAsTask
        ///
        /// printn "D"
        /// t.Wait()
        /// printn "E"
        /// </code>
        /// Prints "A", "B", "D" immediately, then "C", "E" in 1 second.
        /// </example>
        static member StartImmediateAsTask:
            computation:Async2<'T> * ?cancellationToken:CancellationToken-> Task<'T>


    /// <summary>A module of extension members providing support for awaiting any task-like value via the GetAwaiter pattern.</summary>
    ///
    /// <category index="2">Awaiting Results</category>
    [<AutoOpen>]
    module Async2TaskLikeExtensions =

        type Async2 with

            /// <summary>Creates an asynchronous computation that will wait for the given task-like value to complete and return
            /// its result.</summary>
            /// <param name="task">The task-like value to await.</param>
            /// <remarks>
            /// <p>For the common case where you are running a Task within an Asynchronous Computation,
            /// see <c>StartTaskImmediate</c>, which surfaces the ambient <c>CancellationToken</c>
            /// so that it can be passed to the Task being started.</p>
            /// <p>The value must satisfy the GetAwaiter pattern: it must have a <c>GetAwaiter()</c> method
            /// returning an awaiter implementing <see cref="T:System.Runtime.CompilerServices.ICriticalNotifyCompletion"/>
            /// with <c>IsCompleted</c> and <c>GetResult()</c> members.</p>
            /// <p>Exceptions thrown by <c>GetResult()</c> are propagated directly.</p>
            /// <p>Unlike the <see cref="T:System.Threading.Tasks.Task"/>
#if NETSTANDARD2_1 || NET
            /// and <see cref="T:System.Threading.Tasks.ValueTask"/>
#endif
            /// overloads, an <see cref="T:System.AggregateException"/> carrying multiple inner exceptions is not preserved:
            /// the first inner exception surfaces (standard <c>GetResult()</c> semantics).</p>
            /// <p>This overload uses statically resolved type parameters (SRTP) so it can accept any task-like type.
#if NETSTANDARD2_1 || NET
            /// The specific overloads for <see cref="T:System.Threading.Tasks.Task`1"/>, <see cref="T:System.Threading.Tasks.Task"/>,
            /// <see cref="T:System.Threading.Tasks.ValueTask`1"/> and <see cref="T:System.Threading.Tasks.ValueTask"/>
#else
            /// The specific overloads for <see cref="T:System.Threading.Tasks.Task`1"/> and <see cref="T:System.Threading.Tasks.Task"/>
#endif
            /// are preferred when the argument type is known.</p>
            /// </remarks>
            /// <category index="2">Awaiting Results</category>
            /// <example id="await-tasklike-1">
            /// <code lang="fsharp">
            /// // A minimal custom task-like type
            /// type MyTask&lt;'T&gt;(task: System.Threading.Tasks.Task&lt;'T&gt;) =
            ///     member _.GetAwaiter() = task.GetAwaiter()
            ///
            /// let myTask = MyTask(System.Threading.Tasks.Task.FromResult 42)
            /// async2 {
            ///     let! result = Async2.Await myTask
            ///     printfn $"Result: {result}"
            /// } |> Async2.RunSynchronously
            /// </code>
            /// Prints <c>Result: 42</c>.
            /// </example>
            // NOTE Aside from being a catch-all to cover the GetAwaiter pattern,
            // On netstandard2.0, this overload also covers ValueTask and ValueTask<'T>.
            [<NoEagerConstraintApplication>]
            static member inline Await< ^TaskLike, ^Awaiter, 'T> :
                task: ^TaskLike -> Async2<'T>
                    when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
                    and ^Awaiter :> ICriticalNotifyCompletion
                    and ^Awaiter: (member get_IsCompleted: unit -> bool)
                    and ^Awaiter: (member GetResult: unit -> 'T)

            /// <summary>Creates an asynchronous computation that passes the ambient <c>Async2.CancellationToken</c> to
            /// <c>createTask</c>, and then awaits the resulting task-like value.</summary>
            ///
            /// <param name="createTask">A function that accepts a <c>CancellationToken</c> and returns a task-like value
            /// satisfying the GetAwaiter pattern.</param>
            ///
            /// <remarks>The value returned by <c>createTask</c> must satisfy the GetAwaiter pattern: it must have a
            /// <c>GetAwaiter()</c> method returning an awaiter implementing
            /// <see cref="T:System.Runtime.CompilerServices.ICriticalNotifyCompletion"/>
            /// with <c>IsCompleted</c> and <c>GetResult()</c> members.
            ///
            /// This overload uses statically resolved type parameters (SRTP) so it can accept factories returning
            /// any task-like type, including <c>YieldAwaitable</c> (from <c>Task.Yield()</c>) and
            /// <c>ConfiguredTaskAwaitable</c> (from <c>task.ConfigureAwait(false)</c>).
            /// The specific overloads for <see cref="T:System.Threading.Tasks.Task`1"/>, <see cref="T:System.Threading.Tasks.Task"/>,
            /// <see cref="T:System.Threading.Tasks.ValueTask`1"/> and <see cref="T:System.Threading.Tasks.ValueTask"/>
            /// are preferred when the factory return type is known.
            /// </remarks>
            /// <category index="0">Starting Async2 Computations</category>
            /// <example id="startTaskImmediate-tasklike-1">
            /// <code lang="fsharp">
            /// // Straightforward: factory returns Task&lt;string&gt;, which is handled by
            /// // the specific Task&lt;'T&gt; overload of StartTaskImmediate (not this one).
            /// let fetchPlain (url: string) =
            ///     Async2.StartTaskImmediate(fun ct ->
            ///         httpClient.GetStringAsync(url, ct))   // returns Task&lt;string&gt;
            ///
            /// // Adding ConfigureAwait(false) to the mix yields a ConfiguredTaskAwaitable&lt;string&gt;,
            /// // which has no specific overload — this SRTP overload handles it.
            /// let fetchConfigured (url: string) =
            ///     Async2.StartTaskImmediate(fun ct ->
            ///         httpClient.GetStringAsync(url, ct).ConfigureAwait(false))
            ///
            /// async2 {
            ///     let! html = fetchConfigured "https://example.com"
            ///     printfn $"Downloaded {html.Length} chars"
            /// } |> Async2.RunSynchronouslyImmediate
            /// </code>
            /// </example>
            [<NoEagerConstraintApplication>]
            static member inline StartTaskImmediate< ^TaskLike, ^Awaiter, 'T> :
                createTask: (CancellationToken -> ^TaskLike) -> Async2<'T>
                    when ^TaskLike: (member GetAwaiter: unit -> ^Awaiter)
                    and ^Awaiter :> ICriticalNotifyCompletion
                    and ^Awaiter: (member get_IsCompleted: unit -> bool)
                    and ^Awaiter: (member GetResult: unit -> 'T)

    /// <summary>The F# compiler emits references to this type to implement F# async2 expressions.</summary>
    ///
    /// <category index="5">Async2 Internals</category>
    [<NoEquality; NoComparison>]
    type Async2Return

    /// <summary>The F# compiler emits references to this type to implement F# async2 expressions.</summary>
    ///
    /// <category index="5">Async2 Internals</category>
    [<Struct; NoEquality; NoComparison>]
    type Async2Activation<'T> =

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        ///
        /// <example-tbd></example-tbd>
        member IsCancellationRequested: bool

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        ///
        /// <example-tbd></example-tbd>
        static member Success: Async2Activation<'T> -> result: 'T -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        ///
        /// <example-tbd></example-tbd>
        member OnSuccess: result: 'T -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <example-tbd></example-tbd>
        member OnExceptionRaised: unit -> unit

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        ///
        /// <example-tbd></example-tbd>
        member OnCancellation: unit -> Async2Return

        /// Used by MailboxProcessor
        member internal QueueContinuationWithTrampoline: 'T -> Async2Return

        /// Used by MailboxProcessor
        member internal CallContinuation: 'T -> Async2Return

    [<NoEquality; NoComparison>]
    // Internals used by MailboxProcessor
    type internal Async2Result<'T>  =
        | Ok of 'T
        | Error of ExceptionDispatchInfo
        | Canceled of OperationCanceledException

    /// <summary>Entry points for generated code</summary>
    ///
    /// <category index="5">Async2 Internals</category>
    [<Sealed>]
    module Async2Primitives =

        /// <summary>The F# compiler emits calls to this function to implement F# async2 expressions.</summary>
        ///
        /// <param name="body">The body of the async2 computation.</param>
        ///
        /// <returns>The async2 computation.</returns>
        val MakeAsync: body:(Async2Activation<'T> -> Async2Return) -> Async2<'T>

        /// <summary>The F# compiler emits calls to this function to implement constructs for F# async2 expressions.</summary>
        ///
        /// <param name="computation">The async2 computation.</param>
        /// <param name="ctxt">The async2 activation.</param>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        val Invoke: computation: Async2<'T> -> ctxt:Async2Activation<'T> -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement constructs for F# async2 expressions.</summary>
        ///
        /// <param name="ctxt">The async2 activation.</param>
        /// <param name="result1">The result of the first part of the computation.</param>
        /// <param name="part2">A function returning the second part of the computation.</param>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        val CallThenInvoke: ctxt:Async2Activation<'T> -> result1:'U -> part2:('U -> Async2<'T>) -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement the <c>let!</c> construct for F# async2 expressions.</summary>
        ///
        /// <param name="ctxt">The async2 activation.</param>
        /// <param name="part1">The first part of the computation.</param>
        /// <param name="part2">A function returning the second part of the computation.</param>
        ///
        /// <returns>An async2 activation suitable for running part1 of the asynchronous execution.</returns>
        val Bind: ctxt:Async2Activation<'T> -> part1:Async2<'U> -> part2:('U -> Async2<'T>) -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement the <c>try/finally</c> construct for F# async2 expressions.</summary>
        ///
        /// <param name="ctxt">The async2 activation.</param>
        /// <param name="computation">The computation to protect.</param>
        /// <param name="finallyFunction">The finally code.</param>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        val TryFinally: ctxt:Async2Activation<'T> -> computation: Async2<'T> -> finallyFunction: (unit -> unit) -> Async2Return

        /// <summary>The F# compiler emits calls to this function to implement the <c>try/with</c> construct for F# async2 expressions.</summary>
        ///
        /// <param name="ctxt">The async2 activation.</param>
        /// <param name="computation">The computation to protect.</param>
        /// <param name="catchFunction">The exception filter.</param>
        ///
        /// <returns>A value indicating asynchronous execution.</returns>
        val TryWith: ctxt:Async2Activation<'T> -> computation: Async2<'T> -> catchFunction: (Exception -> Async2<'T> option) -> Async2Return

        [<Sealed; AutoSerializable(false)>]
        // Internals used by MailboxProcessor
        type internal ResultCell<'T> =
            new : unit -> ResultCell<'T>
            member GetWaitHandle: unit -> WaitHandle
            member Close: unit -> unit
            interface IDisposable
            member RegisterResult: 'T * reuseThread: bool -> Async2Return
            member GrabResult: unit -> 'T
            member ResultAvailable : bool
            member AwaitResult_NoDirectCancelOrTimeout : Async2<'T>
            member TryWaitForResultSynchronously: ?timeout: int -> 'T option

        // Internals used by MailboxProcessor
        val internal CreateAsyncResultAsync : Async2Result<'T> -> Async2<'T>

    /// <summary>The type of the <c>async2</c> operator, used to build workflows for asynchronous computations.</summary>
    ///
    /// <category index="1">Async2 Programming</category>
    [<CompiledName("FSharpAsyncBuilder")>]
    [<Sealed>]
    type Async2Builder =
        /// <summary>Creates an asynchronous computation that enumerates the sequence <c>seq</c>
        /// on demand and runs <c>body</c> for each element.</summary>
        ///
        /// <remarks>A cancellation check is performed on each iteration of the loop.
        ///
        /// The existence of this method permits the use of <c>for</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="sequence">The sequence to enumerate.</param>
        /// <param name="body">A function to take an item from the sequence and create
        /// an asynchronous computation.  Can be seen as the body of the <c>for</c> expression.</param>
        ///
        /// <returns>An asynchronous computation that will enumerate the sequence and run <c>body</c>
        /// for each element.</returns>
        ///
        /// <example-tbd></example-tbd>
        member For: sequence:seq<'T> * body:('T -> Async2<unit>) -> Async2<unit>

        /// <summary>Creates an asynchronous computation that just returns <c>()</c>.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of empty <c>else</c> branches in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        /// <returns>An asynchronous computation that returns <c>()</c>.</returns>
        ///
        /// <example-tbd></example-tbd>
        member Zero : unit -> Async2<unit>

        /// <summary>Creates an asynchronous computation that first runs <c>computation1</c>
        /// and then runs <c>computation2</c>, returning the result of <c>computation2</c>.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of expression sequencing in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="computation1">The first part of the sequenced computation.</param>
        /// <param name="computation2">The second part of the sequenced computation.</param>
        ///
        /// <returns>An asynchronous computation that runs both of the computations sequentially.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline Combine : computation1:Async2<unit> * computation2:Async2<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that runs <c>computation</c> repeatedly
        /// until <c>guard()</c> becomes false.</summary>
        ///
        /// <remarks>A cancellation check is performed whenever the computation is executed.
        ///
        /// The existence of this method permits the use of <c>while</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="guard">The function to determine when to stop executing <c>computation</c>.</param>
        /// <param name="computation">The function to be executed.  Equivalent to the body
        /// of a <c>while</c> expression.</param>
        ///
        /// <returns>An asynchronous computation that behaves similarly to a while loop when run.</returns>
        ///
        /// <example-tbd></example-tbd>
        member While : guard:(unit -> bool) * computation:Async2<unit> -> Async2<unit>

        /// <summary>Creates an asynchronous computation that returns the result <c>v</c>.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of <c>return</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="value">The value to return from the computation.</param>
        ///
        /// <returns>An asynchronous computation that returns <c>value</c> when executed.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline Return : value:'T -> Async2<'T>

        /// <summary>Delegates to the input computation.</summary>
        ///
        /// <remarks>The existence of this method permits the use of <c>return!</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="computation">The input computation.</param>
        ///
        /// <returns>The input computation.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline ReturnFrom : computation:Async2<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that runs <c>generator</c>.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.</remarks>
        ///
        /// <param name="generator">The function to run.</param>
        ///
        /// <returns>An asynchronous computation that runs <c>generator</c>.</returns>
        ///
        /// <example-tbd></example-tbd>
        member Delay : generator:(unit -> Async2<'T>) -> Async2<'T>

        /// <summary>Creates an asynchronous computation that runs <c>binder(resource)</c>.
        /// The action <c>resource.Dispose()</c> is executed as this computation yields its result
        /// or if the asynchronous computation exits by an exception or by cancellation.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of <c>use</c> and <c>use!</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="resource">The resource to be used and disposed.</param>
        /// <param name="binder">The function that takes the resource and returns an asynchronous
        /// computation.</param>
        ///
        /// <returns>An asynchronous computation that binds and eventually disposes <c>resource</c>.</returns>
        ///
        /// <example-tbd></example-tbd>
        member Using: resource:'T * binder:('T -> Async2<'U>) -> Async2<'U> when 'T :> IDisposable|null

        /// <summary>Creates an asynchronous computation that runs <c>computation</c>, and when
        /// <c>computation</c> generates a result <c>T</c>, runs <c>binder res</c>.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of <c>let!</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="computation">The computation to provide an unbound result.</param>
        /// <param name="binder">The function to bind the result of <c>computation</c>.</param>
        ///
        /// <returns>An asynchronous computation that performs a monadic bind on the result
        /// of <c>computation</c>.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline Bind: computation: Async2<'T> * binder: ('T -> Async2<'U>) -> Async2<'U>

        /// <summary>Creates an asynchronous computation that runs <c>computation</c>. The action <c>compensation</c> is executed
        /// after <c>computation</c> completes, whether <c>computation</c> exits normally or by an exception. If <c>compensation</c> raises an exception itself
        /// the original exception is discarded and the new exception becomes the overall result of the computation.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of <c>try/finally</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="computation">The input computation.</param>
        /// <param name="compensation">The action to be run after <c>computation</c> completes or raises an
        /// exception (including cancellation).</param>
        ///
        /// <returns>An asynchronous computation that executes computation and compensation afterwards or
        /// when an exception is raised.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline TryFinally : computation:Async2<'T> * compensation:(unit -> unit) -> Async2<'T>

        /// <summary>Creates an asynchronous computation that runs <c>computation</c> and returns its result.
        /// If an exception happens then <c>catchHandler(exn)</c> is called and the resulting computation executed instead.</summary>
        ///
        /// <remarks>A cancellation check is performed when the computation is executed.
        ///
        /// The existence of this method permits the use of <c>try/with</c> in the
        /// <c>async2 { ... }</c> computation expression syntax.</remarks>
        ///
        /// <param name="computation">The input computation.</param>
        /// <param name="catchHandler">The function to run when <c>computation</c> throws an exception.</param>
        ///
        /// <returns>An asynchronous computation that executes <c>computation</c> and calls <c>catchHandler</c> if an
        /// exception is thrown.</returns>
        ///
        /// <example-tbd></example-tbd>
        member inline TryWith : computation:Async2<'T> * catchHandler:(exn -> Async2<'T>) -> Async2<'T>

        // member inline TryWithFilter : computation:Async2<'T> * catchHandler:(exn -> Async2<'T> option) -> Async2<'T>

        /// Generate an object used to build asynchronous computations using F# computation expressions. The value
        /// 'async2' is a pre-defined instance of this type.
        ///
        /// A cancellation check is performed when the computation is executed.
        internal new : unit -> Async2Builder

    /// <summary>A module of extension members providing asynchronous operations for some basic CLI types related to concurrency and I/O.</summary>
    ///
    /// <category index="1">Async2 Programming</category>
    [<AutoOpen>]
    module CommonExtensions =

        type System.IO.Stream with

            /// <summary>Returns an asynchronous computation that will read from the stream into the given buffer.</summary>
            /// <param name="buffer">The buffer to read into.</param>
            /// <param name="offset">An optional offset as a number of bytes in the stream.</param>
            /// <param name="count">An optional number of bytes to read from the stream.</param>
            ///
            /// <returns>An asynchronous computation that will read from the stream into the given buffer.</returns>
            ///
            /// <exception cref="T:System.ArgumentException">Thrown when the sum of offset and count is longer than
            /// the buffer length.</exception>
            /// <exception cref="T:System.ArgumentOutOfRangeException">Thrown when offset or count is negative.</exception>
            ///
            /// <example-tbd></example-tbd>
            [<CompiledName("AsyncRead")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncRead : buffer:byte array * ?offset:int * ?count:int -> Async2<int>

            /// <summary>Returns an asynchronous computation that will read the given number of bytes from the stream.</summary>
            ///
            /// <param name="count">The number of bytes to read.</param>
            ///
            /// <returns>An asynchronous computation that returns the read byte array when run.</returns>
            ///
            /// <example-tbd></example-tbd>
            [<CompiledName("AsyncReadBytes")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncRead : count:int -> Async2<byte array>

            /// <summary>Returns an asynchronous computation that will write the given bytes to the stream.</summary>
            ///
            /// <param name="buffer">The buffer to write from.</param>
            /// <param name="offset">An optional offset as a number of bytes in the stream.</param>
            /// <param name="count">An optional number of bytes to write to the stream.</param>
            ///
            /// <returns>An asynchronous computation that will write the given bytes to the stream.</returns>
            ///
            /// <exception cref="T:System.ArgumentException">Thrown when the sum of offset and count is longer than
            /// the buffer length.</exception>
            /// <exception cref="T:System.ArgumentOutOfRangeException">Thrown when offset or count is negative.</exception>
            ///
            /// <example-tbd></example-tbd>
            [<CompiledName("AsyncWrite")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncWrite : buffer:byte array * ?offset:int * ?count:int -> Async2<unit>


        ///<summary>The family of first class event values for delegate types that satisfy the F# delegate constraint.</summary>
        type IObservable<'T> with
            /// <summary>Permanently connects a listener function to the observable. The listener will
            /// be invoked for each observation.</summary>
            ///
            /// <param name="callback">The function to be called for each observation.</param>
            ///
            /// <example-tbd></example-tbd>
            [<CompiledName("AddToObservable")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member Add: callback:('T -> unit) -> unit

            /// <summary>Connects a listener function to the observable. The listener will
            /// be invoked for each observation. The listener can be removed by
            /// calling Dispose on the returned IDisposable object.</summary>
            ///
            /// <param name="callback">The function to be called for each observation.</param>
            ///
            /// <returns>An object that will remove the listener if disposed.</returns>
            ///
            /// <example-tbd></example-tbd>
            [<CompiledName("SubscribeToObservable")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member Subscribe: callback:('T -> unit) -> IDisposable

    /// <summary>A module of extension members providing asynchronous operations for some basic Web operations.</summary>
    ///
    /// <category index="1">Async2 Programming</category>
    [<AutoOpen>]
    module WebExtensions =

        type System.Net.WebRequest with
            /// <summary>Returns an asynchronous computation that, when run, will wait for a response to the given WebRequest.</summary>
            /// <returns>An asynchronous computation that waits for response to the <c>WebRequest</c>.</returns>
            ///
            /// <example id="get-response">
            /// <code lang="fsharp">
            /// open System.Net
            /// open System.IO
            /// let responseStreamToString = fun (responseStream : WebResponse) ->
            ///     let reader = new StreamReader(responseStream.GetResponseStream())
            ///     reader.ReadToEnd()
            /// let webRequest = WebRequest.Create("https://www.w3.org")
            /// let result = webRequest.AsyncGetResponse() |> Async2.RunSynchronously |> responseStreamToString
            /// </code>
            /// </example>
            /// Gets the web response asynchronously and converts response stream to string
            [<CompiledName("AsyncGetResponse")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncGetResponse : unit -> Async2<System.Net.WebResponse>

        type System.Net.WebClient with

            /// <summary>Returns an asynchronous computation that, when run, will wait for the download of the given URI.</summary>
            ///
            /// <param name="address">The URI to retrieve.</param>
            ///
            /// <returns>An asynchronous computation that will wait for the download of the URI.</returns>
            ///
            /// <example id="async2-download-string">
            /// <code lang="fsharp">
            /// open System
            /// let client = new WebClient()
            /// Uri("https://www.w3.org") |> client.AsyncDownloadString |> Async2.RunSynchronously
            /// </code>
            /// This will download the server response from https://www.w3.org
            /// </example>
            [<CompiledName("AsyncDownloadString")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncDownloadString : address:Uri -> Async2<string>

            /// <summary>Returns an asynchronous computation that, when run, will wait for the download of the given URI.</summary>
            ///
            /// <param name="address">The URI to retrieve.</param>
            ///
            /// <returns>An asynchronous computation that will wait for the download of the URI.</returns>
            ///
            /// <example id="async2-download-data">
            /// <code lang="fsharp">
            /// open System.Net
            /// open System.Text
            /// open System
            /// let client = new WebClient()
            /// client.AsyncDownloadData(Uri("https://www.w3.org")) |> Async2.RunSynchronously |> Encoding.ASCII.GetString
            /// </code>
            /// </example>
            /// Downloads the data in bytes and decodes it to a string.
            [<CompiledName("AsyncDownloadData")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncDownloadData : address:Uri -> Async2<byte array>

            /// <summary>Returns an asynchronous computation that, when run, will wait for the download of the given URI to specified file.</summary>
            ///
            /// <param name="address">The URI to retrieve.</param>
            /// <param name="fileName">The file name to save download to.</param>
            ///
            /// <returns>An asynchronous computation that will wait for the download of the URI to specified file.</returns>
            ///
            /// <example id="async2-download-file">
            /// <code lang="fsharp">
            /// open System.Net
            /// open System
            /// let client = new WebClient()
            /// Uri("https://www.w3.com") |> fun x -> client.AsyncDownloadFile(x, "output.html") |> Async2.RunSynchronously
            /// </code>
            /// This will download the server response as a file and output it as output.html
            /// </example>
            [<CompiledName("AsyncDownloadFile")>] // give the extension member a nice, unmangled compiled name, unique within this module
            member AsyncDownloadFile : address:Uri * fileName: string -> Async2<unit>

    // Internals used by MailboxProcessor
    [<AutoOpen>]
    module Async2BuilderImpl =
        val async2 : Async2Builder

    /// <summary>Contains camelCase module-level functions for <see cref="T:Microsoft.FSharp.Control.FSharpAsync2`1"/> computations.</summary>
    ///
    /// <category index="1">Async2 Programming</category>
    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module Async2 =

        /// <summary>Creates an asynchronous computation that returns the given value.</summary>
        ///
        /// <param name="value">The value to return.</param>
        ///
        /// <returns>An asynchronous computation that returns <c>value</c> when executed.</returns>
        ///
        /// <example id="async2-result-1">
        /// <code lang="fsharp">
        /// let computation = Async2.result 42
        /// computation |> Async2.RunSynchronouslyImmediate // evaluates to 42
        /// </code>
        /// </example>
        [<CompiledName("Result")>]
        val inline result: value: 'T -> Async2<'T>

        /// <summary>Creates an asynchronous computation that applies the mapping function to the result of the given computation.</summary>
        ///
        /// <param name="mapping">The function to apply to the result.</param>
        /// <param name="computation">The input computation.</param>
        ///
        /// <returns>An asynchronous computation that applies <c>mapping</c> to the result of <c>computation</c>.</returns>
        ///
        /// <example id="async2-map-1">
        /// <code lang="fsharp">
        /// let computation = Async2.result 21 |> Async2.map (fun x -> x * 2)
        /// computation |> Async2.RunSynchronouslyImmediate // evaluates to 42
        /// </code>
        /// </example>
        [<CompiledName("Map")>]
        val inline map: mapping: ('T -> 'U) -> computation: Async2<'T> -> Async2<'U>

        /// <summary>Creates an asynchronous computation that passes the result of the given computation to the binder function.</summary>
        ///
        /// <param name="binder">A function that takes the result of the computation and returns a new asynchronous computation.</param>
        /// <param name="computation">The input computation.</param>
        ///
        /// <returns>An asynchronous computation that performs a monadic bind on the result of <c>computation</c>.</returns>
        ///
        /// <example id="async2-bind-1">
        /// <code lang="fsharp">
        /// let computation = Async2.result 21 |> Async2.bind (fun x -> Async2.result (x * 2))
        /// computation |> Async2.RunSynchronouslyImmediate // evaluates to 42
        /// </code>
        /// </example>
        [<CompiledName("Bind")>]
        val inline bind: binder: ('T -> Async2<'U>) -> computation: Async2<'T> -> Async2<'U>

        /// <summary>Creates an asynchronous computation that runs the given computation and ignores its result.</summary>
        ///
        /// <param name="computation">The input computation.</param>
        ///
        /// <returns>A computation that is equivalent to the input computation, but disregards the result.</returns>
        ///
        /// <example id="async2-ignore-1">
        /// <code lang="fsharp">
        /// let readFile filename numBytes: Async2&lt;unit&gt; =
        ///     async2 {
        ///         use file = System.IO.File.OpenRead(filename)
        ///         do! file.AsyncRead(numBytes) |> Async2.ignore&lt;byte[]&gt;
        ///     }
        /// </code>
        /// </example>
        /// <example id="async2-ignore-2">
        /// <code lang="fsharp">
        /// let computation : Async2&lt;unit&gt; = Async2.result 42 |> Async2.ignore&lt;int&gt;
        /// computation |> Async2.RunSynchronously // evaluates to ()
        /// </code>
        /// </example>
        [<CompiledName("Ignore")>]
        [<RequiresExplicitTypeArguments>]
        val inline ignore<'T> : computation: Async2<'T> -> Async2<unit>

        /// <summary>Creates an asynchronous computation that yields the original result on success, or the result of
        /// <c>handler exn</c> for non-cancellation exceptions.</summary>
        /// <remarks><c>OperationCanceledException</c> and derived types such as <c>TaskCanceledException</c> propagate unchanged,
        /// and therefore are never passed to <c>handler</c>.
        /// </remarks>
        /// <param name="handler">A function to handle (non-cancellation) exceptions, yielding a recovery value based on the exception.
        /// Any exception thrown by <c>handler</c> will propagate.</param>
        /// <param name="computation">The input computation.</param>
        /// <returns>An asynchronous computation that yields the result of <c>computation</c> on success,
        /// or <c>handler exn</c> on failure.
        /// Propagates the underlying cancellation exception where cancellation occurs.</returns>
        /// <example id="async2-catchwith-1">
        /// <code lang="fsharp">
        /// let safeDiv x y =
        ///     async2 { return x / y }
        ///     |> Async2.catchWith (fun _ -> 0)
        /// safeDiv 10 0 |> Async2.RunSynchronouslyImmediate // evaluates to 0
        /// </code>
        /// </example>
        [<CompiledName("CatchWith")>]
        val catchWith: handler: (exn -> 'T) -> computation: Async2<'T> -> Async2<'T>

        /// <summary>Creates an asynchronous computation that reifies the outcome of the given <c>computation</c> as a <c>Result</c>:
        /// <c>Ok</c> on success, <c>Error</c> on failure, so exceptions become values. Cancellation still propagates.</summary>
        /// <remarks><c>OperationCanceledException</c> and derived types such as <c>TaskCanceledException</c> propagate unchanged.</remarks>
        /// <param name="computation">The input computation.</param>
        /// <returns>An asynchronous computation that yields a <c>Result</c>: <c>Ok</c> with the outcome on success,
        /// or <c>Error</c> with the exception on failure.
        /// Propagates the underlying cancellation exception when cancellation occurs.</returns>
        /// <example id="async2-catch-1">
        /// <code lang="fsharp">
        /// let safeDiv x y =
        ///     async2 { return x / y } |> Async2.catch
        /// safeDiv 10 2 |> Async2.RunSynchronouslyImmediate // evaluates to Ok 5
        /// safeDiv 10 0 |> Async2.RunSynchronouslyImmediate // evaluates to Error (DivideByZeroException ...)
        /// </code>
        /// </example>
        [<CompiledName("Catch")>]
        val catch: computation: Async2<'T> -> Async2<Result<'T, exn>>

        /// <summary>An asynchronous computation that returns <c>unit</c>. This is equivalent to <c>async2.Zero()</c>.</summary>
        ///
        /// <example id="async2-empty-1">
        /// <code lang="fsharp">
        /// Async2.empty |> Async2.RunSynchronouslyImmediate // evaluates to ()
        /// </code>
        /// </example>
        [<CompiledName("Empty")>]
        val empty: Async2<unit>

        /// <summary>Creates an asynchronous computation that executes each of the <c>computations</c> in sequence, returning <c>unit</c>.</summary>
        /// <param name="computations">A sequence of unit computations to be executed in sequence.</param>
        /// <returns>A computation that runs all inputs in sequence and returns <c>unit</c>.</returns>
        /// <example id="async2-sequentialdo-1">
        /// <code lang="fsharp">
        /// // NOTE numbers are guaranteed to be printed in order 1..10
        /// seq { for i in 1..10 -> async2 { printfn "%d" i } }
        /// |> Async2.sequentialDo
        /// |> Async2.RunSynchronouslyImmediate
        /// </code>
        /// </example>
        [<CompiledName("SequentialDo")>]
        val sequentialDo: computations: seq<Async2<unit>> -> Async2<unit>

        /// <summary>Creates an asynchronous computation that executes all the supplied asynchronous computations
        /// with concurrency limited to at most <c>maxDegreeOfParallelism</c>,
        /// and returns their results as an array in the same order as the inputs.</summary>
        /// <remarks>While the result order matches the input order, the relative start and completion order of computations is arbitrary.</remarks>
        /// <param name="maxDegreeOfParallelism">The maximum number of computations to run concurrently. Must be &gt; 0.</param>
        /// <param name="computations">A sequence of computations to be parallelized.</param>
        /// <returns>A computation that returns an array of results from the input computations in the same order they were supplied.</returns>
        ///
        /// <example id="async2-parallellimit-1">
        /// <code lang="fsharp">
        /// let results =
        ///     seq { for i in 1..10 -> async2 { return i * i } }
        ///     |> Async2.parallelLimit 3
        ///     |> Async2.RunSynchronouslyImmediate
        /// results // evaluates to [| 1; 4; 9; 16; 25; 36; 49; 64; 81; 100 |]
        /// </code>
        /// </example>
        [<CompiledName("ParallelLimit")>]
        val parallelLimit: maxDegreeOfParallelism: int -> computations: seq<Async2<'T>> -> Async2<'T[]>

        /// <summary>Creates an asynchronous computation that executes all the supplied asynchronous computations returning unit,
        /// with concurrency limited to at most <c>maxDegreeOfParallelism</c>.</summary>
        /// <remarks>The relative start and completion order of computations is arbitrary.</remarks>
        /// <param name="maxDegreeOfParallelism">The maximum number of computations to run concurrently. Must be &gt; 0.</param>
        /// <param name="computations">A sequence of unit computations to be parallelized.</param>
        ///
        /// <returns>A computation that runs all inputs with limited parallelism and returns <c>unit</c>.</returns>
        ///
        /// <example id="async2-paralleldolimit-1">
        /// <code lang="fsharp">
        /// seq { for i in 1..10 -> async2 { printfn "%d" i } } // NOTE output order can vary
        /// |> Async2.parallelDoLimit 3
        /// |> Async2.RunSynchronouslyImmediate
        /// </code>
        /// </example>
        [<CompiledName("ParallelDoLimit")>]
        val parallelDoLimit: maxDegreeOfParallelism: int -> computations: seq<Async2<unit>> -> Async2<unit>
