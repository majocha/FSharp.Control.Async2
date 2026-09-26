namespace Microsoft.FSharp.Control.AsyncSeq2Implementation

// note: runtime-async intrinsics are a preview compiler feature, requires '--langversion:preview'.
#nowarn "57" // Experimental library feature, requires '--langversion:preview'.
#nowarn "1204" // This construct is for use by compiled F# code and should not be used directly.

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Microsoft.FSharp.Core.CompilerServices

// the proper type from 0.4.0 onwards, see FSI file
type AsyncSeq2<'T> = IAsyncEnumerable<'T>

module AlgorithmTasklikeHelpers =

    /// A structure that looks like an Awaiter
    type Awaiter<'Awaiter, 'TResult
        when 'Awaiter :> ICriticalNotifyCompletion
        and 'Awaiter: (member get_IsCompleted: unit -> bool)
        and 'Awaiter: (member GetResult: unit -> 'TResult)> = 'Awaiter

    type Awaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> = 'Awaitable

    module Awaiter =
        let inline isCompleted (awaiter: Awaiter<_, _>) = awaiter.get_IsCompleted ()
        let inline getResult (awaiter: Awaiter<_, _>) = awaiter.GetResult()
        let inline onCompleted (awaiter: Awaiter<_, _>) continuation = awaiter.OnCompleted continuation
        let inline unsafeOnCompleted (awaiter: Awaiter<_, _>) continuation = awaiter.UnsafeOnCompleted continuation

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

open AlgorithmTasklikeHelpers

module AlgorithmSeqBuilderSources =

    // A delegate to unify dissimilar builder source types, this allows us to have no additional Bind or MergeSources overloads.
    // The delegate's invocation is inlined, so this is zero cost.
    type Started<'T> = delegate of unit -> 'T

    [<NoEagerConstraintApplication>]
    let inline startAwaitable awaitable =
        // Make sure the delegate captures only started awaitables to make MergeSources concurrent.
        let awaiter = Awaitable.getAwaiter awaitable
        Started(fun () ->
            AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter)

open AlgorithmSeqBuilderSources

type AlgorithmSeqBuilder() =

    member inline _.Zero() = Seq.empty

    member inline _.Yield(value) = Seq.singleton value

    member inline _.Delay([<InlineIfLambda>] body: unit -> seq<'T>) = body

    member inline _.Combine(first, [<InlineIfLambda>] rest) =
        Seq.append first (Seq.delay (fun () -> rest ()))

    member inline _.For(source: seq<'U>, [<InlineIfLambda>] body: 'U -> seq<'T>) =
        Seq.collect (fun value -> body value) source

    member inline _.While([<InlineIfLambda>] guard, [<InlineIfLambda>] body) =
        RuntimeHelpers.EnumerateWhile (fun () -> guard ()) (Seq.delay (fun () -> body ()))

    member inline _.TryFinally([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) =
        RuntimeHelpers.EnumerateThenFinally (Seq.delay (fun () -> body ())) (fun () -> compensation ())

    member inline _.TryFinallyAsync([<InlineIfLambda>] body, [<InlineIfLambda>] compensation: unit -> Task) =
        RuntimeHelpers.EnumerateThenFinally
            (Seq.delay (fun () -> body ()))
            (fun () -> AsyncHelpers.Await(compensation ()))

    member inline _.TryWith([<InlineIfLambda>] body, [<InlineIfLambda>] handler) =
        RuntimeHelpers.EnumerateTryWith (Seq.delay (fun () -> body ())) (fun _ -> 1) handler

    member inline _.Using(resource: 'R, [<InlineIfLambda>] body: 'R -> seq<'T>) =
        RuntimeHelpers.EnumerateThenFinally
            (Seq.delay (fun () -> body resource))
            (fun () ->
                match box resource with
                | :? IAsyncDisposable as disposable -> AsyncHelpers.Await(disposable.DisposeAsync())
                | :? IDisposable as disposable -> disposable.Dispose()
                | _ -> ())

    member inline this.For(source: IAsyncEnumerable<'U>, [<InlineIfLambda>] body: 'U -> seq<'T>) =
        Seq.delay (fun () ->
            let iterator = source.GetAsyncEnumerator(StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())

            this.Using(
                iterator,
                fun iterator ->
                    this.While((fun () -> AsyncHelpers.Await(iterator.MoveNextAsync())), (fun () -> body iterator.Current))
            ))

    member inline this.YieldFrom(source: seq<'T>) =
        this.For(source, fun value -> this.Yield value)

    member inline this.YieldFrom(source: IAsyncEnumerable<'T>) =
        this.For(source, fun value -> this.Yield value)

    member inline _.Bind([<InlineIfLambda>] await: Started<'T>, [<InlineIfLambda>] continuation: 'T -> 'U seq) =
        await.Invoke() |> continuation

    member inline _.Run([<InlineIfLambda>] recipe: unit -> seq<'T>) : IAsyncEnumerable<'T> =
        StateMachineHelpers.__runtimeAsyncSequence recipe

    member inline _.Source(task: Task<'T>) = Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: Task) = Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: ValueTask<'T>) = Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: ValueTask) = Started(fun () -> AsyncHelpers.Await task)

[<AutoOpen>]
module TaskSeqAwaitableExtensionsLowPriority =

    type AlgorithmSeqBuilder with
        member inline _.Source(awaitable: Awaitable<_, _, _>) = startAwaitable awaitable

[<AutoOpen>]
module TaskSeqAwaitableExtensionsHighPriority =

    type AlgorithmSeqBuilder with
        member inline _.Source(source: seq<'T>) = source
        member inline _.Source(source: IAsyncEnumerable<'T>) = source
        member inline _.Source(task: #Task<_>) = startAwaitable task
        member inline _.Source(computation: Async2<'T>) =
            Started(fun () ->
                computation.StartTrampolined(StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
                |> AsyncHelpers.Await)

[<AutoOpen>]
module AlgorithmSeqBuilder =
    let algorithmSeq = AlgorithmSeqBuilder()

[<AutoOpen>]
module TaskSeqDynamicBuilder =
    let taskSeqDynamic = AlgorithmSeqBuilder()
