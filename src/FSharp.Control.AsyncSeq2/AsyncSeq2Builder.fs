namespace Microsoft.FSharp.Control

#nowarn "57"
#nowarn "1204"

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Core.CompilerServices

/// An asynchronous sequence, compatible with IAsyncEnumerable<'T>.
type AsyncSeq2<'T> = IAsyncEnumerable<'T>

/// Builds a cold asynchronous sequence whose bindings share the enumerator's cancellation token.
type AsyncSeq2Builder() =

    member inline _.Zero() = Seq.empty

    member inline _.Yield(value) = Seq.singleton value

    member inline _.Delay([<InlineIfLambda>] body: unit -> seq<'T>) = body

    member inline _.Combine(first, [<InlineIfLambda>] rest) =
        Seq.append first (Seq.delay rest)

    member inline _.For(source: seq<'U>, [<InlineIfLambda>] body: 'U -> seq<'T>) =
        Seq.collect body source

    member inline _.While([<InlineIfLambda>] guard, [<InlineIfLambda>] body) =
        RuntimeHelpers.EnumerateWhile guard (Seq.delay body)

    member inline _.TryFinally([<InlineIfLambda>] body, [<InlineIfLambda>] compensation) =
        RuntimeHelpers.EnumerateThenFinally (Seq.delay body) compensation

    member inline _.TryWith([<InlineIfLambda>] body, [<InlineIfLambda>] handler) =
        RuntimeHelpers.EnumerateTryWith (Seq.delay body) (fun _ -> 1) handler

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
            let enumerator = source.GetAsyncEnumerator(StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())

            this.Using(
                enumerator,
                fun enumerator ->
                    this.While(
                        (fun () -> AsyncHelpers.Await(enumerator.MoveNextAsync())),
                        (fun () -> body enumerator.Current)
                    )
            ))

    member inline this.YieldFrom(source: seq<'T>) =
        this.For(source, fun value -> this.Yield value)

    member inline this.YieldFrom(source: IAsyncEnumerable<'T>) =
        this.For(source, fun value -> this.Yield value)

    member inline _.Bind([<InlineIfLambda>] source: Async2BuilderSources.Started<'U>, [<InlineIfLambda>] continuation: 'U -> seq<'T>) =
        continuation (source.Invoke())

    member inline _.Bind([<InlineIfLambda>] source: Async2BuilderSources.Cold<'U>, [<InlineIfLambda>] continuation: 'U -> seq<'T>) =
        continuation (source.Invoke(StateMachineHelpers.__runtimeAsyncSequenceCancellationToken()))

    member inline _.Run([<InlineIfLambda>] recipe: unit -> seq<'T>) : AsyncSeq2<'T> =
        StateMachineHelpers.__runtimeAsyncSequence recipe

    member inline _.Source(computation: Async2<'T>) =
        Async2BuilderSources.Cold(fun ct -> computation.StartTrampolined ct |> AsyncHelpers.Await)

    member inline _.Source(computation: Async<'T>) =
        Async2BuilderSources.Cold(fun ct ->
            Async.StartImmediateAsTask(computation, cancellationToken = ct)
            |> AsyncHelpers.Await)

    member inline _.Source(source: seq<'T>) = source
    member inline _.Source(source: IAsyncEnumerable<'T>) = source
    member inline _.Source(task: Task<'T>) = Async2BuilderSources.Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: Task) = Async2BuilderSources.Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: ValueTask<'T>) = Async2BuilderSources.Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source(task: ValueTask) = Async2BuilderSources.Started(fun () -> AsyncHelpers.Await task)
    member inline _.Source([<InlineIfLambda>] createTask: CancellationToken -> Task<'T>) =
        Async2BuilderSources.Cold(fun ct -> AsyncHelpers.Await(createTask ct))

    member inline _.Source([<InlineIfLambda>] createTask: CancellationToken -> Task) =
        Async2BuilderSources.Cold(fun ct -> AsyncHelpers.Await(createTask ct))

    member inline _.Source([<InlineIfLambda>] createTask: CancellationToken -> ValueTask<'T>) =
        Async2BuilderSources.Cold(fun ct -> AsyncHelpers.Await(createTask ct))

    member inline _.Source([<InlineIfLambda>] createTask: CancellationToken -> ValueTask) =
        Async2BuilderSources.Cold(fun ct -> AsyncHelpers.Await(createTask ct))

[<AutoOpen>]
module AsyncSeq2BuilderAwaitableExtensionsHighPriority =
    type AsyncSeq2Builder with
        member inline _.Source(task: #Task<'T>) =
            Async2BuilderSources.startAwaitable task

        member inline _.Source([<InlineIfLambda>] coldAwaitable) =
            Async2BuilderSources.startAwaitable (coldAwaitable ())

        member inline _.Source([<InlineIfLambda>] cancellableAwaitable) =
            Async2BuilderSources.startCancellableAwaitable cancellableAwaitable

[<AutoOpen>]
module AsyncSeq2BuilderInstance =
    /// Builds an asynchronous sequence that can be consumed by async2 or .NET async enumeration.
    let asyncSeq2 = AsyncSeq2Builder()
