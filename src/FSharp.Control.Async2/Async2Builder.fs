namespace Microsoft.FSharp.Control

open System
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Microsoft.FSharp.Core
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Core.LanguagePrimitives.IntrinsicOperators
open Microsoft.FSharp.Collections

module AwaitableHelpers =

    /// A structure that looks like an Awaiter
    type Awaiter<'Awaiter, 'TResult
        when 'Awaiter :> ICriticalNotifyCompletion
        and 'Awaiter: (member get_IsCompleted: unit -> bool)
        and 'Awaiter: (member GetResult: unit -> 'TResult)> = 'Awaiter

    type Awaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> = 'Awaitable

    type ColdAwaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> =
        unit -> 'Awaitable

    type CancellableAwaitable<'Awaitable, 'Awaiter, 'TResult
        when 'Awaitable: (member GetAwaiter: unit -> Awaiter<'Awaiter, 'TResult>)> =
        CancellationToken -> 'Awaitable

    module Awaiter =
        let inline isCompleted (awaiter: Awaiter<_, _>) = awaiter.get_IsCompleted ()
        let inline getResult (awaiter: Awaiter<_, _>) = awaiter.GetResult()

        let inline onCompleted (awaiter: Awaiter<_, _>) continuation =
            awaiter.OnCompleted continuation

        let inline unsafeOnCompleted (awaiter: Awaiter<_, _>) continuation =
            awaiter.UnsafeOnCompleted continuation

    module Awaitable =
        let inline getAwaiter (awaitable: Awaitable<_, _, _>) = awaitable.GetAwaiter()

open AwaitableHelpers

module Async2BuilderSources =

    // A delegate to unify dissimilar builder source types, this allows us to have no additional Bind or MergeSources overloads.
    // The delegate takes the cancellation token threaded through the builder; its invocation is inlined, so this is zero cost.
    type Started<'T> = delegate of unit -> 'T
    // We need to distinguish between hot and cold awaitables, we can pass the cancellation token only to the cold ones.
    // Ideally the signature should be CancellationToken -> Started<'T>, but the Started<_> delegates execute AsyncHelpers.Await
    // and must be inlined unconditionally into async method body.
    type Cold<'T> = delegate of CancellationToken -> 'T

    let inline startAwaitable awaitable =
        let awaiter = Awaitable.getAwaiter awaitable

        Started(fun () ->
            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

    let inline startCancellableAwaitable cancellableAwaitable =
        Cold(fun ct ->
            let awaiter =
                cancellableAwaitable ct
                |> Awaitable.getAwaiter

            if not (Awaiter.isCompleted awaiter) then
                AsyncHelpers.UnsafeAwaitAwaiter awaiter
            Awaiter.getResult awaiter
        )

open Async2BuilderSources

[<Sealed; NoEquality; NoComparison; CompiledName("FSharpAsync2`1")>]
type Async2<'T> (start: CancellationToken -> Task<'T>) =
    member _.Start = start

type Async2Code<'T> = CancellationToken -> 'T

module Async2Builder =
    let inline isAlreadyBackground () =
        isNull SynchronizationContext.Current
        && obj.ReferenceEquals(TaskScheduler.Current, TaskScheduler.Default)

type Async2Builder() =

    // The code type of the builder is `CancellationToken -> 'T`, i.e. the cancellation token is
    // passed along as state to every delayed continuation.
    member inline _.Delay([<InlineIfLambda>] generator: unit -> Async2Code<'T>) : Async2Code<'T> =
        fun ct ->
            ct.ThrowIfCancellationRequested()
            generator () ct

    member inline _.Zero() : Async2Code<unit> = fun _ -> ()

    member inline _.Return(value: 'T) : Async2Code<'T> = fun _ -> value

    member inline _.Combine
        (
            [<InlineIfLambda>] first: Async2Code<'A>,
            [<InlineIfLambda>] second: Async2Code<'T>
        ) : Async2Code<'T> =
        fun ct ->
            first ct
            |> ignore

            second ct

    member inline _.TryWith
        (
            [<InlineIfLambda>] body: Async2Code<'T>,
            [<InlineIfLambda>] handler: exn -> Async2Code<'T>
        ) : Async2Code<'T> =
        fun ct ->
            try
                body ct
            with error ->
                handler error ct

    member inline _.TryFinally
        (
            [<InlineIfLambda>] body: Async2Code<'T>,
            [<InlineIfLambda>] compensation: unit -> unit
        ) : Async2Code<'T> =
        fun ct ->
            try
                body ct
            finally
                compensation ()

    member inline _.Using
        (resource: 'T :> IDisposable | null, [<InlineIfLambda>] body: 'T -> Async2Code<'U>)
        : Async2Code<'U> =
        fun ct ->
            try
                body resource ct
            finally
                if not (isNull (box resource)) then resource.Dispose()

    member inline _.While
        (guard: unit -> bool, [<InlineIfLambda>] body: Async2Code<unit>)
        : Async2Code<unit> =
        fun ct ->
            while guard () do
                body ct

    member inline _.For
        (sequence: seq<'T>, [<InlineIfLambda>] body: 'T -> Async2Code<unit>)
        : Async2Code<unit> =
        fun ct ->
            for item in sequence do
                body item ct

    member inline _.Bind
        (
            [<InlineIfLambda>] awaited: Started<'T>,
            [<InlineIfLambda>] continuation: 'T -> Async2Code<'U>
        ) : Async2Code<'U> =
        fun ct -> continuation (awaited.Invoke()) ct

    member inline this.Bind
        (
            [<InlineIfLambda>] cancellable: Cold<'T>,
            [<InlineIfLambda>] continuation: 'T -> Async2Code<'U>
        ) : Async2Code<'U> =
        fun ct -> continuation (cancellable.Invoke ct) ct

    member inline _.ReturnFrom([<InlineIfLambda>] awaited: Started<'T>) : Async2Code<'T> =
        fun ct -> awaited.Invoke()

    member inline _.ReturnFrom
        ([<InlineIfLambda>] cancellable: Cold<'T>)
        : Async2Code<'T> =
        fun ct -> cancellable.Invoke ct

    member inline _.MergeSources
        ([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: Started<'B>)
        =
        let left = left.Invoke()
        let right = right.Invoke()
        Started(fun () -> struct (left, right))

    member inline this.MergeSources
        ([<InlineIfLambda>] left: Cold<'A>, [<InlineIfLambda>] right: Cold<'B>)
        =
        Cold(fun ct ->
            let right = __runtimeAsyncReturnValueTask (right.Invoke ct)
            let left = left.Invoke ct

            struct (left,
                    right
                    |> AsyncHelpers.Await)
        )

    member inline this.MergeSources
        ([<InlineIfLambda>] left: Started<'A>, [<InlineIfLambda>] right: Cold<'B>)
        =
        Cold(fun ct ->
            let right = right.Invoke ct
            let left = left.Invoke()
            struct (left, right)
        )

    member inline this.MergeSources
        ([<InlineIfLambda>] left: Cold<'A>, [<InlineIfLambda>] right: Started<'B>)
        =
        Cold(fun ct ->
            let left = left.Invoke ct
            let right = right.Invoke()
            struct (left, right)
        )

    member inline this.Source(computation: Async2<'T>) =
        Cold(fun ct -> computation.Start ct |> AsyncHelpers.Await)

    member inline _.Run([<InlineIfLambda>] code: Async2Code<'T>) : Async2<'T> =
        Async2(fun ct -> __runtimeAsyncReturn (code ct))

[<AutoOpen>]
module Async2BuilderAsyncDisposableExtensions =
    type Async2Builder with
        member inline _.Using
            (resource: 'T :> IAsyncDisposable | null, [<InlineIfLambda>] body: 'T -> CancellationToken -> 'U)
            : CancellationToken -> 'U =
            fun ct ->
                try
                    body resource ct
                finally
                    if not (isNull (box resource)) then
                        resource.DisposeAsync() |> AsyncHelpers.Await

        member inline this.For
            (sequence: IAsyncEnumerable<'T>, [<InlineIfLambda>] body: 'T -> CancellationToken -> unit)
            : CancellationToken -> unit =
            fun ct ->
                this.Using
                    (sequence.GetAsyncEnumerator ct,
                     fun enumerator ct ->
                         while enumerator.MoveNextAsync()
                               |> AsyncHelpers.Await do
                             body enumerator.Current ct)
                    ct

[<AutoOpen>]
module Async2BuilderAwaitableExtensions =
    type Async2Builder with
        member inline _.Source(awaitable) = startAwaitable awaitable

        member inline this.Source([<InlineIfLambda>] coldAwaitable) =
            startAwaitable (coldAwaitable ())

        member inline this.Source([<InlineIfLambda>] cancellableAwaitable) =
            startCancellableAwaitable cancellableAwaitable

[<AutoOpen>]
module Async2BuilderSourceExtensions =
    type Async2Builder with

        // Accepted sources for For
        member inline _.Source(sequence: 'T seq) = sequence
        member inline _.Source(sequence: IAsyncEnumerable<'T>) = sequence

        // Cannonical runtime async Bind sources
        member inline _.Source(task: Task<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: Task) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask<'T>) = Started(fun () -> task |> AsyncHelpers.Await)
        member inline _.Source(task: ValueTask) = Started(fun () -> task |> AsyncHelpers.Await)

        // Cold start sources
        //member inline this.Source(computation: Async<'T>) =
        //    Cold(fun ct -> Async.StartImmediateAsTask(computation, ct) |> AsyncHelpers.Await)

[<AutoOpen>]
module Async2BuilderImpl =
    let async2 = Async2Builder()

