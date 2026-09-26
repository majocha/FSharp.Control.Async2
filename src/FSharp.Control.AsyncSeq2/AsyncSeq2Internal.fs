namespace Microsoft.FSharp.Control.AsyncSeq2Implementation

open System
open Microsoft.FSharp.Control
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Core.CompilerServices
open System.Runtime.CompilerServices

#nowarn "57"
#nowarn "1204"

[<Struct>]
type internal AsyncEnumStatus =
    | BeforeAll
    | WithCurrent
    | AfterAll

[<Struct>]
type internal TakeOrSkipKind =
    /// use the Seq.take semantics, raises exception if not enough elements
    | Take
    /// use the Seq.skip semantics, raises exception if not enough elements
    | Skip
    /// use the Seq.truncate semantics, safe operation, returns all if count exceeds the seq
    | Truncate
    /// no Seq equiv, but like Stream.drop in Scala: safe operation, return empty if not enough elements
    | Drop

[<Struct>]
type internal Action<'T, 'U, 'TaskU when 'TaskU :> Task<'U>> =
    | CountableAction of countable_action: (int -> 'T -> 'U)
    | SimpleAction of simple_action: ('T -> 'U)
    | AsyncCountableAction of async_countable_action: (int -> 'T -> 'TaskU)
    | AsyncSimpleAction of async_simple_action: ('T -> 'TaskU)

[<Struct>]
type internal FolderAction<'T, 'State, 'TaskState when 'TaskState :> Task<'State>> =
    | FolderAction of state_action: ('State -> 'T -> 'State)
    | AsyncFolderAction of async_state_action: ('State -> 'T -> 'TaskState)

[<Struct>]
type internal ChooserAction<'T, 'U, 'TaskOption when 'TaskOption :> Task<'U option>> =
    | TryPick of try_pick: ('T -> 'U option)
    | TryPickAsync of async_try_pick: ('T -> 'TaskOption)

[<Struct>]
type internal ChooserVAction<'T, 'U, 'TaskValueOption when 'TaskValueOption :> Task<'U voption>> =
    | TryPickV of try_pickv: ('T -> 'U voption)
    | TryPickVAsync of async_try_pickv: ('T -> 'TaskValueOption)

[<Struct>]
type internal PredicateAction<'T, 'TaskBool when 'TaskBool :> Task<bool>> =
    | Predicate of try_filter: ('T -> bool)
    | PredicateAsync of async_try_filter: ('T -> 'TaskBool)

[<Struct>]
type internal InitAction<'T, 'TaskT when 'TaskT :> Task<'T>> =
    | InitAction of init_item: (int -> 'T)
    | InitActionAsync of async_init_item: (int -> 'TaskT)

[<Struct>]
type internal ProjectorAction<'T, 'Key, 'TaskKey when 'TaskKey :> Task<'Key>> =
    | ProjectorAction of projector: ('T -> 'Key)
    | AsyncProjectorAction of async_projector: ('T -> 'TaskKey)

[<Struct>]
type internal MapFolderAction<'T, 'State, 'Result, 'TaskResultState when 'TaskResultState :> Task<'Result * 'State>> =
    | MapFolderAction of map_folder_action: ('State -> 'T -> 'Result * 'State)
    | AsyncMapFolderAction of async_map_folder_action: ('State -> 'T -> 'TaskResultState)

[<Struct>]
type internal ManyOrOne<'T> =
    | Many of source_seq: AsyncSeq2<'T>
    | One of source_item: 'T

module internal AsyncSeq2Internal =
    /// Raise an NRE for arguments that are null. Only used for 'source' parameters, never for function parameters.
    let inline checkNonNull argName arg =
        if isNull arg then
            nullArg argName

    let inline raiseEmptySeq () = invalidArg "source" "The input task sequence was empty."

    /// Moves the enumerator to its first element, assuming it has just been allocated.
    /// Raises "The input sequence was empty" if there was no first element.
    let inline moveFirstOrRaiseUnsafe (e: IAsyncEnumerator<_>) = FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
        let hasFirst = AsyncHelpers.Await (e.MoveNextAsync())

        if not hasFirst then
            invalidArg "source" "The input task sequence was empty."
    )

    /// Tests the given integer value and raises if it is -1 or lower.
    let inline raiseCannotBeNegative name value =
        if value >= 0 then
            ()
        else
            invalidArg name $"The value must be non-negative, but was {value}."

    let inline raiseOutOfBounds name =
        invalidArg name "The value or index must be within the bounds of the task sequence."

    let inline raiseInsufficient () =
        // this is correct, it is NOT an InvalidOperationException (see Seq.fs in F# Core)
        // but instead, it's an ArgumentException... FWIW lol
        invalidArg "source" "The input task sequence was has an insufficient number of elements."

    let inline raiseNotFound () =
        KeyNotFoundException("The predicate function or index did not satisfy any item in the task sequence.")
        |> raise

    let isEmpty (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                let step = AsyncHelpers.Await (e.MoveNextAsync())
                not step
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let empty<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator _ =
                { new IAsyncEnumerator<'T> with
                    member _.MoveNextAsync() = ValueTask.False
                    member _.Current = Unchecked.defaultof<'T>
                    member _.DisposeAsync() = ValueTask.CompletedTask
                }
        }

    let singleton (value: 'T) =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator _ =
                let mutable status = BeforeAll

                { new IAsyncEnumerator<'T> with
                    member _.MoveNextAsync() =
                        match status with
                        | BeforeAll ->
                            status <- WithCurrent
                            ValueTask.True
                        | WithCurrent ->
                            status <- AfterAll
                            ValueTask.False
                        | AfterAll -> ValueTask.False

                    member _.Current: 'T =
                        match status with
                        | WithCurrent -> value
                        | _ -> Unchecked.defaultof<'T>

                    member _.DisposeAsync() = ValueTask.CompletedTask
                }
        }

    let replicate count value =
        raiseCannotBeNegative (nameof count) count

        algorithmSeq {
            for _ in 1..count do
                yield value
        }

    let replicateInfinite value = algorithmSeq {
        while true do
            yield value
    }

    let replicateInfiniteAsync (computation: unit -> #Task<'T>) = algorithmSeq {
        while true do
            let! value = computation ()
            yield value
    }

    let replicateUntilNoneAsync (computation: unit -> #Task<'T option>) = algorithmSeq {
        let mutable go = true

        while go do
            let! result = computation ()

            match result with
            | Some value -> yield value
            | None -> go <- false
    }

    /// Returns length unconditionally, or based on a predicate
    let lengthBy predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let mutable i = 0

                match predicate with
                | None ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        i <- i + 1

                | Some(Predicate predicate) ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        if predicate e.Current then
                            i <- i + 1

                | Some(PredicateAsync predicate) ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        match AsyncHelpers.Await (predicate e.Current) with
                        | true -> i <- i + 1
                        | false -> ()

                i
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    /// Returns length unconditionally, or based on a predicate
    let lengthBeforeMax max (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let mutable i = 0
                let mutable go = true

                while go && i < max do
                    let hasMore = AsyncHelpers.Await (e.MoveNextAsync())

                    if hasMore then i <- i + 1 else go <- false

                i
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let inline maxMin ([<InlineIfLambda>] maxOrMin) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                AsyncHelpers.Await (moveFirstOrRaiseUnsafe e)

                let mutable acc = e.Current

                while AsyncHelpers.Await (e.MoveNextAsync()) do
                    acc <- maxOrMin e.Current acc

                acc
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let inline tryMaxMin ([<InlineIfLambda>] maxOrMin) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let hasFirst = AsyncHelpers.Await (e.MoveNextAsync())

                if not hasFirst then
                    None
                else
                    let mutable acc = e.Current

                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        acc <- maxOrMin e.Current acc

                    Some acc
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    // 'compare' is either `<` or `>` (i.e, less-than, greater-than resp.)
    let inline maxMinBy ([<InlineIfLambda>] compare) ([<InlineIfLambda>] projection) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                AsyncHelpers.Await (moveFirstOrRaiseUnsafe e)
                let value = e.Current
                let mutable accProjection = projection value
                let mutable accValue = value

                while AsyncHelpers.Await (e.MoveNextAsync()) do
                    let value = e.Current
                    let currentProjection = projection value

                    if compare accProjection currentProjection then
                        accProjection <- currentProjection
                        accValue <- value

                accValue
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryExactlyOne (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                match AsyncHelpers.Await (e.MoveNextAsync()) with
                | true ->
                    let current = e.Current
                    match AsyncHelpers.Await (e.MoveNextAsync()) with
                    | true -> None
                    | false -> Some current
                | false -> None
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )


    let init count initializer = algorithmSeq {
        let mutable i = 0

        let count =
            match count with
            | Some c ->
                raiseCannotBeNegative (nameof count) c
                c

            | None -> Int32.MaxValue

        match initializer with
        | InitAction init ->
            while i < count do
                yield init i
                i <- i + 1

        | InitActionAsync asyncInit ->
            while i < count do
                let! result = asyncInit i
                yield result
                i <- i + 1

    }

    let unfold generator state = algorithmSeq {
        let mutable go = true
        let mutable currentState = state

        while go do
            match generator currentState with
            | None -> go <- false
            | Some(value, nextState) ->
                yield value
                currentState <- nextState
    }

    let unfoldAsync generator state = algorithmSeq {
        let mutable go = true
        let mutable currentState = state

        while go do
            let! result = (generator currentState: Task<_>)

            match result with
            | None -> go <- false
            | Some(value, nextState) ->
                yield value
                currentState <- nextState
    }

    let iter action (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                // Each branch keeps its own while! loop so the match dispatch is hoisted out and
                // the JIT sees a tight, single-case loop (same pattern as sum/sumBy etc.).
                match action with
                | CountableAction action ->
                    let mutable i = 0

                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        action i e.Current
                        i <- i + 1

                | SimpleAction action ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        action e.Current

                | AsyncCountableAction action ->
                    let mutable i = 0

                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        AsyncHelpers.Await (action i e.Current)
                        i <- i + 1

                | AsyncSimpleAction action ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        AsyncHelpers.Await (action e.Current)
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let fold folder initial (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let mutable result = initial

                match folder with
                | FolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        result <- folder result e.Current

                | AsyncFolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let tempResult = AsyncHelpers.Await (folder result e.Current : Task<_>)
                        result <- tempResult

                result
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let foldWhile predicate folder initial (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let mutable result = initial
                let mutable running = true

                while running do
                    let hasNext = AsyncHelpers.Await (e.MoveNextAsync())

                    if hasNext then
                        if predicate result e.Current then
                            result <- folder result e.Current
                        else
                            running <- false
                    else
                        running <- false

                result
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let scan folder initial (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        match folder with
        | FolderAction folder -> algorithmSeq {
            let mutable state = initial
            yield state

            for item in source do
                state <- folder state item
                yield state
          }

        | AsyncFolderAction folder -> algorithmSeq {
            let mutable state = initial
            yield state

            for item in source do
                let! newState = folder state item
                state <- newState
                yield state
          }

    let reduce folder (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let hasFirst = AsyncHelpers.Await (e.MoveNextAsync())

                if not hasFirst then
                    raiseEmptySeq ()

                let mutable result = e.Current

                match folder with
                | FolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        result <- folder result e.Current

                | AsyncFolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let tempResult = AsyncHelpers.Await (folder result e.Current : Task<_>)
                        result <- tempResult

                result
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let mapFold (folder: MapFolderAction<_, _, _, _>) initial (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let mutable state = initial
                let results = ResizeArray()

                match folder with
                | MapFolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let result, newState = folder state e.Current
                        results.Add result
                        state <- newState

                | AsyncMapFolderAction folder ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let (result, newState) = AsyncHelpers.Await (folder state e.Current : Task<_>)
                        results.Add result
                        state <- newState

                results.ToArray(), state
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let threadState (folder: 'State -> 'T -> 'U * 'State) initial (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        checkNonNull (nameof source) source

        algorithmSeq {
            let mutable state = initial

            for item in source do
                let result, newState = folder state item
                state <- newState
                yield result
        }

    let threadStateAsync (folder: 'State -> 'T -> #Task<'U * 'State>) initial (source: AsyncSeq2<'T>) : AsyncSeq2<'U> =
        checkNonNull (nameof source) source

        algorithmSeq {
            let mutable state = initial

            for item in source do
                let! (result, newState) = folder state item
                state <- newState
                yield result
        }

    let toResizeArrayAsync (source: AsyncSeq2<'T>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let res = ResizeArray<'T>()
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                while AsyncHelpers.Await (e.MoveNextAsync()) do
                    res.Add e.Current

                res
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let toResizeArrayAndMapAsync mapper source = (toResizeArrayAsync >> Task.map mapper) source

    let map mapper (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        match mapper with
        | CountableAction mapper -> algorithmSeq {
            let mutable i = 0

            for c in source do
                yield mapper i c
                i <- i + 1
          }

        | SimpleAction mapper -> algorithmSeq {
            for c in source do
                yield mapper c
          }

        | AsyncCountableAction mapper -> algorithmSeq {
            let mutable i = 0

            for c in source do
                let! result = mapper i c
                yield result
                i <- i + 1
          }

        | AsyncSimpleAction mapper -> algorithmSeq {
            for c in source do
                let! result = mapper c
                yield result
          }

    let zip (source1: AsyncSeq2<_>) (source2: AsyncSeq2<_>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            go <- step1 && step2

            while go do
                yield e1.Current, e2.Current
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                go <- step1 && step2
        }

    let zip3 (source1: AsyncSeq2<_>) (source2: AsyncSeq2<_>) (source3: AsyncSeq2<_>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2
        checkNonNull (nameof source3) source3

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e3 = source3.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            let! step3 = e3.MoveNextAsync()
            go <- step1 && step2 && step3

            while go do
                yield e1.Current, e2.Current, e3.Current
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                let! step3 = e3.MoveNextAsync()
                go <- step1 && step2 && step3
        }

    let zipWith (mapping: 'T -> 'U -> 'V) (source1: AsyncSeq2<'T>) (source2: AsyncSeq2<'U>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            go <- step1 && step2

            while go do
                yield mapping e1.Current e2.Current
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                go <- step1 && step2
        }

    let zipWithAsync (mapping: 'T -> 'U -> #Task<'V>) (source1: AsyncSeq2<'T>) (source2: AsyncSeq2<'U>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            go <- step1 && step2

            while go do
                let! result = mapping e1.Current e2.Current
                yield result
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                go <- step1 && step2
        }

    let zipWith3 (mapping: 'T1 -> 'T2 -> 'T3 -> 'V) (source1: AsyncSeq2<'T1>) (source2: AsyncSeq2<'T2>) (source3: AsyncSeq2<'T3>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2
        checkNonNull (nameof source3) source3

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e3 = source3.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            let! step3 = e3.MoveNextAsync()
            go <- step1 && step2 && step3

            while go do
                yield mapping e1.Current e2.Current e3.Current
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                let! step3 = e3.MoveNextAsync()
                go <- step1 && step2 && step3
        }

    let zipWithAsync3 (mapping: 'T1 -> 'T2 -> 'T3 -> #Task<'V>) (source1: AsyncSeq2<'T1>) (source2: AsyncSeq2<'T2>) (source3: AsyncSeq2<'T3>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2
        checkNonNull (nameof source3) source3

        algorithmSeq {
            use e1 = source1.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e2 = source2.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            use e3 = source3.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let mutable go = true
            let! step1 = e1.MoveNextAsync()
            let! step2 = e2.MoveNextAsync()
            let! step3 = e3.MoveNextAsync()
            go <- step1 && step2 && step3

            while go do
                let! result = mapping e1.Current e2.Current e3.Current
                yield result
                let! step1 = e1.MoveNextAsync()
                let! step2 = e2.MoveNextAsync()
                let! step3 = e3.MoveNextAsync()
                go <- step1 && step2 && step3
        }

    let compareWith (comparer: 'T -> 'T -> int) (source1: AsyncSeq2<'T>) (source2: AsyncSeq2<'T>) =
        checkNonNull (nameof source1) source1
        checkNonNull (nameof source2) source2

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e1 = source1.GetAsyncEnumerator CancellationToken.None
            let e2 = source2.GetAsyncEnumerator CancellationToken.None

            try
                let mutable result = 0
                let step1 = AsyncHelpers.Await (e1.MoveNextAsync())
                let step2 = AsyncHelpers.Await (e2.MoveNextAsync())
                let mutable has1 = step1
                let mutable has2 = step2

                while result = 0 && (has1 || has2) do
                    match has1, has2 with
                    | false, _ -> result <- -1 // source1 is shorter: less than
                    | _, false -> result <- 1 // source2 is shorter: greater than
                    | true, true ->
                        let cmp = comparer e1.Current e2.Current

                        if cmp <> 0 then
                            result <- cmp
                        else
                            let s1 = AsyncHelpers.Await (e1.MoveNextAsync())
                            let s2 = AsyncHelpers.Await (e2.MoveNextAsync())
                            has1 <- s1
                            has2 <- s2

                result
            finally
                AsyncHelpers.Await (e1.DisposeAsync())
                AsyncHelpers.Await (e2.DisposeAsync())
        )

    let collect (binder: _ -> #IAsyncEnumerable<_>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            for c in source do
                yield! binder c :> IAsyncEnumerable<_>
        }

    let collectSeq (binder: _ -> #seq<_>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            for c in source do
                yield! binder c :> seq<_>
        }

    let collectAsync (binder: _ -> #Task<#IAsyncEnumerable<_>>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            for c in source do
                let! result = binder c
                yield! result :> IAsyncEnumerable<_>
        }

    let collectSeqAsync (binder: _ -> #Task<#seq<_>>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            for c in source do
                let! result = binder c
                yield! result :> seq<_>
        }

    let tryLast (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                let mutable last = ValueNone
                while AsyncHelpers.Await (e.MoveNextAsync()) do
                    last <- ValueSome e.Current
                match last with
                | ValueSome value -> Some value
                | ValueNone -> None
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryHead (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                match AsyncHelpers.Await (e.MoveNextAsync()) with
                | true -> Some e.Current
                | false -> None
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryTail (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            let next =
                try AsyncHelpers.Await (e.MoveNextAsync())
                with error ->
                    AsyncHelpers.Await (e.DisposeAsync())
                    raise error
            if next then
                Some (algorithmSeq {
                    use e = e
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        yield e.Current
                })
            else
                AsyncHelpers.Await (e.DisposeAsync())
                None
        )

    let firstOrDefault defaultValue source =
        tryHead source
        |> Task.map (Option.defaultValue defaultValue)

    let lastOrDefault defaultValue source =
        tryLast source
        |> Task.map (Option.defaultValue defaultValue)

    let splitAt count (source: AsyncSeq2<'T>) =
        checkNonNull (nameof source) source

        if count < 0 then
            invalidArg (nameof count) $"The value must be non-negative, but was {count}."

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            let first = ResizeArray<'T>(count)
            let mutable i = 0
            let mutable go = true
            try
                while go && i < count do
                    let step = AsyncHelpers.Await (e.MoveNextAsync())
                    if step then
                        first.Add e.Current
                        i <- i + 1
                    else
                        go <- false

            with error ->
                AsyncHelpers.Await (e.DisposeAsync())
                raise error
            if go then
                let rest = algorithmSeq {
                    use e = e
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        yield e.Current
                }
                first.ToArray(), rest
            else
                AsyncHelpers.Await (e.DisposeAsync())
                first.ToArray(), empty<'T>
        )

    let tryItem index (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            if index < 0 then
                None
            else
                let e = source.GetAsyncEnumerator CancellationToken.None
                try
                    let mutable go = true
                    let mutable idx = 0
                    let mutable foundItem = None
                    let step = AsyncHelpers.Await (e.MoveNextAsync())
                    go <- step

                    while go && idx < index do
                        let step = AsyncHelpers.Await (e.MoveNextAsync())
                        go <- step
                        idx <- idx + 1

                    if go then foundItem <- Some e.Current
                    foundItem
                finally
                    AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryPick chooser (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                let mutable go = AsyncHelpers.Await (e.MoveNextAsync())
                let mutable foundItem = None
                match chooser with
                | TryPick picker ->
                    while go do
                        match picker e.Current with
                        | Some value ->
                            foundItem <- Some value
                            go <- false
                        | None -> go <- AsyncHelpers.Await (e.MoveNextAsync())
                | TryPickAsync picker ->
                    while go do
                        match AsyncHelpers.Await (picker e.Current) with
                        | Some value ->
                            foundItem <- Some value
                            go <- false
                        | None -> go <- AsyncHelpers.Await (e.MoveNextAsync())
                foundItem
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryFind predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                let mutable go = AsyncHelpers.Await (e.MoveNextAsync())
                let mutable foundItem = None
                match predicate with
                | Predicate predicate ->
                    while go do
                        let current = e.Current
                        if predicate current then
                            foundItem <- Some current
                            go <- false
                        else go <- AsyncHelpers.Await (e.MoveNextAsync())
                | PredicateAsync predicate ->
                    while go do
                        let current = e.Current
                        if AsyncHelpers.Await (predicate current) then
                            foundItem <- Some current
                            go <- false
                        else go <- AsyncHelpers.Await (e.MoveNextAsync())
                foundItem
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let tryFindIndex predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None
            try
                let mutable go = AsyncHelpers.Await (e.MoveNextAsync())
                let mutable isFound = false
                let mutable index = -1
                match predicate with
                | Predicate predicate ->
                    while go && not isFound do
                        index <- index + 1
                        isFound <- predicate e.Current
                        if not isFound then go <- AsyncHelpers.Await (e.MoveNextAsync())
                | PredicateAsync predicate ->
                    while go && not isFound do
                        index <- index + 1
                        isFound <- AsyncHelpers.Await (predicate e.Current)
                        if not isFound then go <- AsyncHelpers.Await (e.MoveNextAsync())
                if isFound then Some index else None
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let choose chooser (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {

            match chooser with
            | TryPick picker ->
                for item in source do
                    match picker item with
                    | Some value -> yield value
                    | None -> ()

            | TryPickAsync picker ->
                for item in source do
                    match! picker item with
                    | Some value -> yield value
                    | None -> ()
        }

    let chooseV chooser (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {

            match chooser with
            | TryPickV picker ->
                for item in source do
                    match picker item with
                    | ValueSome value -> yield value
                    | ValueNone -> ()

            | TryPickVAsync picker ->
                for item in source do
                    match! picker item with
                    | ValueSome value -> yield value
                    | ValueNone -> ()
        }

    let filter predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            match predicate with
            | Predicate syncPredicate ->
                for item in source do
                    if syncPredicate item then
                        yield item

            | PredicateAsync asyncPredicate ->
                for item in source do
                    match! asyncPredicate item with
                    | true -> yield item
                    | false -> ()
        }

    let distinct (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            // only create hashset when we start iterating; sequential so plain HashSet suffices
            let seen = HashSet<_>(HashIdentity.Structural)

            for item in source do
                if seen.Add item then
                    yield item
        }

    let distinctBy (projection: _ -> _) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            let seen = HashSet<_>(HashIdentity.Structural)

            for item in source do
                if seen.Add(projection item) then
                    yield item
        }

    let distinctByAsync (projection: _ -> #Task<_>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            let seen = HashSet<_>(HashIdentity.Structural)

            for item in source do
                let! key = projection item

                if seen.Add key then
                    yield item
        }

    let skipOrTake skipOrTake count (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source
        raiseCannotBeNegative (nameof count) count

        match skipOrTake with
        | Skip ->
            // don't create a new sequence if count = 0
            if count = 0 then
                source
            else
                algorithmSeq {
                    use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())

                    for _ in 1..count do
                        let! hasMore = e.MoveNextAsync()

                        if not hasMore then
                            raiseInsufficient ()

                    while! e.MoveNextAsync() do
                        yield e.Current

                }
        | Drop ->
            // don't create a new sequence if count = 0
            if count = 0 then
                source
            else
                algorithmSeq {
                    use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
                    let mutable i = 0
                    let mutable cont = true

                    // advance past 'count' elements; stop early if the source is shorter
                    while cont && i < count do
                        let! hasMore = e.MoveNextAsync()
                        if hasMore then i <- i + 1 else cont <- false

                    // return remaining elements; enumerator is at element (count-1) so one
                    // more MoveNext is needed to reach element (count)
                    if cont then
                        while! e.MoveNextAsync() do
                            yield e.Current

                }
        | Take ->
            // don't initialize an empty task sequence
            if count = 0 then
                empty
            else
                algorithmSeq {
                    use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())

                    for _ in count .. -1 .. 1 do
                        let! step = e.MoveNextAsync()

                        if not step then
                            raiseInsufficient ()

                        yield e.Current
                }

        | Truncate ->
            // don't create a new sequence if count = 0
            if count = 0 then
                empty
            else
                algorithmSeq {
                    use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
                    let mutable yielded = 0
                    let mutable cont = true

                    // yield up to 'count' elements; stop when exhausted or limit reached
                    while cont && yielded < count do
                        let! hasMore = e.MoveNextAsync()

                        if hasMore then
                            yield e.Current
                            yielded <- yielded + 1
                        else
                            cont <- false

                }

    let takeWhile isInclusive predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! notEmpty = e.MoveNextAsync()
            let mutable hasMore = notEmpty

            match predicate with
            | Predicate synchronousPredicate ->
                while hasMore && synchronousPredicate e.Current do
                    yield e.Current
                    let! cont = e.MoveNextAsync()
                    hasMore <- cont

            | PredicateAsync asyncPredicate ->
                let mutable predicateHolds = true

                while hasMore && predicateHolds do // TODO: check perf if `while!` is going to be better or equal
                    let! predicateIsTrue = asyncPredicate e.Current

                    if predicateIsTrue then
                        yield e.Current
                        let! cont = e.MoveNextAsync()
                        hasMore <- cont

                    predicateHolds <- predicateIsTrue

            // "inclusive" means: always return the item that we pulled, regardless of the result of applying the predicate
            // and only stop thereafter. The non-inclusive versions, in contrast, do not return the item under which the predicate is false.
            if hasMore && isInclusive then
                yield e.Current
        }

    let skipWhile isInclusive predicate (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! notEmpty = e.MoveNextAsync()
            let mutable hasMore = notEmpty

            match predicate with
            | Predicate synchronousPredicate ->
                while hasMore && synchronousPredicate e.Current do
                    // keep skipping
                    let! cont = e.MoveNextAsync()
                    hasMore <- cont

            | PredicateAsync asyncPredicate ->
                let mutable predicateHolds = true

                while hasMore && predicateHolds do // TODO: check perf if `while!` is going to be better or equal
                    let! predicateIsTrue = asyncPredicate e.Current

                    if predicateIsTrue then
                        // keep skipping
                        let! cont = e.MoveNextAsync()
                        hasMore <- cont

                    predicateHolds <- predicateIsTrue

            // "inclusive" means: always skip the item that we pulled, regardless of the result of applying the predicate
            // and only stop thereafter. The non-inclusive versions, in contrast, do not skip the item under which the predicate is false.
            if hasMore && not isInclusive then
                yield e.Current // don't skip, unless inclusive

            // propagate the rest
            while! e.MoveNextAsync() do
                yield e.Current
        }

    /// InsertAt or InsertManyAt
    let insertAt index valueOrValues (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        match valueOrValues with
        | Many values -> checkNonNull "values" values
        | One _ -> ()

        raiseCannotBeNegative (nameof index) index

        algorithmSeq {
            let mutable i = 0

            for item in source do
                if i = index then
                    match valueOrValues with
                    | Many values -> yield! values
                    | One value -> yield value

                yield item
                i <- i + 1

            // allow inserting at the end
            if i = index then
                match valueOrValues with
                | Many values -> yield! values
                | One value -> yield value

            if i < index then
                raiseOutOfBounds (nameof index)
        }

    let removeAt index (source: AsyncSeq2<'T>) =
        checkNonNull (nameof source) source
        raiseCannotBeNegative (nameof index) index

        algorithmSeq {
            let mutable i = 0

            for item in source do
                if i <> index then
                    yield item

                i <- i + 1

            // cannot remove past end of sequence
            if i <= index then
                raiseOutOfBounds (nameof index)
        }

    let removeManyAt index count (source: AsyncSeq2<'T>) =
        checkNonNull (nameof source) source
        raiseCannotBeNegative (nameof index) index

        algorithmSeq {
            let mutable i = 0
            let indexEnd = index + count

            for item in source do
                if i < index || i >= indexEnd then
                    yield item

                i <- i + 1

            // cannot remove past end of sequence
            if i <= index then
                raiseOutOfBounds (nameof index)
        }

    let updateAt index value (source: AsyncSeq2<'T>) =
        checkNonNull (nameof source) source
        raiseCannotBeNegative (nameof index) index

        algorithmSeq {
            let mutable i = 0

            for item in source do
                if i <> index then // most common scenario on top (cpu prediction)
                    yield item
                else
                    yield value

                i <- i + 1

            // cannot update past end of sequence
            if i <= index then
                raiseOutOfBounds (nameof index)
        }

    let except (itemsToExclude: AsyncSeq2<_>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source
        checkNonNull (nameof itemsToExclude) itemsToExclude

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                // only create hashset by the time we actually start iterating;
                // algorithmSeq enumerates sequentially, so a plain HashSet suffices — no locking needed.
                let hashSet = HashSet<_>(HashIdentity.Structural)

                use excl = itemsToExclude.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())

                while! excl.MoveNextAsync() do
                    hashSet.Add excl.Current |> ignore

                // if true, it was added, and therefore unique, so we return it
                // if false, it existed, and therefore a duplicate, and we skip
                if hashSet.Add e.Current then
                    yield e.Current

                while! e.MoveNextAsync() do
                    let current = e.Current

                    if hashSet.Add current then
                        yield current

        }

    let exceptOfSeq itemsToExclude (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source
        checkNonNull (nameof itemsToExclude) itemsToExclude

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                // only create hashset by the time we actually start iterating;
                // initialize directly from the seq — algorithmSeq is sequential so no locking needed.
                let hashSet = HashSet<_>(itemsToExclude, HashIdentity.Structural)

                // if true, it was added, and therefore unique, so we return it
                // if false, it existed, and therefore a duplicate, and we skip
                if hashSet.Add e.Current then
                    yield e.Current

                while! e.MoveNextAsync() do
                    let current = e.Current

                    if hashSet.Add current then
                        yield current

        }

    let distinctUntilChanged (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                let mutable previous = e.Current
                yield previous

                while! e.MoveNextAsync() do
                    let current = e.Current

                    if current <> previous then
                        yield current
                        previous <- current
        }

    let distinctUntilChangedWith (comparer: 'T -> 'T -> bool) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                let mutable previous = e.Current
                yield previous

                while! e.MoveNextAsync() do
                    let current = e.Current

                    if not (comparer previous current) then
                        yield current
                        previous <- current
        }

    let distinctUntilChangedWithAsync (comparer: 'T -> 'T -> #Task<bool>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                let mutable previous = e.Current
                yield previous

                while! e.MoveNextAsync() do
                    let current = e.Current
                    let! areEqual = comparer previous current

                    if not areEqual then
                        yield current
                        previous <- current
        }

    let pairwise (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        algorithmSeq {
            use e = source.GetAsyncEnumerator(FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncSequenceCancellationToken())
            let! hasFirst = e.MoveNextAsync()

            if hasFirst then
                let mutable previous = e.Current

                while! e.MoveNextAsync() do
                    let current = e.Current
                    yield previous, current
                    previous <- current
        }

    let groupBy (projector: ProjectorAction<'T, 'Key, _>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let groups = Dictionary<'Key, ResizeArray<'T>>(HashIdentity.Structural)
                let order = ResizeArray<'Key>()

                match projector with
                | ProjectorAction proj ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let key = proj e.Current
                        let mutable ra = Unchecked.defaultof<_>

                        if not (groups.TryGetValue(key, &ra)) then
                            ra <- ResizeArray()
                            groups[key] <- ra
                            order.Add key

                        ra.Add e.Current

                | AsyncProjectorAction proj ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let key = AsyncHelpers.Await (proj e.Current : Task<_>)
                        let mutable ra = Unchecked.defaultof<_>

                        if not (groups.TryGetValue(key, &ra)) then
                            ra <- ResizeArray()
                            groups[key] <- ra
                            order.Add key

                        ra.Add e.Current

                Array.init order.Count (fun i ->
                    let k = order[i]
                    k, groups[k].ToArray())
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )


    let countBy (projector: ProjectorAction<'T, 'Key, _>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let counts = Dictionary<'Key, int>(HashIdentity.Structural)
                let order = ResizeArray<'Key>()

                match projector with
                | ProjectorAction proj ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let key = proj e.Current
                        let mutable count = 0

                        if not (counts.TryGetValue(key, &count)) then
                            order.Add key

                        counts[key] <- count + 1

                | AsyncProjectorAction proj ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let key = AsyncHelpers.Await (proj e.Current : Task<_>)
                        let mutable count = 0

                        if not (counts.TryGetValue(key, &count)) then
                            order.Add key

                        counts[key] <- count + 1

                Array.init order.Count (fun i -> let k = order[i] in k, counts[k])
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )

    let partition (predicate: PredicateAction<'T, _>) (source: AsyncSeq2<_>) =
        checkNonNull (nameof source) source

        FSharp.Core.CompilerServices.StateMachineHelpers.__runtimeAsyncReturn (
            let e = source.GetAsyncEnumerator CancellationToken.None

            try
                let trueItems = ResizeArray<'T>()
                let falseItems = ResizeArray<'T>()

                match predicate with
                | Predicate pred ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let item = e.Current

                        if pred item then
                            trueItems.Add item
                        else
                            falseItems.Add item

                | PredicateAsync pred ->
                    while AsyncHelpers.Await (e.MoveNextAsync()) do
                        let item = e.Current
                        let result = AsyncHelpers.Await (pred item : Task<bool>)
                        if result then trueItems.Add item else falseItems.Add item

                trueItems.ToArray(), falseItems.ToArray()
            finally
                AsyncHelpers.Await (e.DisposeAsync())
        )


    let chunkBySize chunkSize (source: AsyncSeq2<'T>) : AsyncSeq2<'T[]> =
        if chunkSize < 1 then
            invalidArg (nameof chunkSize) $"The value must be positive, but was %i{chunkSize}."

        checkNonNull (nameof source) source

        algorithmSeq {
            // Use a fixed-size array with a count index to avoid ResizeArray overhead.
            let buffer = Array.zeroCreate<'T> chunkSize
            let mutable count = 0

            for item in source do
                buffer.[count] <- item
                count <- count + 1

                if count = chunkSize then
                    yield Array.copy buffer
                    count <- 0

            if count > 0 then
                // Last partial chunk: copy only the filled portion.
                yield buffer.[0 .. count - 1]
        }

    let chunkBy (projection: 'T -> 'Key) (source: AsyncSeq2<'T>) : AsyncSeq2<'Key * 'T[]> =
        checkNonNull (nameof source) source

        algorithmSeq {
            let mutable maybeCurrentKey = ValueNone
            let mutable currentChunk = ResizeArray<'T>()

            for item in source do
                let key = projection item

                match maybeCurrentKey with
                | ValueNone ->
                    maybeCurrentKey <- ValueSome key
                    currentChunk.Add item
                | ValueSome prevKey ->
                    if prevKey = key then
                        currentChunk.Add item
                    else
                        yield prevKey, currentChunk.ToArray()
                        currentChunk.Clear() // reuse backing array; ToArray() already captured a snapshot
                        currentChunk.Add item
                        maybeCurrentKey <- ValueSome key

            match maybeCurrentKey with
            | ValueNone -> ()
            | ValueSome lastKey -> yield lastKey, currentChunk.ToArray()
        }

    let chunkByAsync (projection: 'T -> #Task<'Key>) (source: AsyncSeq2<'T>) : AsyncSeq2<'Key * 'T[]> =
        checkNonNull (nameof source) source

        algorithmSeq {
            let mutable maybeCurrentKey = ValueNone
            let mutable currentChunk = ResizeArray<'T>()

            for item in source do
                let! key = projection item

                match maybeCurrentKey with
                | ValueNone ->
                    maybeCurrentKey <- ValueSome key
                    currentChunk.Add item
                | ValueSome prevKey ->
                    if prevKey = key then
                        currentChunk.Add item
                    else
                        yield prevKey, currentChunk.ToArray()
                        currentChunk.Clear() // reuse backing array; ToArray() already captured a snapshot
                        currentChunk.Add item
                        maybeCurrentKey <- ValueSome key

            match maybeCurrentKey with
            | ValueNone -> ()
            | ValueSome lastKey -> yield lastKey, currentChunk.ToArray()
        }

    let windowed windowSize (source: AsyncSeq2<_>) =
        if windowSize <= 0 then
            invalidArg (nameof windowSize) $"The value must be positive, but was %i{windowSize}."

        checkNonNull (nameof source) source

        algorithmSeq {
            // Ring buffer: arr holds elements in circular order.
            // 'count' tracks total elements seen; count % windowSize is the next write position.
            let arr = Array.zeroCreate windowSize
            let mutable count = 0

            for item in source do
                arr.[count % windowSize] <- item
                count <- count + 1

                if count >= windowSize then
                    // Copy ring buffer in source order into a fresh array.
                    let result = Array.zeroCreate windowSize
                    let start = count % windowSize // index of oldest element in the ring

                    if start = 0 then
                        Array.blit arr 0 result 0 windowSize
                    else
                        Array.blit arr start result 0 (windowSize - start)
                        Array.blit arr 0 result (windowSize - start) start

                    yield result
        }
