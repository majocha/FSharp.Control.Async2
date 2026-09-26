namespace Microsoft.FSharp.Control.AsyncSeq2Implementation

open System
open System.Threading.Tasks
open Microsoft.FSharp.Control

module ValueTask =
    let False = ValueTask<bool>()
    let True = ValueTask<bool> true
    let inline fromResult (value: 'T) = ValueTask<'T> value
    let inline ofSource taskSource version = ValueTask<bool>(taskSource, version)
    let inline ofTask (task: Task<'T>) = ValueTask<'T> task

    let inline ignore (valueTask: ValueTask<'T>) =
        runtimeValueTask {
            let! _ = valueTask
            return ()
        }

    [<Obsolete "From version 0.4.0 onward, 'ValueTask.FromResult' is deprecated in favor of 'ValueTask.fromResult'. It will be removed in an upcoming release.">]
    let inline FromResult (value: 'T) = ValueTask<'T> value

    [<Obsolete "From version 0.4.0 onward, 'ValueTask.ofIValueTaskSource' is deprecated in favor of 'ValueTask.ofSource'. It will be removed in an upcoming release.">]
    let inline ofIValueTaskSource taskSource version = ofSource taskSource version

module Task =
    let inline fromResult (value: 'U) : Task<'U> = Task.FromResult value
    let inline ofAsync (async: Async<'T>) = runtimeTask { return! async }
    let inline ofTask (task': Task) = runtimeTask { do! task' }
    let inline apply (func: _ -> _) = func >> Task.FromResult
    let inline toAsync (task: Task<'T>) = Async.AwaitTask task
    let inline toValueTask (task: Task<'T>) = ValueTask<'T> task
    let inline ofValueTask (valueTask: ValueTask<'T>) = runtimeTask { return! valueTask }

    let inline ignore (task: Task<'T>) =
        runtimeTask {
            // ensure the task is awaited
            let! _ = task
            return ()
        }
        :> Task

    let inline map mapper (task: Task<'T>) : Task<'U> = runtimeTask {
        let! result = task
        return mapper result
    }

    let inline bind (binder: 'T -> #Task<'U>) (task: Task<'T>) : Task<'U> = runtimeTask {
        let! t = task
        return! binder t
    }

module Async =
    let inline ofTask (task: Task<'T>) = Async.AwaitTask task
    let inline ofUnitTask (task: Task) = Async.AwaitTask task
    let inline toTask (async: Async<'T>) = runtimeTask { return! async }

    let inline ignore (async: Async<'T>) = Async.Ignore async

    let inline map mapper (async: Async<'T>) : Async<'U> = ExtraTopLevelOperators.async {
        let! result = async
        return mapper result
    }

    let inline bind binder (async: Async<'T>) : Async<'U> = ExtraTopLevelOperators.async {
        let! result = async
        return! binder result
    }
