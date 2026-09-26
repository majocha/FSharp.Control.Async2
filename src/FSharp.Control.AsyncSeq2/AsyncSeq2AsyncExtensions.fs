namespace Microsoft.FSharp.Control

open System
open System.Threading.Tasks

[<AutoOpen>]
module AsyncSeq2AsyncExtensions =
    let private awaitTaskCorrect (task: Task<unit>) : Async<unit> =
        Async.FromContinuations(fun (success, failure, cancellation) ->
            task.ContinueWith(fun (completed: Task<unit>) ->
                if completed.IsFaulted then
                    let errors = completed.Exception.InnerExceptions
                    failure (if errors.Count = 1 then errors.[0] else completed.Exception)
                elif completed.IsCanceled then
                    cancellation (OperationCanceledException("The operation was cancelled."))
                else
                    success ())
            |> ignore)

    type AsyncBuilder with
        member _.For(source: AsyncSeq2<'T>, action: 'T -> Async<unit>) =
            async {
                let! ct = Async.CancellationToken
                let computation = async2 {
                    for item in source do
                        do! action item
                }
                return! Async2.StartAsTask(computation, cancellationToken = ct) |> awaitTaskCorrect
            }
