namespace Microsoft.FSharp.Control

open System
open System.Threading.Tasks

type private AsyncSeq2SrcNode<'T> =
    { Next: TaskCompletionSource<('T * AsyncSeq2SrcNode<'T>) option> }

/// A source that broadcasts subsequent values to all sequences created from it.
type AsyncSeq2Src<'T> =
    private
        { Gate: obj
          mutable Tail: AsyncSeq2SrcNode<'T>
          mutable Closed: bool }

[<RequireQualifiedAccess>]
module AsyncSeq2Src =
    let private node () =
        { Next = TaskCompletionSource<_>(TaskCreationOptions.RunContinuationsAsynchronously) }

    /// Creates a new broadcast source.
    let create<'T> () : AsyncSeq2Src<'T> =
        { Gate = obj ()
          Tail = node ()
          Closed = false }

    /// Publishes a value to sequences created before this call.
    let put (value: 'T) (source: AsyncSeq2Src<'T>) =
        lock source.Gate (fun () ->
            if source.Closed then invalidOp "The source is closed."
            let next = node ()
            source.Tail.Next.SetResult(Some(value, next))
            source.Tail <- next)

    /// Completes all sequences created before this call.
    let close (source: AsyncSeq2Src<'T>) =
        lock source.Gate (fun () ->
            if source.Closed then invalidOp "The source is closed."
            source.Closed <- true
            source.Tail.Next.SetResult(None))

    /// Fails all sequences created before this call.
    let error (failure: exn) (source: AsyncSeq2Src<'T>) =
        if isNull failure then nullArg (nameof failure)
        lock source.Gate (fun () ->
            if source.Closed then invalidOp "The source is closed."
            source.Closed <- true
            source.Tail.Next.SetException(failure))

    /// Creates a sequence that observes values published after this call.
    let toAsyncSeq (source: AsyncSeq2Src<'T>) : AsyncSeq2<'T> =
        let first = lock source.Gate (fun () -> source.Tail)
        asyncSeq2 {
            let! token = Async2.CancellationToken
            let mutable current = first
            let mutable reading = true
            while reading do
                match! current.Next.Task.WaitAsync(token) with
                | Some (value, next) ->
                    yield value
                    current <- next
                | None -> reading <- false
        }
