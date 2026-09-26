namespace Microsoft.FSharp.Control

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<AutoOpen>]
module AsyncSeq2ObservableOperations =
    type AsyncSeq2 with
        static member ofObservableBuffered(source: IObservable<'T>) : AsyncSeq2<'T> =
            AsyncSeq2Internal.checkNonNull (nameof source) source
            asyncSeq2 {
                let! ct = Async2.CancellationToken
                let channel = Channel.CreateUnbounded<'T>()
                use subscription =
                    source.Subscribe(
                        { new IObserver<'T> with
                            member _.OnNext value = channel.Writer.TryWrite(value) |> ignore
                            member _.OnError error = channel.Writer.TryComplete(error) |> ignore
                            member _.OnCompleted() = channel.Writer.TryComplete() |> ignore })
                for item in channel.Reader.ReadAllAsync(ct) do
                    yield item
            }

        [<Obsolete("The original ofObservable loses values. Use ofObservableBuffered instead.", true)>]
        static member ofObservable(source: IObservable<'T>) : AsyncSeq2<'T> =
            AsyncSeq2.ofObservableBuffered source

        static member toObservable(source: AsyncSeq2<'T>) : IObservable<'T> =
            AsyncSeq2Internal.checkNonNull (nameof source) source
            { new IObservable<'T> with
                member _.Subscribe(observer: IObserver<'T>) =
                    if isNull observer then nullArg (nameof observer)
                    let cts = new CancellationTokenSource()
                    let mutable stopped = 0
                    let running =
                        AsyncSeq2.iter (fun value ->
                            if Volatile.Read(&stopped) = 0 then observer.OnNext value) source
                        |> fun computation -> Async2.StartAsTask(computation, cancellationToken = cts.Token)
                    running.ContinueWith(fun (completed: Task) ->
                        if Interlocked.Exchange(&stopped, 1) = 0 then
                            if completed.IsFaulted then observer.OnError(completed.Exception.InnerException)
                            elif completed.IsCanceled then observer.OnError(OperationCanceledException(cts.Token))
                            else observer.OnCompleted()
                        cts.Dispose())
                    |> ignore
                    { new IDisposable with
                        member _.Dispose() =
                            if Interlocked.Exchange(&stopped, 1) = 0 then cts.Cancel() }
            }
