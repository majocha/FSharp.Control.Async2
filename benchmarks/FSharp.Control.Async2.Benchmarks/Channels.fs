namespace FSharp.Control.Async2.Benchmarks

open System
open System.Threading.Channels
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

/// An ingestion pipeline over bounded `System.Threading.Channels`: a reader produces events, a parser
/// transforms them and an aggregator sums them, each stage running concurrently with the others. The
/// channel APIs return `ValueTask`. A capacity of 1 hands over every item; 100 lets stages run ahead.
module ChannelPipeline =
    [<Literal>]
    let Items = 10_000

open ChannelPipeline

[<MemoryDiagnoser>]
type ChannelPipelineBenchmarks() =
    let newChannel capacity =
        Channel.CreateBounded<int>(BoundedChannelOptions(capacity, SingleReader = true, SingleWriter = true))

    // Takes what is already buffered without awaiting, as channel consumers usually do.
    let drain (reader: ChannelReader<int>) (each: int -> unit) =
        let mutable item = 0

        while reader.TryRead(&item) do
            each item

    let parse (event: int) = event * 2

    let produceAsync (writer: ChannelWriter<int>) =
        async {
            for event in 1..Items do
                do! writer.WriteAsync(event).AsTask() |> Async.AwaitTask

            writer.Complete()
        }

    let transformAsync (reader: ChannelReader<int>) (writer: ChannelWriter<int>) =
        async {
            let reading = ref true

            while reading.Value do
                let! available = reader.WaitToReadAsync().AsTask() |> Async.AwaitTask

                if available then
                    let parsed = ResizeArray()
                    drain reader (fun event -> parsed.Add(parse event))

                    for item in parsed do
                        do! writer.WriteAsync(item).AsTask() |> Async.AwaitTask
                else
                    reading.Value <- false

            writer.Complete()
        }

    let aggregateAsync (reader: ChannelReader<int>) =
        async {
            let total = ref 0L
            let reading = ref true

            while reading.Value do
                let! available = reader.WaitToReadAsync().AsTask() |> Async.AwaitTask

                if available then
                    drain reader (fun item -> total.Value <- total.Value + int64 item)
                else
                    reading.Value <- false

            return total.Value
        }

    let produceAsync2 (writer: ChannelWriter<int>) =
        async2 {
            for event in 1..Items do
                do! writer.WriteAsync event

            writer.Complete()
        }

    let transformAsync2 (reader: ChannelReader<int>) (writer: ChannelWriter<int>) =
        async2 {
            let mutable reading = true

            while reading do
                let! available = reader.WaitToReadAsync()

                if available then
                    let parsed = ResizeArray()
                    drain reader (fun event -> parsed.Add(parse event))

                    for item in parsed do
                        do! writer.WriteAsync item
                else
                    reading <- false

            writer.Complete()
        }

    let aggregateAsync2 (reader: ChannelReader<int>) =
        async2 {
            let total = ref 0L
            let mutable reading = true

            while reading do
                let! available = reader.WaitToReadAsync()

                if available then
                    drain reader (fun item -> total.Value <- total.Value + int64 item)
                else
                    reading <- false

            return total.Value
        }

    let produceTask (writer: ChannelWriter<int>) =
        task {
            for event in 1..Items do
                do! writer.WriteAsync event

            writer.Complete()
        }

    let transformTask (reader: ChannelReader<int>) (writer: ChannelWriter<int>) =
        task {
            let mutable reading = true

            while reading do
                let! available = reader.WaitToReadAsync()

                if available then
                    let parsed = ResizeArray()
                    drain reader (fun event -> parsed.Add(parse event))

                    for item in parsed do
                        do! writer.WriteAsync item
                else
                    reading <- false

            writer.Complete()
        }

    let aggregateTask (reader: ChannelReader<int>) =
        task {
            let total = ref 0L
            let mutable reading = true

            while reading do
                let! available = reader.WaitToReadAsync()

                if available then
                    drain reader (fun item -> total.Value <- total.Value + int64 item)
                else
                    reading <- false

            return total.Value
        }

    [<Params(1, 100)>]
    member val Capacity = 1 with get, set

    [<Benchmark(Baseline = true, OperationsPerInvoke = Items)>]
    member this.Async() =
        let events = newChannel this.Capacity
        let parsed = newChannel this.Capacity

        Run.async (
            async {
                let! producing = Async.StartChild(produceAsync events.Writer)
                let! transforming = Async.StartChild(transformAsync events.Reader parsed.Writer)
                let! total = aggregateAsync parsed.Reader
                do! producing
                do! transforming
                return total
            }
        )

    [<Benchmark(OperationsPerInvoke = Items)>]
    member this.Async2() =
        let events = newChannel this.Capacity
        let parsed = newChannel this.Capacity

        Run.async2 (
            async2 {
                let! producing = Async2.StartChild(produceAsync2 events.Writer)
                let! transforming = Async2.StartChild(transformAsync2 events.Reader parsed.Writer)
                let! total = aggregateAsync2 parsed.Reader
                do! producing
                do! transforming
                return total
            }
        )

    [<Benchmark(OperationsPerInvoke = Items)>]
    member this.Task() =
        let events = newChannel this.Capacity
        let parsed = newChannel this.Capacity

        Run.task (fun () ->
            task {
                let producing = Task.Run(fun () -> produceTask events.Writer :> Task)
                let transforming = Task.Run(fun () -> transformTask events.Reader parsed.Writer :> Task)
                let! total = aggregateTask parsed.Reader
                do! producing
                do! transforming
                return total
            })
