namespace FSharp.Control.Async2.Benchmarks

open System
open System.Text
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

/// Tailing a 1 MB access log from a socket in 4 KB reads and counting its lines. Most reads find data
/// already buffered; every `AsyncEvery`-th read waits for the network.
[<MemoryDiagnoser>]
type StreamParsingBenchmarks() =
    let log =
        let line = "2026-10-26T12:00:00Z GET /api/orders/42 200 17ms\n"
        Encoding.ASCII.GetBytes(String.replicate (1024 * 1024 / line.Length) line)

    let buffer = Array.zeroCreate<byte> 4096
    let mutable stream = Unchecked.defaultof<SimulatedNetworkStream>

    let countLines (count: int) =
        MemoryExtensions.Count(ReadOnlySpan<byte>(buffer, 0, count), 10uy)

    [<Params(1, 8)>]
    member val AsyncEvery = 1 with get, set

    [<GlobalSetup>]
    member this.Setup() =
        stream <- new SimulatedNetworkStream(log, this.AsyncEvery)

    /// What FSharp.Core's `Stream.AsyncRead` does: the `Task` overload of `ReadAsync` under `Async.AwaitTask`.
    [<Benchmark(Baseline = true)>]
    member _.Async() =
        stream.Reset()

        Run.async (
            async {
                let lines = ref 0
                let reading = ref true

                while reading.Value do
                    let! count = Async.AwaitTask(stream.ReadAsync(buffer, 0, buffer.Length))

                    if count = 0 then
                        reading.Value <- false
                    else
                        lines.Value <- lines.Value + countLines count

                return lines.Value
            }
        )

    /// Awaits the `ValueTask` overload directly, passing the computation's cancellation token.
    [<Benchmark>]
    member _.Async2() =
        stream.Reset()

        Run.async2 (
            async2 {
                let! ct = CurrentCancellationToken
                let mutable lines = 0
                let mutable reading = true

                while reading do
                    let! count = stream.ReadAsync(buffer.AsMemory(), ct)

                    if count = 0 then
                        reading <- false
                    else
                        lines <- lines + countLines count

                return lines
            }
        )

    [<Benchmark>]
    member _.Task() =
        stream.Reset()

        Run.task (fun () ->
            task {
                let mutable lines = 0
                let mutable reading = true

                while reading do
                    let! count = stream.ReadAsync(buffer.AsMemory())

                    if count = 0 then
                        reading <- false
                    else
                        lines <- lines + countLines count

                return lines
            })
