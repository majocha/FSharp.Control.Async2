namespace FSharp.Control.Async2.Benchmarks

open System
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

/// A backend-for-frontend building a product page: it calls `Services` downstream services (price,
/// stock, reviews, recommendations, ...) at once and combines their answers. Every call is a cold
/// computation that starts its own request, as a real client wrapper would.
[<MemoryDiagnoser>]
type ScatterGatherBenchmarks() =
    let fetchAsync (service: int) =
        async {
            let! answer = Simulated.remoteCall service |> Async.AwaitTask
            return answer * 2
        }

    let fetchAsync2 (service: int) =
        async2 {
            let! answer = Simulated.remoteCall service
            return answer * 2
        }

    let fetchTask (service: int) =
        task {
            let! answer = Simulated.remoteCall service
            return answer * 2
        }

    [<Params(4, 32)>]
    member val Services = 4 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Async() =
        Run.async (
            async {
                let! answers = Async.Parallel [ for service in 1 .. this.Services -> fetchAsync service ]
                return Array.sum answers
            }
        )

    [<Benchmark>]
    member this.Async2() =
        Run.async2 (
            async2 {
                let! answers = Async2.Parallel [ for service in 1 .. this.Services -> fetchAsync2 service ]
                return Array.sum answers
            }
        )

    [<Benchmark>]
    member this.Task() =
        Run.task (fun () ->
            task {
                let! answers = Task.WhenAll [ for service in 1 .. this.Services -> fetchTask service ]
                return Array.sum answers
            })

/// A nightly sync pushing 256 records to a rate-limited API, at most 8 requests in flight. Each upload
/// is two round trips (send, then confirm) inside a `try/finally` that records progress.
[<MemoryDiagnoser>]
type ThrottledUploadBenchmarks() =
    let records = Array.init 256 id
    let mutable uploaded = 0

    let uploadAsync (record: int) =
        async {
            try
                do! Simulated.roundTrip () |> Async.AwaitTask
                let! confirmation = Simulated.remoteCall record |> Async.AwaitTask
                return confirmation
            finally
                Interlocked.Increment(&uploaded) |> ignore
        }

    let uploadAsync2 (record: int) =
        async2 {
            try
                do! Simulated.roundTrip ()
                let! confirmation = Simulated.remoteCall record
                return confirmation
            finally
                Interlocked.Increment(&uploaded) |> ignore
        }

    let uploadTask (record: int) =
        task {
            try
                do! Simulated.roundTrip ()
                let! confirmation = Simulated.remoteCall record
                return confirmation
            finally
                Interlocked.Increment(&uploaded) |> ignore
        }

    [<Benchmark(Baseline = true)>]
    member _.Async() =
        Run.async (Async.Parallel(Seq.map uploadAsync records, maxDegreeOfParallelism = 8))
        |> Array.sum

    [<Benchmark>]
    member _.Async2() =
        Run.async2 (Async2.Parallel(Seq.map uploadAsync2 records, maxDegreeOfParallelism = 8))
        |> Array.sum

    /// The usual task equivalent: `Parallel.ForEachAsync` writing into a result array.
    [<Benchmark>]
    member _.Task() =
        Run.task (fun () ->
            task {
                let confirmations = Array.zeroCreate records.Length
                let options = ParallelOptions(MaxDegreeOfParallelism = 8)

                do!
                    Parallel.ForEachAsync(
                        records,
                        options,
                        fun record _ ->
                            ValueTask(
                                task {
                                    let! confirmation = uploadTask record
                                    confirmations[record] <- confirmation
                                }
                            )
                    )

                return Array.sum confirmations
            })

/// Crawling an object store or a directory tree: listing a folder is a remote call, and its subfolders
/// are crawled in parallel, recursively. A tree of fan-out 4 and depth `Depth` has (4^(Depth+1)-1)/3 folders.
[<MemoryDiagnoser>]
type TreeCrawlBenchmarks() =
    let rec crawlAsync depth folder =
        async {
            let! size = Simulated.remoteCall (folder % 1000) |> Async.AwaitTask

            if depth = 0 then
                return size
            else
                let! sizes = Async.Parallel [ for child in 1..4 -> crawlAsync (depth - 1) (folder * 4 + child) ]
                return size + Array.sum sizes
        }

    let rec crawlAsync2 depth folder =
        async2 {
            let! size = Simulated.remoteCall (folder % 1000)

            if depth = 0 then
                return size
            else
                let! sizes = Async2.Parallel [ for child in 1..4 -> crawlAsync2 (depth - 1) (folder * 4 + child) ]
                return size + Array.sum sizes
        }

    let rec crawlTask depth folder : Task<int> =
        task {
            let! size = Simulated.remoteCall (folder % 1000)

            if depth = 0 then
                return size
            else
                let! sizes = Task.WhenAll [ for child in 1..4 -> crawlTask (depth - 1) (folder * 4 + child) ]
                return size + Array.sum sizes
        }

    [<Params(3, 5)>]
    member val Depth = 3 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Async() = Run.async (crawlAsync this.Depth 0)

    [<Benchmark>]
    member this.Async2() = Run.async2 (crawlAsync2 this.Depth 0)

    [<Benchmark>]
    member this.Task() = Run.task (fun () -> crawlTask this.Depth 0)
