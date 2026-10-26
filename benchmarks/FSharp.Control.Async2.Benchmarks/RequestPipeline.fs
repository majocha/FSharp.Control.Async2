namespace FSharp.Control.Async2.Benchmarks

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

type Request = { UserId: int; Path: string }
type Response = { Status: int; Body: int }
type Profile = { Name: string; Tier: int }

/// An HTTP endpoint behind a middleware stack composed from handler functions, as in Giraffe or
/// Saturn: error handling (`try/with`), request timing (`try/finally`), authentication against a
/// session cache, then the endpoint, which reads the user's profile through a read-through cache.
/// Cache lookups return `ValueTask`: a hit completes synchronously, a miss goes to the database.
[<MemoryDiagnoser>]
type RequestPipelineBenchmarks() =
    let requests =
        Array.init 1000 (fun i -> { UserId = i % 200; Path = "/api/profile" })

    let profiles =
        Dictionary<int, Profile>(seq { for id in 0..199 -> KeyValuePair(id, { Name = $"user{id}"; Tier = id % 3 }) })

    let mutable elapsedTicks = 0L

    let mutable lookups = 0

    // The cache is warm except for a fixed share of lookups, spread evenly over the requests.
    let mutable hitPercent = 100

    let isSessionValid (userId: int) : ValueTask<bool> = ValueTask<bool>(userId >= 0)

    let tryCachedProfile (userId: int) : ValueTask<Profile> =
        lookups <- lookups + 1

        if lookups % 100 < hitPercent then
            ValueTask<Profile>(profiles[userId])
        else
            ValueTask<Profile>(Simulated.remoteCall profiles[userId])

    let render (profile: Profile) = { Status = 200; Body = profile.Name.Length + profile.Tier }

    // async: ValueTask-returning APIs go through AsTask and Async.AwaitTask.
    let asyncPipeline: Request -> Async<Response> =
        let endpoint (request: Request) =
            async {
                let! profile = tryCachedProfile(request.UserId).AsTask() |> Async.AwaitTask
                return render profile
            }

        let authenticate (next: Request -> Async<Response>) (request: Request) =
            async {
                let! valid = isSessionValid(request.UserId).AsTask() |> Async.AwaitTask

                if valid then
                    return! next request
                else
                    return { Status = 401; Body = 0 }
            }

        let timing (next: Request -> Async<Response>) (request: Request) =
            async {
                let started = Stopwatch.GetTimestamp()

                try
                    return! next request
                finally
                    elapsedTicks <- elapsedTicks + (Stopwatch.GetTimestamp() - started)
            }

        let handleErrors (next: Request -> Async<Response>) (request: Request) =
            async {
                try
                    return! next request
                with _ ->
                    return { Status = 500; Body = 0 }
            }

        endpoint |> authenticate |> timing |> handleErrors

    let async2Pipeline: Request -> Async2<Response> =
        let endpoint (request: Request) =
            async2 {
                let! profile = tryCachedProfile request.UserId
                return render profile
            }

        let authenticate (next: Request -> Async2<Response>) (request: Request) =
            async2 {
                let! valid = isSessionValid request.UserId

                if valid then
                    return! next request
                else
                    return { Status = 401; Body = 0 }
            }

        let timing (next: Request -> Async2<Response>) (request: Request) =
            async2 {
                let started = Stopwatch.GetTimestamp()

                try
                    return! next request
                finally
                    elapsedTicks <- elapsedTicks + (Stopwatch.GetTimestamp() - started)
            }

        let handleErrors (next: Request -> Async2<Response>) (request: Request) =
            async2 {
                try
                    return! next request
                with _ ->
                    return { Status = 500; Body = 0 }
            }

        endpoint |> authenticate |> timing |> handleErrors

    let taskPipeline: Request -> Task<Response> =
        let endpoint (request: Request) =
            task {
                let! profile = tryCachedProfile request.UserId
                return render profile
            }

        let authenticate (next: Request -> Task<Response>) (request: Request) =
            task {
                let! valid = isSessionValid request.UserId

                if valid then
                    return! next request
                else
                    return { Status = 401; Body = 0 }
            }

        let timing (next: Request -> Task<Response>) (request: Request) =
            task {
                let started = Stopwatch.GetTimestamp()

                try
                    return! next request
                finally
                    elapsedTicks <- elapsedTicks + (Stopwatch.GetTimestamp() - started)
            }

        let handleErrors (next: Request -> Task<Response>) (request: Request) =
            task {
                try
                    return! next request
                with _ ->
                    return { Status = 500; Body = 0 }
            }

        endpoint |> authenticate |> timing |> handleErrors

    /// Percentage of profile lookups served from the cache; the rest wait for the database.
    [<Params(100, 90, 0)>]
    member val CacheHitPercent = 100 with get, set

    [<GlobalSetup>]
    member this.Setup() = hitPercent <- this.CacheHitPercent

    [<Benchmark(Baseline = true, OperationsPerInvoke = 1000)>]
    member _.Async() =
        Run.async (
            async {
                let total = ref 0

                for request in requests do
                    let! response = asyncPipeline request
                    total.Value <- total.Value + response.Body

                return total.Value
            }
        )

    [<Benchmark(OperationsPerInvoke = 1000)>]
    member _.Async2() =
        Run.async2 (
            async2 {
                let mutable total = 0

                for request in requests do
                    let! response = async2Pipeline request
                    total <- total + response.Body

                return total
            }
        )

    [<Benchmark(OperationsPerInvoke = 1000)>]
    member _.Task() =
        Run.task (fun () ->
            task {
                let mutable total = 0

                for request in requests do
                    let! response = taskPipeline request
                    total <- total + response.Body

                return total
            })
