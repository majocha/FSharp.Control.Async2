namespace FSharp.Control.Async2.Benchmarks

open System
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

/// Transient-fault handling around a flaky upstream: each of 100 operations times out `Failures` times
/// before it succeeds. A generic retry policy catches the timeout, backs off and tries again. Unlike the
/// recursion benchmarks, every failure throws a newly allocated exception, as a real client would.
[<MemoryDiagnoser>]
type RetryBenchmarks() =
    static let Operations = 100
    static let MaxAttempts = 5

    let rec retryAsync attempt (operation: int -> Async<'T>) =
        async {
            try
                return! operation attempt
            with :? TimeoutException when attempt < MaxAttempts ->
                do! Simulated.roundTrip () |> Async.AwaitTask
                return! retryAsync (attempt + 1) operation
        }

    let rec retryAsync2 attempt (operation: int -> Async2<'T>) =
        async2 {
            try
                return! operation attempt
            with :? TimeoutException when attempt < MaxAttempts ->
                do! Simulated.roundTrip ()
                return! retryAsync2 (attempt + 1) operation
        }

    let rec retryTask attempt (operation: int -> Threading.Tasks.Task<'T>) =
        task {
            try
                return! operation attempt
            with :? TimeoutException when attempt < MaxAttempts ->
                do! Simulated.roundTrip ()
                return! retryTask (attempt + 1) operation
        }

    [<Params(0, 2)>]
    member val Failures = 0 with get, set

    [<Benchmark(Baseline = true)>]
    member this.Async() =
        let failures = this.Failures

        let callUpstream request attempt =
            async {
                let! answer = Simulated.remoteCall request |> Async.AwaitTask

                if attempt <= failures then
                    return raise (TimeoutException "The upstream did not answer in time.")
                else
                    return answer
            }

        Run.async (
            async {
                let total = ref 0

                for request in 1..Operations do
                    let! answer = retryAsync 1 (callUpstream request)
                    total.Value <- total.Value + answer

                return total.Value
            }
        )

    [<Benchmark>]
    member this.Async2() =
        let failures = this.Failures

        let callUpstream request attempt =
            async2 {
                let! answer = Simulated.remoteCall request

                if attempt <= failures then
                    return raise (TimeoutException "The upstream did not answer in time.")
                else
                    return answer
            }

        Run.async2 (
            async2 {
                let mutable total = 0

                for request in 1..Operations do
                    let! answer = retryAsync2 1 (callUpstream request)
                    total <- total + answer

                return total
            }
        )

    [<Benchmark>]
    member this.Task() =
        let failures = this.Failures

        let callUpstream request attempt =
            task {
                let! answer = Simulated.remoteCall request

                if attempt <= failures then
                    return raise (TimeoutException "The upstream did not answer in time.")
                else
                    return answer
            }

        Run.task (fun () ->
            task {
                let mutable total = 0

                for request in 1..Operations do
                    let! answer = retryTask 1 (callUpstream request)
                    total <- total + answer

                return total
            })
