namespace FSharp.Control.Async2.Benchmarks

open System
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open Microsoft.FSharp.Control

[<MemoryDiagnoser>]
type RecursiveComputationBenchmarks() =
    let failure = InvalidOperationException("Benchmark exception")

    let rec successful depth =
        async2 {
            if depth % 16 = 0 then
                do! Task.Yield()

            if depth = 0 then
                return 1
            else
                return! successful (depth - 1)
        }

    let rec withTryWith depth =
        async2 {
            try
                if depth % 16 = 0 then
                    do! Task.Yield()

                if depth = 0 then
                    return 1
                else
                    return! withTryWith (depth - 1)
            with error when obj.ReferenceEquals(error, failure) ->
                return 1
        }

    let rec withTryFinally depth =
        async2 {
            try
                if depth % 16 = 0 then
                    do! Task.Yield()

                if depth = 0 then
                    return 1
                else
                    return! withTryFinally (depth - 1)
            finally
                ()
        }

    let rec throwThroughTryWith topDepth depth =
        async2 {
            try
                if depth % 16 = 0 then
                    do! Task.Yield()

                if depth = 0 then
                    return raise failure
                else
                    return! throwThroughTryWith topDepth (depth - 1)
            with error when obj.ReferenceEquals(error, failure) ->
                if depth = topDepth then
                    return 1
                else
                    return reraise ()
        }

    let rec throwThroughTryFinally depth =
        async2 {
            try
                if depth % 16 = 0 then
                    do! Task.Yield()

                if depth = 0 then
                    return raise failure
                else
                    return! throwThroughTryFinally (depth - 1)
            finally
                ()
        }

    [<Params(16, 256, 2048)>]
    member val Depth = 0 with get, set

    [<Benchmark>]
    member this.DeepSuccess() =
        Async2.RunSynchronously(successful this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark>]
    member this.DeepTryWithSuccess() =
        Async2.RunSynchronously(withTryWith this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark>]
    member this.DeepTryFinallySuccess() =
        Async2.RunSynchronously(withTryFinally this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark>]
    member this.DeepExceptionThroughTryWith() =
        Async2.RunSynchronously(throwThroughTryWith this.Depth this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark>]
    member this.DeepExceptionThroughTryFinally() =
        try
            Async2.RunSynchronously(throwThroughTryFinally this.Depth, cancellationToken = CancellationToken.None)
            |> ignore
            invalidOp "Expected the deepest level to throw."
        with error when obj.ReferenceEquals(error, failure) ->
            1

    [<Benchmark>]
    member this.DeepSuccessWithCancelableToken() =
        use cancellation = new CancellationTokenSource()
        Async2.RunSynchronously(successful this.Depth, cancellationToken = cancellation.Token)

    [<Benchmark>]
    member this.CancelBeforeStart() =
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()

        try
            Async2.RunSynchronously(successful this.Depth, cancellationToken = cancellation.Token)
            |> ignore
            invalidOp "Expected cancellation before starting."
        with :? OperationCanceledException as error when error.CancellationToken = cancellation.Token ->
            0

    [<Benchmark>]
    member this.CancelAtDeepestLevel() =
        use cancellation = new CancellationTokenSource()

        let rec cancelAtDepth depth =
            async2 {
                if depth % 16 = 0 then
                    do! Task.Yield()

                if depth = 0 then
                    cancellation.Cancel()
                    return 1
                else
                    return! cancelAtDepth (depth - 1)
            }

        try
            Async2.RunSynchronously(cancelAtDepth this.Depth, cancellationToken = cancellation.Token)
            |> ignore
            invalidOp "Expected cancellation at the deepest level."
        with :? OperationCanceledException as error when error.CancellationToken = cancellation.Token ->
            0

module Program =
    [<EntryPoint>]
    let main args =
        let summaries =
            BenchmarkSwitcher.FromAssembly(typeof<RecursiveComputationBenchmarks>.Assembly).Run(args)

        if summaries |> Seq.exists (fun summary ->
            summary.HasCriticalValidationErrors
            || (summary.Reports |> Seq.exists (fun report -> not report.Success))) then
            1
        else
            0
