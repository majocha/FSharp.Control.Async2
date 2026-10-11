namespace FSharp.Control.Async2.Benchmarks

open System
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Running
open Microsoft.FSharp.Control

[<MemoryDiagnoser>]
[<CategoriesColumn>]
[<GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)>]
type RecursiveComputationBenchmarks() =
    let failure = InvalidOperationException("Benchmark exception")

    let yieldAsync =
        Async.FromContinuations(fun (continuation, _, _) ->
            Task.Yield().GetAwaiter().UnsafeOnCompleted(fun () -> continuation ()))

    let runAsync computation cancellationToken =
        Async.StartImmediateAsTask(computation, cancellationToken = cancellationToken)
            .GetAwaiter()
            .GetResult()

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

    let rec successfulAsync depth =
        async {
            if depth % 16 = 0 then
                do! yieldAsync

            if depth = 0 then
                return 1
            else
                return! successfulAsync (depth - 1)
        }

    let rec withTryWithAsync depth =
        async {
            try
                if depth % 16 = 0 then
                    do! yieldAsync

                if depth = 0 then
                    return 1
                else
                    return! withTryWithAsync (depth - 1)
            with error when obj.ReferenceEquals(error, failure) ->
                return 1
        }

    let rec withTryFinallyAsync depth =
        async {
            try
                if depth % 16 = 0 then
                    do! yieldAsync

                if depth = 0 then
                    return 1
                else
                    return! withTryFinallyAsync (depth - 1)
            finally
                ()
        }

    let rec throwThroughTryWithAsync topDepth depth =
        async {
            try
                if depth % 16 = 0 then
                    do! yieldAsync

                if depth = 0 then
                    return raise failure
                else
                    return! throwThroughTryWithAsync topDepth (depth - 1)
            with error when obj.ReferenceEquals(error, failure) ->
                if depth = topDepth then
                    return 1
                else
                    return reraise ()
        }

    let rec throwThroughTryFinallyAsync depth =
        async {
            try
                if depth % 16 = 0 then
                    do! yieldAsync

                if depth = 0 then
                    return raise failure
                else
                    return! throwThroughTryFinallyAsync (depth - 1)
            finally
                ()
        }

    [<Params(16, 256, 2048)>]
    member val Depth = 0 with get, set

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepSuccess")>]
    member this.DeepSuccess() =
        Async2.RunSynchronouslyImmediate(successful this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepTryWithSuccess")>]
    member this.DeepTryWithSuccess() =
        Async2.RunSynchronouslyImmediate(withTryWith this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepTryFinallySuccess")>]
    member this.DeepTryFinallySuccess() =
        Async2.RunSynchronouslyImmediate(withTryFinally this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepExceptionThroughTryWith")>]
    member this.DeepExceptionThroughTryWith() =
        Async2.RunSynchronouslyImmediate(throwThroughTryWith this.Depth this.Depth, cancellationToken = CancellationToken.None)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepExceptionThroughTryFinally")>]
    member this.DeepExceptionThroughTryFinally() =
        try
            Async2.RunSynchronouslyImmediate(throwThroughTryFinally this.Depth, cancellationToken = CancellationToken.None)
            |> ignore
            invalidOp "Expected the deepest level to throw."
        with error when obj.ReferenceEquals(error, failure) ->
            1

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("DeepSuccessWithCancelableToken")>]
    member this.DeepSuccessWithCancelableToken() =
        use cancellation = new CancellationTokenSource()
        Async2.RunSynchronouslyImmediate(successful this.Depth, cancellationToken = cancellation.Token)

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("CancelBeforeStart")>]
    member this.CancelBeforeStart() =
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()

        try
            Async2.RunSynchronouslyImmediate(successful this.Depth, cancellationToken = cancellation.Token)
            |> ignore
            invalidOp "Expected cancellation before starting."
        with :? OperationCanceledException as error when error.CancellationToken = cancellation.Token ->
            0

    [<Benchmark(Baseline = true)>]
    [<BenchmarkCategory("CancelAtDeepestLevel")>]
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
            Async2.RunSynchronouslyImmediate(cancelAtDepth this.Depth, cancellationToken = cancellation.Token)
            |> ignore
            invalidOp "Expected cancellation at the deepest level."
        with :? OperationCanceledException as error when error.CancellationToken = cancellation.Token ->
            0

    [<Benchmark>]
    [<BenchmarkCategory("DeepSuccess")>]
    member this.DeepSuccessAsync() =
        runAsync (successfulAsync this.Depth) CancellationToken.None

    [<Benchmark>]
    [<BenchmarkCategory("DeepTryWithSuccess")>]
    member this.DeepTryWithSuccessAsync() =
        runAsync (withTryWithAsync this.Depth) CancellationToken.None

    [<Benchmark>]
    [<BenchmarkCategory("DeepTryFinallySuccess")>]
    member this.DeepTryFinallySuccessAsync() =
        runAsync (withTryFinallyAsync this.Depth) CancellationToken.None

    [<Benchmark>]
    [<BenchmarkCategory("DeepExceptionThroughTryWith")>]
    member this.DeepExceptionThroughTryWithAsync() =
        runAsync (throwThroughTryWithAsync this.Depth this.Depth) CancellationToken.None

    [<Benchmark>]
    [<BenchmarkCategory("DeepExceptionThroughTryFinally")>]
    member this.DeepExceptionThroughTryFinallyAsync() =
        try
            runAsync (throwThroughTryFinallyAsync this.Depth) CancellationToken.None
            |> ignore
            invalidOp "Expected the deepest level to throw."
        with error when obj.ReferenceEquals(error, failure) ->
            1

    [<Benchmark>]
    [<BenchmarkCategory("DeepSuccessWithCancelableToken")>]
    member this.DeepSuccessWithCancelableTokenAsync() =
        use cancellation = new CancellationTokenSource()
        runAsync (successfulAsync this.Depth) cancellation.Token

    [<Benchmark>]
    [<BenchmarkCategory("CancelBeforeStart")>]
    member this.CancelBeforeStartAsync() =
        use cancellation = new CancellationTokenSource()
        cancellation.Cancel()

        try
            runAsync (successfulAsync this.Depth) cancellation.Token
            |> ignore
            invalidOp "Expected cancellation before starting."
        with :? OperationCanceledException when cancellation.IsCancellationRequested ->
            0

    [<Benchmark>]
    [<BenchmarkCategory("CancelAtDeepestLevel")>]
    member this.CancelAtDeepestLevelAsync() =
        use cancellation = new CancellationTokenSource()

        let rec cancelAtDepth depth =
            async {
                if depth % 16 = 0 then
                    do! yieldAsync

                if depth = 0 then
                    cancellation.Cancel()
                    return 1
                else
                    return! cancelAtDepth (depth - 1)
            }

        try
            runAsync (cancelAtDepth this.Depth) cancellation.Token
            |> ignore
            invalidOp "Expected cancellation at the deepest level."
        with :? OperationCanceledException when cancellation.IsCancellationRequested ->
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
