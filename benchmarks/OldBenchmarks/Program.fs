module Async2Benchmarks

open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Reports
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Engines
open Perfolizer.Horology
open Perfolizer.Metrology
open System.Globalization

open Microsoft.FSharp.Control
open BenchmarkDotNet.Running

// --- Helper functions for cancellation cases ---
[<AutoOpen>]
module Cases =
    // --- Simple recursion (tail call) ---

    let rec syncLoopTc n = async2 {
        if n = 250_000 then
            return n
        else
            return! syncLoopTc (n + 1)
    }

    // --- Mutual recursion (tail call) ---

    let rec syncMutATc n = async2 {
        if n = 250_000 then
            return n
        else
            return! syncMutBTc (n + 1)

    }
    and syncMutBTc n = async2 {
        if n = 250_000 then
            return n
        else
            return! syncMutATc (n + 1)
    }


    // --- Simple recursion (non-tail call) ---

    let rec syncLoop n = async2 {
        if n = 250_000 then
            return n
        else
            try
                return! syncLoop (n + 1)
            finally
                ()
    }

    // --- Mutual recursion (non-tail call) ---

    let rec syncMutA n = async2 {
        if n = 250_000 then
            return n
        else
            try
                return! syncMutB (n + 1)
            finally
                ()
    }
    and syncMutB n = async2 {
        if n = 250_000 then
            return n
        else
            try
                return! syncMutA (n + 1)
            finally
                ()
    }

    // --- While loop ---

    let syncWhile () = async2 {
        let mutable n = 0
        while n < 250_000 do
            n <- n + 1
        return n
    }


    // --- TryWith ---

    let syncTryWith () = async2 {
        try
            return! syncLoop 0
        with _ ->
            return -1
    }

    // --- TryFinally ---

    let syncTryFinally () = async2 {
        let mutable finalized = false
        try
            let! res = syncLoop 0
            return res
        finally
            finalized <- true
    }

    // --- String-based recursion and loops ---

    let rec syncStringLoop s = async2 {
        if String.length s = 25_000 then
            return s
        else
            try
                return! syncStringLoop (s + "a")
            finally
                ()
    }

    let syncStringWhile () = async2 {
        let mutable s = ""
        while String.length s < 25_000 do
            s <- s + "x"
        return s
    }

    // --- Deep recursive exception-catching (from Simple.fs) ---
    let rec syncExceptionCatchFoo n = async2 {
        try
            if n = 42 then failwith "We are really gone."
            if n = 0 then
                return 0
            else
                return! syncExceptionCatchFoo (n - 1)
        with exn when (n = 4999) -> return n
    }

[<MemoryDiagnoser>]
[<SimpleJob(RunStrategy.Throughput, warmupCount = 1, iterationCount = 1, invocationCount = 1)>]
//[<SimpleJob(RunStrategy.Throughput)>]
type Async2Benchmarks () =

    // --- Wrapper runner functions ---
    let run comp = comp |> Async2.RunSynchronouslyImmediate |> ignore

    // --- Benchmarks ---
    [<Benchmark>]
    member _.SimpleRecursionTailCall () = run (syncLoopTc 0)

    [<Benchmark>]
    member _.MutualRecursionTailCall () = run (syncMutATc 0)

    [<Benchmark>]
    member _.SimpleRecursion () = run (syncLoop 0)

    [<Benchmark>]
    member _.MutualRecursion () = run (syncMutA 0)

    [<Benchmark>]
    member _.WhileLoop () = run (syncWhile ())

    [<Benchmark>]
    member _.TryWith () = run (syncTryWith ())

    [<Benchmark>]
    member _.TryFinally () = run (syncTryFinally ())

    // --- String-based Benchmarks ---
    [<Benchmark>]
    member _.StringSimpleRecursion () = run (syncStringLoop "")

    [<Benchmark>]
    member _.StringWhileLoop () = run (syncStringWhile ())

    [<Benchmark>]
    member _.DeepRecursiveExceptionCatch () = run (syncExceptionCatchFoo 250_000)


module Program =
    let config =
        ManualConfig.Create(DefaultConfig.Instance)
            .AddDiagnoser(MemoryDiagnoser.Default)
            .WithSummaryStyle(
                SummaryStyle.Default
                    .WithTimeUnit(TimeUnit.Millisecond)
                    .WithSizeUnit(SizeUnit.MB)
                    .WithCultureInfo(CultureInfo.InvariantCulture)
            )

    [<EntryPoint>]
    let main args =

        let summary =
            BenchmarkRunner.Run<Async2Benchmarks>(config)

        if summary.HasCriticalValidationErrors ||
           (summary.Reports |> Seq.exists (fun report -> not report.Success)) then
            1
        else
            0
