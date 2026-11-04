namespace FSharp.Control.Async2.Benchmarks

open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open Microsoft.FSharp.Control

module TopLevelStarts =
    [<Literal>]
    let Count = 1000

open TopLevelStarts

/// Many short starts from code that has no parent context: each one gets a trampoline of its own.
/// `let!` of an Async2 in asyncSeq2 and starts from user code both start this way.
[<MemoryDiagnoser>]
type TopLevelStartBenchmarks() =
    let value (i: int) = async2 { return i }

    let yielding (i: int) =
        async2 {
            do! Task.Yield()
            return i
        }

    /// `let!` of an Async2 that completes synchronously, once for each element of an asyncSeq2.
    [<Benchmark(OperationsPerInvoke = Count)>]
    member _.AsyncSeqBind() =
        asyncSeq2 {
            for i in 0 .. Count - 1 do
                let! x = value i
                yield x
        }
        |> AsyncSeq2.fold (+) 0
        |> Run.async2

    /// Starts from user code that complete synchronously.
    [<Benchmark(OperationsPerInvoke = Count)>]
    member _.StartImmediateAsTask() =
        let mutable total = 0

        for i in 0 .. Count - 1 do
            total <- total + Async2.StartImmediateAsTask(value i).Result

        total

    /// Starts from user code that suspend once, so their trampoline is never reused.
    [<Benchmark(OperationsPerInvoke = Count)>]
    member _.StartImmediateAsTaskSuspending() =
        let mutable total = 0

        for i in 0 .. Count - 1 do
            total <- total + Async2.StartImmediateAsTask(yielding i).GetAwaiter().GetResult()

        total
