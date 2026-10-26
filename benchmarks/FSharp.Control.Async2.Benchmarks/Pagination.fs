namespace FSharp.Control.Async2.Benchmarks

open System.Collections.Generic
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open FSharp.Control
open Microsoft.FSharp.Control

type Order = { Id: int; Currency: int; AmountCents: int64; Shipped: bool }

/// Exporting every order from a paginated REST API as an asynchronous sequence: 20 pages of 50 orders,
/// each page a round trip. The pipeline keeps shipped orders, converts each to the reporting currency
/// through a rate cache (a `ValueTask` that hits synchronously) and sums them. The async flavour uses
/// FSharp.Control.AsyncSeq, the task flavour FSharp.Control.TaskSeq.
[<MemoryDiagnoser>]
type PaginationBenchmarks() =
    static let pageCount = 20

    static let pages =
        Array.init pageCount (fun page ->
            Array.init 50 (fun i ->
                let id = page * 50 + i
                { Id = id; Currency = id % 4; AmountCents = int64 (100 + id); Shipped = id % 3 <> 0 }))

    static let rates = Dictionary<int, int64>(seq { for c in 0..3 -> KeyValuePair(c, int64 (c + 1)) })

    let fetchPage (page: int) = Simulated.remoteCall pages[page]

    let rate (currency: int) : ValueTask<int64> = ValueTask<int64>(rates[currency])

    [<Benchmark(Baseline = true)>]
    member _.Async() =
        asyncSeq {
            for page in 0 .. pageCount - 1 do
                let! orders = fetchPage page |> Async.AwaitTask

                for order in orders do
                    yield order
        }
        |> AsyncSeq.filter _.Shipped
        |> AsyncSeq.mapAsync (fun order ->
            async {
                let! rate = rate(order.Currency).AsTask() |> Async.AwaitTask
                return order.AmountCents * rate
            })
        |> AsyncSeq.fold (+) 0L
        |> Run.async

    [<Benchmark>]
    member _.Async2() =
        asyncSeq2 {
            for page in 0 .. pageCount - 1 do
                let! orders = fetchPage page

                for order in orders do
                    yield order
        }
        |> AsyncSeq2.filter _.Shipped
        |> AsyncSeq2.mapAsync (fun order ->
            async2 {
                let! rate = rate order.Currency
                return order.AmountCents * rate
            })
        |> AsyncSeq2.fold (+) 0L
        |> Run.async2

    [<Benchmark>]
    member _.Task() =
        Run.task (fun () ->
            taskSeq {
                for page in 0 .. pageCount - 1 do
                    let! orders = fetchPage page

                    for order in orders do
                        yield order
            }
            |> TaskSeq.filter _.Shipped
            |> TaskSeq.mapAsync (fun order ->
                task {
                    let! rate = rate order.Currency
                    return order.AmountCents * rate
                })
            |> TaskSeq.fold (+) 0L)
