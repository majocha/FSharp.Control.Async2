namespace FSharp.Control.Async2.Benchmarks

open System.Threading
open System.Threading.Channels
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Configs
open Microsoft.FSharp.Control

/// An in-process metrics agent that owns a table of counters: callers post increments without waiting
/// and query a counter or the total with a reply. The agent body is the usual `let rec loop` with a
/// tail `return!` per message.
module MetricsAgent =
    [<Literal>]
    let Counters = 64

    [<Literal>]
    let Posts = 10_000

    [<Literal>]
    let Queries = 1_000

    type Message =
        | Increment of counter: int
        | Get of counter: int * AsyncReplyChannel<int>
        | Total of AsyncReplyChannel<int64>

    type Message2 =
        | Increment2 of counter: int
        | Get2 of counter: int * AsyncReplyChannel2<int>
        | Total2 of AsyncReplyChannel2<int64>

    let start (cancellationToken: CancellationToken) =
        MailboxProcessor.Start(
            (fun inbox ->
                let counts = Array.zeroCreate<int> Counters

                let rec loop () =
                    async {
                        match! inbox.Receive() with
                        | Increment counter -> counts[counter] <- counts[counter] + 1
                        | Get(counter, reply) -> reply.Reply counts[counter]
                        | Total reply -> reply.Reply(Array.sumBy int64 counts)

                        return! loop ()
                    }

                loop ()),
            cancellationToken
        )

    let start2 (cancellationToken: CancellationToken) =
        MailboxProcessor2.Start(
            (fun inbox ->
                let counts = Array.zeroCreate<int> Counters

                let rec loop () =
                    async2 {
                        match! inbox.Receive() with
                        | Increment2 counter -> counts[counter] <- counts[counter] + 1
                        | Get2(counter, reply) -> reply.Reply counts[counter]
                        | Total2 reply -> reply.Reply(Array.sumBy int64 counts)

                        return! loop ()
                    }

                loop ()),
            cancellationToken
        )

    type Request =
        | IncrementRequest of counter: int
        | GetRequest of counter: int * TaskCompletionSource<int>
        | TotalRequest of TaskCompletionSource<int64>

    /// What the agent looks like with tasks: an unbounded channel drained by a single reader.
    type ChannelAgent(cancellationToken: CancellationToken) =
        let inbox =
            Channel.CreateUnbounded<Request>(UnboundedChannelOptions(SingleReader = true))

        let counts = Array.zeroCreate<int> Counters

        let handle request =
            match request with
            | IncrementRequest counter -> counts[counter] <- counts[counter] + 1
            | GetRequest(counter, reply) -> reply.SetResult counts[counter]
            | TotalRequest reply -> reply.SetResult(Array.sumBy int64 counts)

        let running =
            task {
                let mutable reading = true

                while reading do
                    let! available = inbox.Reader.WaitToReadAsync(cancellationToken)

                    if available then
                        let mutable request = Unchecked.defaultof<Request>

                        while inbox.Reader.TryRead(&request) do
                            handle request
                    else
                        reading <- false
            }

        member _.Post(request: Request) = inbox.Writer.TryWrite request |> ignore

        member _.PostAndReply(buildRequest: TaskCompletionSource<'Reply> -> Request) =
            let reply = TaskCompletionSource<'Reply>(TaskCreationOptions.RunContinuationsAsynchronously)
            inbox.Writer.TryWrite(buildRequest reply) |> ignore
            reply.Task

        member _.Stop() =
            inbox.Writer.Complete()
            running.Wait()

open MetricsAgent

[<MemoryDiagnoser>]
[<CategoriesColumn>]
[<GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)>]
type MailboxBenchmarks() =
    let cancellation = new CancellationTokenSource()
    let mutable agent = Unchecked.defaultof<MailboxProcessor<Message>>
    let mutable agent2 = Unchecked.defaultof<MailboxProcessor2<Message2>>
    let mutable channelAgent = Unchecked.defaultof<ChannelAgent>

    [<GlobalSetup>]
    member _.Setup() =
        agent <- start cancellation.Token
        agent2 <- start2 cancellation.Token
        channelAgent <- ChannelAgent(cancellation.Token)

    [<GlobalCleanup>]
    member _.Cleanup() =
        channelAgent.Stop()
        cancellation.Cancel()

    /// A burst of 10,000 fire-and-forget increments, then one query that waits for all of them.
    [<Benchmark(Baseline = true, OperationsPerInvoke = Posts)>]
    [<BenchmarkCategory("PostBurst")>]
    member _.Async_PostBurst() =
        for i in 1..Posts do
            agent.Post(Increment(i % Counters))

        Run.async (agent.PostAndAsyncReply Total)

    [<Benchmark(OperationsPerInvoke = Posts)>]
    [<BenchmarkCategory("PostBurst")>]
    member _.Async2_PostBurst() =
        for i in 1..Posts do
            agent2.Post(Increment2(i % Counters))

        Run.async2 (agent2.PostAndAsyncReply Total2)

    [<Benchmark(OperationsPerInvoke = Posts)>]
    [<BenchmarkCategory("PostBurst")>]
    member _.Task_PostBurst() =
        for i in 1..Posts do
            channelAgent.Post(IncrementRequest(i % Counters))

        Run.task (fun () -> channelAgent.PostAndReply TotalRequest)

    /// A caller asking 1,000 questions one after another, each waiting for its answer.
    [<Benchmark(Baseline = true, OperationsPerInvoke = Queries)>]
    [<BenchmarkCategory("RequestReply")>]
    member _.Async_RequestReply() =
        Run.async (
            async {
                let sum = ref 0

                for i in 1..Queries do
                    let! count = agent.PostAndAsyncReply(fun reply -> Get(i % Counters, reply))
                    sum.Value <- sum.Value + count

                return sum.Value
            }
        )

    [<Benchmark(OperationsPerInvoke = Queries)>]
    [<BenchmarkCategory("RequestReply")>]
    member _.Async2_RequestReply() =
        Run.async2 (
            async2 {
                let mutable sum = 0

                for i in 1..Queries do
                    let! count = agent2.PostAndAsyncReply(fun reply -> Get2(i % Counters, reply))
                    sum <- sum + count

                return sum
            }
        )

    [<Benchmark(OperationsPerInvoke = Queries)>]
    [<BenchmarkCategory("RequestReply")>]
    member _.Task_RequestReply() =
        Run.task (fun () ->
            task {
                let mutable sum = 0

                for i in 1..Queries do
                    let! count = channelAgent.PostAndReply(fun reply -> GetRequest(i % Counters, reply))
                    sum <- sum + count

                return sum
            })
