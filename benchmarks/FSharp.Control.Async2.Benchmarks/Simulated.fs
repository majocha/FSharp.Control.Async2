namespace FSharp.Control.Async2.Benchmarks

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Microsoft.FSharp.Control

/// Stand-ins for the .NET APIs a service calls. Like a database driver, an HTTP client or a socket they
/// return `Task` and `ValueTask`, and a remote call completes asynchronously, resuming on the thread pool.
module Simulated =
    /// A network round trip that answers with `value`.
    let remoteCall (value: 'T) : Task<'T> =
        task {
            do! Task.Yield()
            return value
        }

    /// A network round trip with no answer, such as a write or a backoff pause.
    let roundTrip () : Task =
        task { do! Task.Yield() }

/// Each scenario starts its computation on the calling thread and blocks on the result, so the three
/// flavours pay the same entry cost and the measurement is the workload itself.
module Run =
    let async (computation: Async<'T>) =
        Async.StartImmediateAsTask(computation).GetAwaiter().GetResult()

    let async2 (computation: Async2<'T>) =
        Async2.RunSynchronouslyImmediate computation

    let task (start: unit -> Task<'T>) =
        start().GetAwaiter().GetResult()

/// A socket-like stream over a byte array: most reads are served from data already buffered and complete
/// synchronously, and every `asyncEvery`-th read waits for the network and completes asynchronously.
type SimulatedNetworkStream(data: byte array, asyncEvery: int) =
    inherit Stream()

    let mutable position = 0
    let mutable reads = 0

    member _.Reset() =
        position <- 0
        reads <- 0

    member private _.Copy(destination: Memory<byte>) =
        let count = min destination.Length (data.Length - position)
        data.AsSpan(position, count).CopyTo(destination.Span)
        position <- position + count
        count

    override this.ReadAsync(buffer: Memory<byte>, _: CancellationToken) : ValueTask<int> =
        reads <- reads + 1

        if reads % asyncEvery = 0 then
            ValueTask<int>(
                task {
                    do! Task.Yield()
                    return this.Copy buffer
                }
            )
        else
            ValueTask<int>(this.Copy buffer)

    override this.ReadAsync(buffer: byte array, offset: int, count: int, cancellationToken: CancellationToken) =
        this.ReadAsync(Memory<byte>(buffer, offset, count), cancellationToken).AsTask()

    override this.Read(buffer: byte array, offset: int, count: int) =
        this.Copy(Memory<byte>(buffer, offset, count))

    override _.CanRead = true
    override _.CanSeek = false
    override _.CanWrite = false
    override _.Length = int64 data.Length

    override _.Position
        with get () = int64 position
        and set _ = raise (NotSupportedException())

    override _.Flush() = ()
    override _.Seek(_, _) = raise (NotSupportedException())
    override _.SetLength(_) = raise (NotSupportedException())
    override _.Write(_, _, _) = raise (NotSupportedException())
