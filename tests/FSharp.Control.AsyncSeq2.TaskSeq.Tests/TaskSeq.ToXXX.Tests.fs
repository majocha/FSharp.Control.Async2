module AsyncSeq2.Tests.``Conversion-To``

open System.Collections.Generic
open System.Threading.Channels

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

////////////////////////////////////////////////////////////////////////////
///                                                                      ///
/// Notes for contributors:                                              ///
///                                                                      ///
/// Conversion functions are expected to return a certain type           ///
/// To prevent accidental change of signature, and because most          ///
/// sequence-like functions can succeed tests interchangeably with       ///
/// different sequence-like signatures, these tests                      ///
/// deliberately have a type annotation to prevent accidental changing   ///
/// of the surface-area signatures.                                      ///
///                                                                      ///
////////////////////////////////////////////////////////////////////////////

module EmptySeq =
    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toArrayAsync with empty`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let! (results: _[]) = tq |> ColdTask.toArrayAsync
        results |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toListAsync with empty`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let! (results: list<_>) = tq |> ColdTask.toListAsync
        results |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toIListAsync with empty`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let! (results: IList<_>) = tq |> ColdTask.toIListAsync
        results |> Seq.toArray |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toResizeArray with empty`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let! (results: ResizeArray<_>) = tq |> ColdTask.toResizeArrayAsync
        results |> Seq.toArray |> should be Empty
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toArray with empty`` variant =
        let tq = Gen.getEmptyVariant variant
        let (results: _[]) = tq |> AsyncSeq2.toArraySync
        results |> should be Empty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toList with empty`` variant =
        let tq = Gen.getEmptyVariant variant
        let (results: list<_>) = tq |> AsyncSeq2.toListSync
        results |> should be Empty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-toSeqCached with empty`` variant =
        let tq = Gen.getEmptyVariant variant
        let (results: seq<_>) = tq |> AsyncSeq2.toSeq
        results |> Seq.toArray |> should be Empty

module Immutable =

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toArrayAsync should succeed`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let! (results: _[]) = tq |> ColdTask.toArrayAsync
        results |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toListAsync should succeed`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let! (results: list<_>) = tq |> ColdTask.toListAsync
        results |> should equal [ 1..10 ]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toIListAsync should succeed`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let! (results: IList<_>) = tq |> ColdTask.toIListAsync
        results |> Seq.toArray |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toResizeArray should succeed`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let! (results: ResizeArray<_>) = tq |> ColdTask.toResizeArrayAsync
        results |> Seq.toArray |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toArray should succeed and be blocking`` variant =
        let tq = Gen.getSeqImmutable variant
        let (results: _[]) = tq |> AsyncSeq2.toArraySync
        results |> should equal [| 1..10 |]

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toList should succeed and be blocking`` variant =
        let tq = Gen.getSeqImmutable variant
        let (results: list<_>) = tq |> AsyncSeq2.toListSync
        results |> should equal [ 1..10 ]

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toSeqCached should succeed and be blocking`` variant =
        let tq = Gen.getSeqImmutable variant
        let (results: seq<_>) = tq |> AsyncSeq2.toSeq
        results |> Seq.toArray |> should equal [| 1..10 |]


module SideEffects =

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toArrayAsync should execute side effects multiple times`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let! (results1: _[]) = tq |> ColdTask.toArrayAsync
        let! (results2: _[]) = tq |> ColdTask.toArrayAsync
        results1 |> should equal [| 1..10 |]
        results2 |> should equal [| 11..20 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toArrayAsync can be applied multiple times to the same sequence`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let! (results1: _[]) = tq |> ColdTask.toArrayAsync
        let! (results2: _[]) = tq |> ColdTask.toArrayAsync
        results1 |> should equal [| 1..10 |]
        results2 |> should equal [| 11..20 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toListAsync should execute side effects multiple times`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let! (results1: list<_>) = tq |> ColdTask.toListAsync
        let! (results2: list<_>) = tq |> ColdTask.toListAsync
        results1 |> should equal [ 1..10 ]
        results2 |> should equal [ 11..20 ]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toIListAsync should execute side effects multiple times`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let! (results1: IList<_>) = tq |> ColdTask.toIListAsync
        let! (results2: IList<_>) = tq |> ColdTask.toIListAsync
        results1 |> Seq.toArray |> should equal [| 1..10 |]
        results2 |> Seq.toArray |> should equal [| 11..20 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toResizeArray should execute side effects multiple times`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let! (results1: ResizeArray<_>) = tq |> ColdTask.toResizeArrayAsync
        let! (results2: ResizeArray<_>) = tq |> ColdTask.toResizeArrayAsync
        results1 |> Seq.toArray |> should equal [| 1..10 |]
        results2 |> Seq.toArray |> should equal [| 11..20 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toArray should execute side effects multiple times`` variant =
        let tq = Gen.getSeqWithSideEffect variant
        let (results1: _[]) = tq |> AsyncSeq2.toArraySync
        let (results2: _[]) = tq |> AsyncSeq2.toArraySync
        results1 |> should equal [| 1..10 |]
        results2 |> should equal [| 11..20 |]

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toList should execute side effects multiple times`` variant =
        let tq = Gen.getSeqWithSideEffect variant
        let (results1: list<_>) = tq |> AsyncSeq2.toListSync
        let (results2: list<_>) = tq |> AsyncSeq2.toListSync
        results1 |> should equal [ 1..10 ]
        results2 |> should equal [ 11..20 ]

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toSeqCached should execute side effects multiple times`` variant =
        let tq = Gen.getSeqWithSideEffect variant
        let (results1: seq<_>) = tq |> AsyncSeq2.toSeq
        let (results2: seq<_>) = tq |> AsyncSeq2.toSeq
        results1 |> Seq.toArray |> should equal [| 1..10 |]
        results2 |> Seq.toArray |> should equal [| 11..20 |]

module Channel =

    [<Fact>]
    let ``AsyncSeq2-toChannelAsync with null writer raises`` () =
        assertNullArg
        <| fun () ->
            ColdTask.toChannelAsync null (AsyncSeq2.ofArray [| 1 |])
            |> ignore

    [<Fact>]
    let ``AsyncSeq2-toChannelAsync with null source raises`` () =
        let ch = Channel.CreateUnbounded<int>()

        assertNullArg
        <| fun () -> ColdTask.toChannelAsync ch.Writer null |> ignore

    [<Fact>]
    let ``AsyncSeq2-ofChannel with null reader raises`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.ofChannel<int> null |> ignore

    [<Fact>]
    let ``AsyncSeq2-toChannelAsync with empty source completes the channel`` () = task {
        let ch = Channel.CreateUnbounded<int>()
        do! ColdTask.toChannelAsync ch.Writer (AsyncSeq2.empty ())
        ch.Reader.Completion.IsCompleted |> should be True
        let! results = AsyncSeq2.ofChannel ch.Reader |> ColdTask.toArrayAsync
        results |> should be Empty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-toChannelAsync writes all elements and completes the channel`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let ch = Channel.CreateUnbounded<int>()
        do! ColdTask.toChannelAsync ch.Writer tq
        let! results = AsyncSeq2.ofChannel ch.Reader |> ColdTask.toArrayAsync
        results |> should equal [| 1..10 |]
        // Completion resolves once the channel is marked done and the buffer is drained
        do! ch.Reader.Completion
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-ofChannel yields all elements written to the channel`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let ch = Channel.CreateUnbounded<int>()
        do! ColdTask.toChannelAsync ch.Writer tq
        let! results = AsyncSeq2.ofChannel ch.Reader |> ColdTask.toArrayAsync
        results |> should equal [| 1..10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-ofChannel ends when channel is completed and drained`` () = task {
        let ch = Channel.CreateUnbounded<int>()
        do! ch.Writer.WriteAsync 42
        do! ch.Writer.WriteAsync 99
        ch.Writer.Complete()
        let! results = AsyncSeq2.ofChannel ch.Reader |> ColdTask.toArrayAsync
        results |> should equal [| 42; 99 |]
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-toChannelAsync executes side effects`` variant = task {
        let tq = Gen.getSeqWithSideEffect variant
        let ch = Channel.CreateUnbounded<int>()
        do! ColdTask.toChannelAsync ch.Writer tq
        let! results = AsyncSeq2.ofChannel ch.Reader |> ColdTask.toArrayAsync
        results |> should equal [| 1..10 |]
    }
