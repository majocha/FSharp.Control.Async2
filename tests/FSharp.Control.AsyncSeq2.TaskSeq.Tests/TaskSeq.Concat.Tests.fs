module AsyncSeq2.Tests.Concat

open System
open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.concat - of task seqs
// AsyncSeq2.concat - of seqs
// AsyncSeq2.concat - of lists
// AsyncSeq2.concat - of arrays
// AsyncSeq2.concat - of resizable arrays
//

let validateSequence ts =
    ts
    |> ColdTask.toListAsync
    |> Task.map (List.map string)
    |> Task.map (String.concat "")
    |> Task.map (should equal "123456789101234567891012345678910")

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid (taskseq)`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.concat (null: AsyncSeq2<AsyncSeq2<_>>)

    [<Fact>]
    let ``Null source is invalid (seq)`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.concat (null: AsyncSeq2<seq<_>>)

    [<Fact>]
    let ``Null source is invalid (array)`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.concat (null: AsyncSeq2<array<_>>)

    [<Fact>]
    let ``Null source is invalid (list)`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.concat (null: AsyncSeq2<list<_>>)

    [<Fact>]
    let ``Null source is invalid (resizarray)`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.concat (null: AsyncSeq2<ResizeArray<_>>)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with nested empty task sequences`` variant =
        asyncSeq2 {
            yield Gen.getEmptyVariant variant
            yield Gen.getEmptyVariant variant
            yield Gen.getEmptyVariant variant
        }
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-concat with nested empty sequences`` () =
        asyncSeq2 {
            yield Seq.empty<string>
            yield Seq.empty<string>
            yield Seq.empty<string>
        }
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-concat with nested empty arrays`` () =
        asyncSeq2 {
            yield Array.empty<int>
            yield Array.empty<int>
            yield Array.empty<int>
        }
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-concat with nested empty lists`` () =
        asyncSeq2 {
            yield List.empty<Guid>
            yield List.empty<Guid>
            yield List.empty<Guid>
        }
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-concat with multiple nested empty resizable arrays`` () =
        asyncSeq2 {
            yield ResizeArray(List.empty<byte>)
            yield ResizeArray(List.empty<byte>)
            yield ResizeArray(List.empty<byte>)
        }
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with empty source (taskseq)`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<IAsyncEnumerable<int>> // task seq is empty so this should not raise
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with empty source (seq)`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<int seq> // task seq is empty so this should not raise
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with empty source (list)`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<int list> // task seq is empty so this should not raise
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with empty source (array)`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<int[]> // task seq is empty so this should not raise
        |> AsyncSeq2.concat
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-concat with empty source (resizearray)`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.box
        |> AsyncSeq2.cast<ResizeArray<int>> // task seq is empty so this should not raise
        |> AsyncSeq2.concat
        |> verifyEmpty


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-concat with three sequences of sequences`` variant =
        asyncSeq2 {
            yield Gen.getSeqImmutable variant
            yield Gen.getSeqImmutable variant
            yield Gen.getSeqImmutable variant
        }
        |> AsyncSeq2.concat
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-concat with three sequences of sequences and few empties`` variant =
        asyncSeq2 {
            yield (AsyncSeq2.empty ())
            yield Gen.getSeqImmutable variant
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield Gen.getSeqImmutable variant
            yield (AsyncSeq2.empty ())
            yield Gen.getSeqImmutable variant
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
        }
        |> AsyncSeq2.concat
        |> validateSequence

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-concat throws when one of inner task sequence is null`` variant =
        fun () ->
            asyncSeq2 {
                yield Gen.getSeqImmutable variant
                yield (AsyncSeq2.empty ())
                yield null
            }
            |> AsyncSeq2.concat
            |> consumeTaskSeq
        |> should throwAsyncExact typeof<NullReferenceException>

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-concat executes side effects of nested (taskseq)`` () =
        let mutable i = 0

        asyncSeq2 {
            yield Gen.getSeqImmutable SeqImmutable.ThreadSpinWait
            yield Gen.getSeqImmutable SeqImmutable.ThreadSpinWait

            yield asyncSeq2 {
                yield! [ 1..10 ]
                i <- i + 1
            }
        }
        |> AsyncSeq2.concat
        |> ColdTask.last // consume
        |> Task.map (fun _ -> i |> should equal 1)

    [<Fact>]
    let ``AsyncSeq2-concat executes side effects of nested (seq)`` () =
        let mutable i = 0

        asyncSeq2 {
            yield seq { 1..10 }
            yield seq { 1..10 }

            yield seq {
                yield! [ 1..10 ]
                i <- i + 1
            }
        }
        |> AsyncSeq2.concat
        |> ColdTask.last // consume
        |> Task.map (fun _ -> i |> should equal 1)

    [<Fact>]
    let ``AsyncSeq2-concat executes side effects of nested (array)`` () =
        let mutable i = 0

        asyncSeq2 {
            yield [| 1..10 |]
            yield [| 1..10 |]

            yield [| yield! [ 1..10 ]; i <- i + 1 |]
        }
        |> AsyncSeq2.concat
        |> ColdTask.last // consume
        |> Task.map (fun _ -> i |> should equal 1)

    [<Fact>]
    let ``AsyncSeq2-concat executes side effects of nested (list)`` () =
        let mutable i = 0

        asyncSeq2 {
            yield [ 1..10 ]
            yield [ 1..10 ]

            yield [ yield! [ 1..10 ]; i <- i + 1 ]
        }
        |> AsyncSeq2.concat
        |> ColdTask.last // consume
        |> Task.map (fun _ -> i |> should equal 1)

    [<Fact>]
    let ``AsyncSeq2-concat executes side effects of nested (resizearray)`` () =
        let mutable i = 0

        asyncSeq2 {
            yield ResizeArray [ 1..10 ]
            yield ResizeArray [ 1..10 ]

            yield
                ResizeArray(
                    seq {
                        yield! [ 1..10 ]
                        i <- i + 1
                    }
                )
        }
        |> AsyncSeq2.concat
        |> ColdTask.last // consume
        |> Task.map (fun _ -> i |> should equal 1)

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-concat consumes side effects in empty sequences`` variant =
        let mutable i = 0

        asyncSeq2 {
            yield asyncSeq2 { do i <- i + 1 }
            yield Gen.getSeqImmutable variant // not yield-bang!
            yield (AsyncSeq2.empty ())
            yield asyncSeq2 { do i <- i + 1 }
            yield Gen.getSeqImmutable variant
            yield (AsyncSeq2.empty ())
            yield Gen.getSeqImmutable variant
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield (AsyncSeq2.empty ())
            yield asyncSeq2 { do i <- i + 1 }
        }
        |> AsyncSeq2.concat
        |> validateSequence
        |> Task.map (fun () -> i |> should equal 3)
