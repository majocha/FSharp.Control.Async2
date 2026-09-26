module AsyncSeq2.Tests.Windowed

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.windowed
//

module EmptySeq =
    [<Fact>]
    let ``AsyncSeq2-windowed with null source raises`` () = assertNullArg <| fun () -> AsyncSeq2.windowed 1 null

    [<Fact>]
    let ``AsyncSeq2-windowed with zero raises ArgumentException before awaiting`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.windowed 0 |> ignore // throws eagerly, before enumeration
        |> should throw typeof<System.ArgumentException>

    [<Fact>]
    let ``AsyncSeq2-windowed with negative raises ArgumentException before awaiting`` () =
        fun () -> (AsyncSeq2.empty<int> ()) |> AsyncSeq2.windowed -1 |> ignore
        |> should throw typeof<System.ArgumentException>

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-windowed on empty sequence yields empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.windowed 1
        |> verifyEmpty

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-windowed(99) on empty sequence yields empty`` variant =
        Gen.getEmptyVariant variant
        |> AsyncSeq2.windowed 99
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-windowed on singleton with windowSize=2 yields empty`` () = asyncSeq2 { yield 42 } |> AsyncSeq2.windowed 2 |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-windowed on singleton with windowSize=1 yields one window`` () = task {
        let! windows =
            asyncSeq2 { yield 42 }
            |> AsyncSeq2.windowed 1
            |> ColdTask.toListAsync

        windows |> should equal [ [| 42 |] ]
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-windowed(1) returns each element wrapped in an array`` variant = task {
        let! windows =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.windowed 1
            |> ColdTask.toArrayAsync

        windows |> Array.length |> should equal 10

        windows
        |> Array.iteri (fun i w -> w |> should equal [| i + 1 |])
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-windowed(2) returns consecutive overlapping pairs as arrays`` variant = task {
        let! windows =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.windowed 2
            |> ColdTask.toArrayAsync

        windows |> Array.length |> should equal 9
        windows |> Array.head |> should equal [| 1; 2 |]
        windows |> Array.last |> should equal [| 9; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-windowed(3) returns correct sliding windows`` variant = task {
        let! windows =
            Gen.getSeqImmutable variant
            |> AsyncSeq2.windowed 3
            |> ColdTask.toArrayAsync

        windows |> Array.length |> should equal 8
        windows.[0] |> should equal [| 1; 2; 3 |]
        windows.[1] |> should equal [| 2; 3; 4 |]
        windows.[7] |> should equal [| 8; 9; 10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-windowed output length is source length minus windowSize plus one`` variant = task {
        let source = Gen.getSeqImmutable variant

        let! len3 = source |> AsyncSeq2.windowed 3 |> AsyncSeq2.length |> Async2.StartAsTask
        let! len5 = source |> AsyncSeq2.windowed 5 |> AsyncSeq2.length |> Async2.StartAsTask
        let! len10 = source |> AsyncSeq2.windowed 10 |> AsyncSeq2.length |> Async2.StartAsTask

        len3 |> should equal 8 // 10 - 3 + 1
        len5 |> should equal 6 // 10 - 5 + 1
        len10 |> should equal 1 // 10 - 10 + 1
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-windowed windowSize larger than source yields empty`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.windowed 11
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-windowed windows overlap correctly - each element shared by adjacent windows`` () = task {
        let! windows =
            asyncSeq2 { yield! [ 'A'; 'B'; 'C'; 'D'; 'E' ] }
            |> AsyncSeq2.windowed 3
            |> ColdTask.toListAsync

        windows
        |> should equal [ [| 'A'; 'B'; 'C' |]; [| 'B'; 'C'; 'D' |]; [| 'C'; 'D'; 'E' |] ]

        // Adjacent windows share all but one element
        windows
        |> List.pairwise
        |> List.iter (fun (w1, w2) ->
            // tail of w1 == init of w2
            w1.[1..] |> should equal w2.[.. w2.Length - 2])
    }

    [<Fact>]
    let ``AsyncSeq2-windowed each window array is independent - modifying one does not affect others`` () = task {
        let! windows =
            asyncSeq2 { yield! [ 1..5 ] }
            |> AsyncSeq2.windowed 3
            |> ColdTask.toArrayAsync

        // Mutate first window
        windows.[0].[0] <- 99

        // Second window is unaffected
        windows.[1] |> should equal [| 2; 3; 4 |]
        windows.[0].[0] |> should equal 99
    }

    [<Fact>]
    let ``AsyncSeq2-windowed with large windowSize equal to source length`` () = task {
        let! windows =
            asyncSeq2 { yield! [ 1..5 ] }
            |> AsyncSeq2.windowed 5
            |> ColdTask.toArrayAsync

        windows |> Array.length |> should equal 1
        windows.[0] |> should equal [| 1; 2; 3; 4; 5 |]
    }

    [<Fact>]
    let ``AsyncSeq2-windowed ring buffer correctness - window wraps correctly at buffer boundary`` () = task {
        // Window size 4 over [1..7] — exercises the ring-buffer copy path.
        let! windows =
            asyncSeq2 { yield! [ 1..7 ] }
            |> AsyncSeq2.windowed 4
            |> ColdTask.toArrayAsync

        windows
        |> should equal [| [| 1; 2; 3; 4 |]; [| 2; 3; 4; 5 |]; [| 3; 4; 5; 6 |]; [| 4; 5; 6; 7 |] |]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-windowed gets all items`` variant = task {
        let! windows =
            Gen.getSeqWithSideEffect variant
            |> AsyncSeq2.windowed 2
            |> ColdTask.toArrayAsync

        windows |> Array.length |> should equal 9
        windows |> Array.head |> should equal [| 1; 2 |]
        windows |> Array.last |> should equal [| 9; 10 |]
    }

    [<Fact>]
    let ``AsyncSeq2-windowed executes all source side-effects`` () = task {
        let mutable sideEffects = 0

        let ts = asyncSeq2 {
            sideEffects <- sideEffects + 1
            yield 1
            sideEffects <- sideEffects + 1
            yield 2
            sideEffects <- sideEffects + 1
        }

        do! ts |> AsyncSeq2.windowed 2 |> consumeTaskSeq
        sideEffects |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-windowed consumes every source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! windows = ts |> AsyncSeq2.windowed 3 |> ColdTask.toListAsync
        count |> should equal 5
        windows |> List.length |> should equal 3
    }

    [<Fact>]
    let ``AsyncSeq2-windowed propagates exception from source`` () =
        let items = asyncSeq2 {
            yield 1
            yield 2
            failwith "boom"
            yield 3
        }

        fun () -> items |> AsyncSeq2.windowed 2 |> consumeTaskSeq
        |> should throwAsyncExact typeof<System.Exception>
