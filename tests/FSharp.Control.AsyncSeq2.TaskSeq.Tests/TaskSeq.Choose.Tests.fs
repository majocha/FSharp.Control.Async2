module AsyncSeq2.Tests.Choose

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.choose
// TaskCallbacks.chooseAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.choose (fun _ -> None) null

        assertNullArg
        <| fun () -> TaskCallbacks.chooseAsync (fun _ -> Task.fromResult None) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-choose`` variant = task {
        let! empty =
            Gen.getEmptyVariant variant
            |> AsyncSeq2.choose (fun _ -> Some 42)
            |> ColdTask.toListAsync

        List.isEmpty empty |> should be True
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chooseAsync`` variant = task {
        let! empty =
            Gen.getEmptyVariant variant
            |> TaskCallbacks.chooseAsync (fun _ -> task { return Some 42 })
            |> ColdTask.toListAsync

        List.isEmpty empty |> should be True
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-choose can convert and filter`` variant = task {
        let chooser number = if number <= 5 then Some(char number + '@') else None
        let ts = Gen.getSeqImmutable variant

        let! letters1 = AsyncSeq2.choose chooser ts |> ColdTask.toArrayAsync
        let! letters2 = AsyncSeq2.choose chooser ts |> ColdTask.toArrayAsync

        String letters1 |> should equal "ABCDE"
        String letters2 |> should equal "ABCDE"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseAsync can convert and filter`` variant = task {
        let chooser number = task { return if number <= 5 then Some(char number + '@') else None }
        let ts = Gen.getSeqImmutable variant

        let! letters1 = TaskCallbacks.chooseAsync chooser ts |> ColdTask.toArrayAsync
        let! letters2 = TaskCallbacks.chooseAsync chooser ts |> ColdTask.toArrayAsync

        String letters1 |> should equal "ABCDE"
        String letters2 |> should equal "ABCDE"
    }

module Immutable2 =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-choose returns all when chooser always returns Some`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! xs = ts |> AsyncSeq2.choose Some |> ColdTask.toArrayAsync
        xs |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseAsync returns all when chooser always returns Some`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! xs =
            ts
            |> TaskCallbacks.chooseAsync (fun x -> task { return Some x })
            |> ColdTask.toArrayAsync

        xs |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-choose returns empty when chooser always returns None`` variant = task {
        let ts = Gen.getSeqImmutable variant

        do! ts |> AsyncSeq2.choose (fun _ -> None) |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseAsync returns empty when chooser always returns None`` variant = task {
        let ts = Gen.getSeqImmutable variant

        do!
            ts
            |> TaskCallbacks.chooseAsync (fun _ -> task { return None })
            |> verifyEmpty
    }

    [<Fact>]
    let ``AsyncSeq2-choose with singleton sequence and Some chooser returns singleton`` () = task {
        let! xs =
            asyncSeq2 { yield 42 }
            |> AsyncSeq2.choose (fun x -> Some(x * 2))
            |> ColdTask.toListAsync

        xs |> should equal [ 84 ]
    }

    [<Fact>]
    let ``AsyncSeq2-choose with singleton sequence and None chooser returns empty`` () =
        asyncSeq2 { yield 42 }
        |> AsyncSeq2.choose (fun _ -> None)
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-choose can change the element type`` () = task {
        // choose maps int -> string option, verifying type-changing behavior
        let chooser n = if n % 2 = 0 then Some(sprintf "even-%d" n) else None

        let! xs =
            asyncSeq2 { yield! [ 1..6 ] }
            |> AsyncSeq2.choose chooser
            |> ColdTask.toListAsync

        xs |> should equal [ "even-2"; "even-4"; "even-6" ]
    }

    [<Fact>]
    let ``AsyncSeq2-chooseAsync can change the element type`` () = task {
        let chooser n = task { return if n % 2 = 0 then Some(sprintf "even-%d" n) else None }

        let! xs =
            asyncSeq2 { yield! [ 1..6 ] }
            |> TaskCallbacks.chooseAsync chooser
            |> ColdTask.toListAsync

        xs |> should equal [ "even-2"; "even-4"; "even-6" ]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-choose applied multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let chooser x number = if number <= x then Some(char number + '@') else None

        let! lettersA = ts |> AsyncSeq2.choose (chooser 5) |> ColdTask.toArrayAsync
        let! lettersK = ts |> AsyncSeq2.choose (chooser 15) |> ColdTask.toArrayAsync
        let! lettersU = ts |> AsyncSeq2.choose (chooser 25) |> ColdTask.toArrayAsync

        String lettersA |> should equal "ABCDE"
        String lettersK |> should equal "KLMNO"
        String lettersU |> should equal "UVWXY"
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chooseAsync applied multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let chooser x number = task { return if number <= x then Some(char number + '@') else None }

        let! lettersA = TaskCallbacks.chooseAsync (chooser 5) ts |> ColdTask.toArrayAsync
        let! lettersK = TaskCallbacks.chooseAsync (chooser 15) ts |> ColdTask.toArrayAsync
        let! lettersU = TaskCallbacks.chooseAsync (chooser 25) ts |> ColdTask.toArrayAsync

        String lettersA |> should equal "ABCDE"
        String lettersK |> should equal "KLMNO"
        String lettersU |> should equal "UVWXY"
    }

    [<Fact>]
    let ``AsyncSeq2-choose evaluates each source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> AsyncSeq2.choose (fun x -> if x < 3 then Some x else None)
            |> ColdTask.toListAsync

        count |> should equal 5 // all 5 elements were visited
        xs |> should equal [ 1; 2 ]
    }

    [<Fact>]
    let ``AsyncSeq2-chooseAsync evaluates each source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> TaskCallbacks.chooseAsync (fun x -> task { return if x < 3 then Some x else None })
            |> ColdTask.toListAsync

        count |> should equal 5
        xs |> should equal [ 1; 2 ]
    }
