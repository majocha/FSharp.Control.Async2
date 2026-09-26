module AsyncSeq2.Tests.ChooseV

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.chooseV
// TaskCallbacks.chooseVAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg
        <| fun () -> AsyncSeq2.chooseV (fun _ -> ValueNone) null

        assertNullArg
        <| fun () -> TaskCallbacks.chooseVAsync (fun _ -> Task.fromResult ValueNone) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chooseV`` variant = task {
        let! empty =
            Gen.getEmptyVariant variant
            |> AsyncSeq2.chooseV (fun _ -> ValueSome 42)
            |> ColdTask.toListAsync

        List.isEmpty empty |> should be True
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-chooseVAsync`` variant = task {
        let! empty =
            Gen.getEmptyVariant variant
            |> TaskCallbacks.chooseVAsync (fun _ -> task { return ValueSome 42 })
            |> ColdTask.toListAsync

        List.isEmpty empty |> should be True
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseV can convert and filter`` variant = task {
        let chooser number =
            if number <= 5 then
                ValueSome(char number + '@')
            else
                ValueNone

        let ts = Gen.getSeqImmutable variant

        let! letters1 = AsyncSeq2.chooseV chooser ts |> ColdTask.toArrayAsync
        let! letters2 = AsyncSeq2.chooseV chooser ts |> ColdTask.toArrayAsync

        String letters1 |> should equal "ABCDE"
        String letters2 |> should equal "ABCDE"
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseVAsync can convert and filter`` variant = task {
        let chooser number = task {
            return
                if number <= 5 then
                    ValueSome(char number + '@')
                else
                    ValueNone
        }

        let ts = Gen.getSeqImmutable variant

        let! letters1 = TaskCallbacks.chooseVAsync chooser ts |> ColdTask.toArrayAsync
        let! letters2 = TaskCallbacks.chooseVAsync chooser ts |> ColdTask.toArrayAsync

        String letters1 |> should equal "ABCDE"
        String letters2 |> should equal "ABCDE"
    }

module Immutable2 =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseV returns all when chooser always returns ValueSome`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! xs = ts |> AsyncSeq2.chooseV ValueSome |> ColdTask.toArrayAsync
        xs |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseVAsync returns all when chooser always returns ValueSome`` variant = task {
        let ts = Gen.getSeqImmutable variant

        let! xs =
            ts
            |> TaskCallbacks.chooseVAsync (fun x -> task { return ValueSome x })
            |> ColdTask.toArrayAsync

        xs |> should equal [| 1..10 |]
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseV returns empty when chooser always returns ValueNone`` variant = task {
        let ts = Gen.getSeqImmutable variant

        do! ts |> AsyncSeq2.chooseV (fun _ -> ValueNone) |> verifyEmpty
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-chooseVAsync returns empty when chooser always returns ValueNone`` variant = task {
        let ts = Gen.getSeqImmutable variant

        do!
            ts
            |> TaskCallbacks.chooseVAsync (fun _ -> task { return ValueNone })
            |> verifyEmpty
    }

    [<Fact>]
    let ``AsyncSeq2-chooseV with singleton sequence and ValueSome chooser returns singleton`` () = task {
        let! xs =
            asyncSeq2 { yield 42 }
            |> AsyncSeq2.chooseV (fun x -> ValueSome(x * 2))
            |> ColdTask.toListAsync

        xs |> should equal [ 84 ]
    }

    [<Fact>]
    let ``AsyncSeq2-chooseV with singleton sequence and ValueNone chooser returns empty`` () =
        asyncSeq2 { yield 42 }
        |> AsyncSeq2.chooseV (fun _ -> ValueNone)
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-chooseV can change the element type`` () = task {
        // choose maps int -> string voption, verifying type-changing behavior
        let chooser n =
            if n % 2 = 0 then
                ValueSome(sprintf "even-%d" n)
            else
                ValueNone

        let! xs =
            asyncSeq2 { yield! [ 1..6 ] }
            |> AsyncSeq2.chooseV chooser
            |> ColdTask.toListAsync

        xs |> should equal [ "even-2"; "even-4"; "even-6" ]
    }

    [<Fact>]
    let ``AsyncSeq2-chooseVAsync can change the element type`` () = task {
        let chooser n = task {
            return
                if n % 2 = 0 then
                    ValueSome(sprintf "even-%d" n)
                else
                    ValueNone
        }

        let! xs =
            asyncSeq2 { yield! [ 1..6 ] }
            |> TaskCallbacks.chooseVAsync chooser
            |> ColdTask.toListAsync

        xs |> should equal [ "even-2"; "even-4"; "even-6" ]
    }

module SideEffects =
    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chooseV applied multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let chooser x number =
            if number <= x then
                ValueSome(char number + '@')
            else
                ValueNone

        let! lettersA = ts |> AsyncSeq2.chooseV (chooser 5) |> ColdTask.toArrayAsync
        let! lettersK = ts |> AsyncSeq2.chooseV (chooser 15) |> ColdTask.toArrayAsync
        let! lettersU = ts |> AsyncSeq2.chooseV (chooser 25) |> ColdTask.toArrayAsync

        String lettersA |> should equal "ABCDE"
        String lettersK |> should equal "KLMNO"
        String lettersU |> should equal "UVWXY"
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-chooseVAsync applied multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant

        let chooser x number = task {
            return
                if number <= x then
                    ValueSome(char number + '@')
                else
                    ValueNone
        }

        let! lettersA = TaskCallbacks.chooseVAsync (chooser 5) ts |> ColdTask.toArrayAsync
        let! lettersK = TaskCallbacks.chooseVAsync (chooser 15) ts |> ColdTask.toArrayAsync
        let! lettersU = TaskCallbacks.chooseVAsync (chooser 25) ts |> ColdTask.toArrayAsync

        String lettersA |> should equal "ABCDE"
        String lettersK |> should equal "KLMNO"
        String lettersU |> should equal "UVWXY"
    }

    [<Fact>]
    let ``AsyncSeq2-chooseV evaluates each source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> AsyncSeq2.chooseV (fun x -> if x < 3 then ValueSome x else ValueNone)
            |> ColdTask.toListAsync

        count |> should equal 5 // all 5 elements were visited
        xs |> should equal [ 1; 2 ]
    }

    [<Fact>]
    let ``AsyncSeq2-chooseVAsync evaluates each source element exactly once`` () = task {
        let mutable count = 0

        let ts = asyncSeq2 {
            for i in 1..5 do
                count <- count + 1
                yield i
        }

        let! xs =
            ts
            |> TaskCallbacks.chooseVAsync (fun x -> task { return if x < 3 then ValueSome x else ValueNone })
            |> ColdTask.toListAsync

        count |> should equal 5
        xs |> should equal [ 1; 2 ]
    }
