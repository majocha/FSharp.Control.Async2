module AsyncSeq2.Tests.MaxMin

open System

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// ColdTask.max
// ColdTask.min
// ColdTask.tryMax
// ColdTask.tryMin
// ColdTask.maxBy
// ColdTask.minBy
// ColdTask.maxByAsync
// ColdTask.minByAsync
//

type MinMax =
    | Max = 0
    | Min = 1
    | MaxBy = 2
    | MinBy = 3
    | MaxByAsync = 4
    | MinByAsync = 5

module MinMax =
    let getFunction =
        function
        | MinMax.Max -> ColdTask.max
        | MinMax.Min -> ColdTask.min
        | MinMax.MaxBy -> ColdTask.maxBy id
        | MinMax.MinBy -> ColdTask.minBy id
        | MinMax.MaxByAsync -> ColdTask.maxByAsync Task.fromResult
        | MinMax.MinByAsync -> ColdTask.minByAsync Task.fromResult
        | _ -> failwith "impossible"

    let getByFunction =
        function
        | MinMax.MaxBy -> ColdTask.maxBy
        | MinMax.MinBy -> ColdTask.minBy
        | MinMax.MaxByAsync -> fun by -> ColdTask.maxByAsync (by >> Task.fromResult)
        | MinMax.MinByAsync -> fun by -> ColdTask.minByAsync (by >> Task.fromResult)
        | _ -> failwith "impossible"

    let getAll () =
        [ MinMax.Max; MinMax.Min; MinMax.MaxBy; MinMax.MinBy; MinMax.MaxByAsync; MinMax.MinByAsync ]
        |> List.map getFunction

    let getAllMin () =
        [ MinMax.Min; MinMax.MinBy; MinMax.MinByAsync ]
        |> List.map getFunction

    let getAllMax () =
        [ MinMax.Max; MinMax.MaxBy; MinMax.MaxByAsync ]
        |> List.map getFunction

    let isMin =
        function
        | MinMax.Min
        | MinMax.MinBy
        | MinMax.MinByAsync -> true
        | _ -> false

    let isMax = isMin >> not


type AllMinMaxFunctions() as this =
    inherit TheoryData<MinMax>()

    do
        this.Add MinMax.Max
        this.Add MinMax.Min
        this.Add MinMax.MaxBy
        this.Add MinMax.MinBy
        this.Add MinMax.MaxByAsync
        this.Add MinMax.MinByAsync

type JustMin() as this =
    inherit TheoryData<MinMax>()

    do
        this.Add MinMax.Min
        this.Add MinMax.MinBy
        this.Add MinMax.MinByAsync

type JustMax() as this =
    inherit TheoryData<MinMax>()

    do
        this.Add MinMax.Max
        this.Add MinMax.MaxBy
        this.Add MinMax.MaxByAsync

type JustMinMaxBy() as this =
    inherit TheoryData<MinMax>()

    do
        this.Add MinMax.MaxBy
        this.Add MinMax.MinBy
        this.Add MinMax.MaxByAsync
        this.Add MinMax.MinByAsync

module EmptySeq =
    [<Theory; ClassData(typeof<AllMinMaxFunctions>)>]
    let ``Null source raises ArgumentNullException`` (minMaxType: MinMax) =
        let minMax = MinMax.getFunction minMaxType

        assertNullArg <| fun () -> minMax (null: AsyncSeq2<int>)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``Empty sequence raises ArgumentException`` variant =
        let test minMax =
            let data = Gen.getEmptyVariant variant

            fun () -> minMax data |> Task.ignore
            |> should throwAsyncExact typeof<ArgumentException>

        for minMax in MinMax.getAll () do
            test minMax

module Functionality =
    [<Fact>]
    let ``AsyncSeq2-max should return maximum`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! max = ColdTask.max ts
        max |> should equal 'Z'
    }

    [<Fact>]
    let ``AsyncSeq2-maxBy should return maximum of input, not the projection`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! max = ColdTask.maxBy id ts
        max |> should equal 'Z'

        let ts = [ 1..10 ] |> AsyncSeq2.ofList
        let! max = ColdTask.maxBy (~-) ts
        max |> should equal 1 // as negated, -1 is highest, should not return projection, but original
    }

    [<Fact>]
    let ``AsyncSeq2-maxByAsync should return maximum of input, not the projection`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! max = ColdTask.maxByAsync Task.fromResult ts
        max |> should equal 'Z'

        let ts = [ 1..10 ] |> AsyncSeq2.ofList
        let! max = ColdTask.maxByAsync (fun x -> Task.fromResult -x) ts
        max |> should equal 1 // as negated, -1 is highest, should not return projection, but original
    }

    [<Fact>]
    let ``AsyncSeq2-min should return minimum`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! min = ColdTask.min ts
        min |> should equal 'A'
    }

    [<Fact>]
    let ``AsyncSeq2-minBy should return minimum of input, not the projection`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! min = ColdTask.minBy id ts
        min |> should equal 'A'

        let ts = [ 1..10 ] |> AsyncSeq2.ofList
        let! min = ColdTask.minBy (~-) ts
        min |> should equal 10 // as negated, -10 is lowest, should not return projection, but original
    }

    [<Fact>]
    let ``AsyncSeq2-minByAsync should return minimum of input, not the projection`` () = task {
        let ts = [ 'A' .. 'Z' ] |> AsyncSeq2.ofList
        let! min = ColdTask.minByAsync Task.fromResult ts
        min |> should equal 'A'

        let ts = [ 1..10 ] |> AsyncSeq2.ofList
        let! min = ColdTask.minByAsync (fun x -> Task.fromResult -x) ts
        min |> should equal 10 // as negated, 1 is highest, should not return projection, but original
    }


module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-max, maxBy, maxByAsync returns maximum`` variant = task {
        let ts = Gen.getSeqImmutable variant

        for max in MinMax.getAllMax () do
            let! max = max ts
            max |> should equal 10
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-min, minBy, minByAsync returns minimum`` variant = task {
        let ts = Gen.getSeqImmutable variant

        for min in MinMax.getAllMin () do
            let! min = min ts
            min |> should equal 1
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-maxBy, maxByAsync returns maximum after projection`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! max = ts |> ColdTask.maxBy (fun x -> -x)
        max |> should equal 1 // because -1 maps to item '1'
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-minBy, minByAsync returns minimum after projection`` variant = task {
        let ts = Gen.getSeqImmutable variant
        let! min = ts |> ColdTask.minBy (fun x -> -x)
        min |> should equal 10 // because -10 maps to item 10
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-max, maxBy, maxByAsync prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield i // 2
            i <- i + 1
            yield i // 3
            yield i + 1 // 4
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.max |> Task.map (should equal 4)
        do! ts |> ColdTask.maxBy (~-) |> Task.map (should equal 6) // next iteration & negation "-6" maps to "6"

        do!
            ts
            |> ColdTask.maxByAsync Task.fromResult
            |> Task.map (should equal 12) // no negation

        i |> should equal 12
    }

    [<Fact>]
    let ``AsyncSeq2-min, minBy, minByAsync prove we execute after-effects test`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield i // 2
            i <- i + 1
            yield i // 3
            yield i + 1 // 4
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.min |> Task.map (should equal 2)
        do! ts |> ColdTask.minBy (~-) |> Task.map (should equal 8) // next iteration & negation

        do!
            ts
            |> ColdTask.minByAsync Task.fromResult
            |> Task.map (should equal 10) // no negation

        i |> should equal 12
    }


    [<Theory; ClassData(typeof<JustMax>)>]
    let ``AsyncSeq2-max with sequence that changes length`` (minMax: MinMax) = task {
        let max = MinMax.getFunction minMax
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        do! max ts |> Task.map (should equal 10)
        do! max ts |> Task.map (should equal 20) // mutable state dangers!!
        do! max ts |> Task.map (should equal 30) // id
    }

    [<Theory; ClassData(typeof<JustMin>)>]
    let ``AsyncSeq2-min with sequence that changes length`` (minMax: MinMax) = task {
        let min = MinMax.getFunction minMax
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        do! min ts |> Task.map (should equal 1)
        do! min ts |> Task.map (should equal 1) // same min after changing state
        do! min ts |> Task.map (should equal 1) // id
    }

    [<Theory; ClassData(typeof<JustMinMaxBy>)>]
    let ``AsyncSeq2-minBy, maxBy with sequence that changes length`` (minMax: MinMax) =
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 10
            yield! [ 1..i ]
        }

        let test minMaxFn v =
            if MinMax.isMin minMax then
                // this ensures the "min" version behaves like the "max" version
                minMaxFn (~-) ts |> Task.map (should equal v)
            else
                minMaxFn id ts |> Task.map (should equal v)

        task {
            do! test (MinMax.getByFunction minMax) 10
            do! test (MinMax.getByFunction minMax) 20
            do! test (MinMax.getByFunction minMax) 30
        }


module TryMaxMin =
    [<Fact>]
    let ``AsyncSeq2-tryMax returns None for null source`` () =
        assertNullArg
        <| fun () -> ColdTask.tryMax (null: AsyncSeq2<int>)

    [<Fact>]
    let ``AsyncSeq2-tryMin returns None for null source`` () =
        assertNullArg
        <| fun () -> ColdTask.tryMin (null: AsyncSeq2<int>)

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryMax returns None on empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> ColdTask.tryMax
        result |> should equal None
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-tryMin returns None on empty`` variant = task {
        let! result = Gen.getEmptyVariant variant |> ColdTask.tryMin
        result |> should equal None
    }

    [<Fact>]
    let ``AsyncSeq2-tryMax returns Some for singleton`` () = task {
        let! result = AsyncSeq2.singleton 42 |> ColdTask.tryMax
        result |> should equal (Some 42)
    }

    [<Fact>]
    let ``AsyncSeq2-tryMin returns Some for singleton`` () = task {
        let! result = AsyncSeq2.singleton 42 |> ColdTask.tryMin
        result |> should equal (Some 42)
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryMax returns Some max of sequence`` variant = task {
        let! result = Gen.getSeqImmutable variant |> ColdTask.tryMax
        result |> should equal (Some 10)
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-tryMin returns Some min of sequence`` variant = task {
        let! result = Gen.getSeqImmutable variant |> ColdTask.tryMin
        result |> should equal (Some 1)
    }

    [<Fact>]
    let ``AsyncSeq2-tryMax and max agree on non-empty sequence`` () = task {
        let ts = AsyncSeq2.ofList [ 3; 1; 4; 1; 5; 9; 2; 6 ]
        let! viaMax = ts |> ColdTask.max
        let ts2 = AsyncSeq2.ofList [ 3; 1; 4; 1; 5; 9; 2; 6 ]
        let! viaTryMax = ts2 |> ColdTask.tryMax
        viaTryMax |> should equal (Some viaMax)
    }

    [<Fact>]
    let ``AsyncSeq2-tryMin and min agree on non-empty sequence`` () = task {
        let ts = AsyncSeq2.ofList [ 3; 1; 4; 1; 5; 9; 2; 6 ]
        let! viaMin = ts |> ColdTask.min
        let ts2 = AsyncSeq2.ofList [ 3; 1; 4; 1; 5; 9; 2; 6 ]
        let! viaTryMin = ts2 |> ColdTask.tryMin
        viaTryMin |> should equal (Some viaMin)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryMax re-iteration reflects side effects`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! first = ts |> ColdTask.tryMax
        let! second = ts |> ColdTask.tryMax
        first |> should equal (Some 10)
        second |> should equal (Some 20)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-tryMin re-iteration reflects side effects`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let! first = ts |> ColdTask.tryMin
        let! second = ts |> ColdTask.tryMin
        first |> should equal (Some 1)
        second |> should equal (Some 11)
    }
