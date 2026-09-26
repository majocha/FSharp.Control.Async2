module AsyncSeq2.Tests.Iter

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

module private PortIter =
    let iter action source =
        AsyncSeq2.iter action source
        |> Async2.StartAsTask

    let iterAsync (action: 'T -> System.Threading.Tasks.Task<unit>) (source: AsyncSeq2<'T>) =
        AsyncSeq2.iterAsync (fun item -> Async2.Await(action item)) source
        |> Async2.StartAsTask

//
// PortIter.iter
// ColdTask.iteri
// PortIter.iterAsync
// ColdTask.iteriAsync
//

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        assertNullArg <| fun () -> PortIter.iter (fun _ -> ()) null

        assertNullArg
        <| fun () -> ColdTask.iteri (fun _ _ -> ()) null

        assertNullArg
        <| fun () -> PortIter.iterAsync (fun _ -> Task.fromResult ()) null

        assertNullArg
        <| fun () -> ColdTask.iteriAsync (fun _ _ -> Task.fromResult ()) null

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-iteri does nothing on empty sequences`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let mutable sum = -1
        do! tq |> ColdTask.iteri (fun i _ -> sum <- sum + i)
        sum |> should equal -1
    }

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-iter does nothing on empty sequences`` variant = task {
        let tq = Gen.getEmptyVariant variant
        let mutable sum = -1
        do! tq |> PortIter.iter (fun i -> sum <- sum + i)
        sum |> should equal -1
    }

module Immutable =
    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-iteri should go over all items`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let mutable sum = 0
        do! tq |> ColdTask.iteri (fun i _ -> sum <- sum + i)
        sum |> should equal 45 // index starts at 0
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-iter should go over all items`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let mutable sum = 0
        do! tq |> PortIter.iter (fun item -> sum <- sum + item)
        sum |> should equal 55 // task-dummies started at 1
    }


    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-iter multiple iterations over same sequence`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let mutable sum = 0
        do! tq |> PortIter.iter (fun item -> sum <- sum + item)
        do! tq |> PortIter.iter (fun item -> sum <- sum + item)
        do! tq |> PortIter.iter (fun item -> sum <- sum + item)
        do! tq |> PortIter.iter (fun item -> sum <- sum + item)
        sum |> should equal 220 // immutable tasks, so 'item' does not change, just 4 x 55
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-iteriAsync should go over all items`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let mutable sum = 0

        do!
            tq
            |> ColdTask.iteriAsync (fun i _ -> task { sum <- sum + i })

        sum |> should equal 45 // index starts at 0
    }

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-iterAsync should go over all items`` variant = task {
        let tq = Gen.getSeqImmutable variant
        let mutable sum = 0

        do!
            tq
            |> PortIter.iterAsync (fun item -> task { sum <- sum + item })

        sum |> should equal 55 // task-dummies started at 1
    }

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-iter prove we execute empty-seq side-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iteri prove we execute empty-seq side-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iterAsync prove we execute empty-seq side-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iteriAsync prove we execute empty-seq side-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iter prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iteri prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iterAsync prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        do! ts |> PortIter.iterAsync (fun _ -> task { return () })
        i |> should equal 9
    }

    [<Fact>]
    let ``AsyncSeq2-iteriAsync prove we execute after-effects`` () = task {
        let mutable i = 0

        let ts = asyncSeq2 {
            i <- i + 1
            i <- i + 1
            yield 42
            i <- i + 1 // we should get here
        }

        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        do! ts |> ColdTask.iteriAsync (fun _ _ -> task { return () })
        i |> should equal 9
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-iter should go over all items`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        do! ts |> PortIter.iter (fun _ -> ())
        // incl. the iteration of 'last', we reach 40
        do! ts |> ColdTask.last |> Task.map (should equal 40)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-iteri show that side effects are executed multiple times`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        do! ts |> ColdTask.iteri (fun _ _ -> ())
        // incl. the iteration of 'last', we reach 40
        do! ts |> ColdTask.last |> Task.map (should equal 40)
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-iter multiple iterations over same sequence`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let mutable sum = 0

        do! PortIter.iter (fun item -> sum <- sum + item) ts
        do! PortIter.iter (fun item -> sum <- sum + item) ts
        do! PortIter.iter (fun item -> sum <- sum + item) ts
        do! PortIter.iter (fun item -> sum <- sum + item) ts

        sum |> should equal 820 // side-effected tasks, so 'item' DOES CHANGE, each next iteration starts 10 higher
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-iteriAsync should go over all items`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let mutable sum = 0

        do!
            ts
            |> ColdTask.iteriAsync (fun i _ -> task { sum <- sum + i })

        sum |> should equal 45 // index starts at 0
    }

    [<Theory; ClassData(typeof<TestSideEffectTaskSeq>)>]
    let ``AsyncSeq2-iterAsync should go over all items`` variant = task {
        let ts = Gen.getSeqWithSideEffect variant
        let mutable sum = 0
        do! PortIter.iterAsync (fun item -> task { sum <- sum + item }) ts
        sum |> should equal 55 // task-dummies started at 1
    }
