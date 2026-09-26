module AsyncSeq2.Tests.``Conversion-From``

open Xunit
open FsUnit.Xunit
open System.Threading

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

let validateSequence sq =
    ColdTask.toArrayAsync sq
    |> Task.map (Seq.toArray >> should equal [| 0..9 |])

module EmptySeq =
    [<Fact>]
    let ``Null source is invalid`` () =
        // note: ofList and its variants do not have null as proper value
        assertNullArg <| fun () -> AsyncSeq2.ofAsync2Array null
        assertNullArg <| fun () -> AsyncSeq2.ofAsync2Seq null
        assertNullArg <| fun () -> AsyncSeq2.ofTaskArray null
        assertNullArg <| fun () -> AsyncSeq2.ofTaskSeq null
        assertNullArg <| fun () -> AsyncSeq2.ofResizeArray null
        assertNullArg <| fun () -> AsyncSeq2.ofArray null
        assertNullArg <| fun () -> AsyncSeq2.ofSeq null

    [<Fact>]
    let ``AsyncSeq2-ofAsync2Array with empty set`` () =
        Array.init 0 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2Array
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofAsync2List with empty set`` () =
        List.init 0 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2List
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofAsync2Seq with empty set`` () =
        Seq.init 0 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2Seq
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofTaskArray with empty set`` () =
        Array.init 0 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskArray
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofTaskList with empty set`` () =
        List.init 0 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskList
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofTaskSeq with empty set`` () =
        Seq.init 0 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskSeq
        |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofResizeArray with empty set`` () = ResizeArray() |> AsyncSeq2.ofResizeArray |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofArray with empty set`` () = Array.init 0 id |> AsyncSeq2.ofArray |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofList with empty set`` () = List.init 0 id |> AsyncSeq2.ofList |> verifyEmpty

    [<Fact>]
    let ``AsyncSeq2-ofSeq with empty set`` () = Seq.init 0 id |> AsyncSeq2.ofSeq |> verifyEmpty


module Immutable =
    [<Fact>]
    let ``AsyncSeq2-ofAsync2Array should succeed`` () =
        Array.init 10 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2Array
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofAsync2List should succeed`` () =
        List.init 10 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2List
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofAsync2Seq should succeed`` () =
        Seq.init 10 (fun x -> async2 { return x })
        |> AsyncSeq2.ofAsync2Seq
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofTaskArray should succeed`` () =
        Array.init 10 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskArray
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofTaskList should succeed`` () =
        List.init 10 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskList
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofTaskSeq should succeed`` () =
        Seq.init 10 (fun x -> task { return x })
        |> AsyncSeq2.ofTaskSeq
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofResizeArray should succeed`` () =
        ResizeArray [ 0..9 ]
        |> AsyncSeq2.ofResizeArray
        |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofArray should succeed`` () = Array.init 10 id |> AsyncSeq2.ofArray |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofList should succeed`` () = List.init 10 id |> AsyncSeq2.ofList |> validateSequence

    [<Fact>]
    let ``AsyncSeq2-ofSeq should succeed`` () = Seq.init 10 id |> AsyncSeq2.ofSeq |> validateSequence

module SideEffects =
    [<Fact>]
    let ``ofAsync2Seq re-evaluates cold computations with the enumeration token`` () = task {
        use cts = new CancellationTokenSource()
        let mutable calls = 0
        let computations = seq {
            for i in 1..3 do
                yield async2 {
                    let! token = Async2.CancellationToken
                    token |> should equal cts.Token
                    calls <- calls + 1
                    return i
                }
        }
        let source = AsyncSeq2.ofAsync2Seq computations
        calls |> should equal 0
        for pass in 1..2 do
            let! values = Async2.StartAsTask(AsyncSeq2.toArray source, cancellationToken = cts.Token)
            values |> should equal [| 1; 2; 3 |]
            calls |> should equal (pass * 3)
    }

    [<Fact>]
    let ``ofSeq re-evaluates the underlying source seq on each re-enumeration`` () = task {
        let mutable count = 0

        // a lazy IEnumerable — each GetEnumerator() call re-executes the body
        let lazySeq = seq {
            for i in 1..3 do
                count <- count + 1
                yield i
        }

        let ts = AsyncSeq2.ofSeq lazySeq
        let! arr1 = ts |> ColdTask.toArrayAsync
        // each item triggered the side effect once
        count |> should equal 3

        let! arr2 = ts |> ColdTask.toArrayAsync
        // the underlying seq is re-traversed on the second GetAsyncEnumerator call
        count |> should equal 6
        arr1 |> should equal arr2
    }

    [<Fact>]
    let ``ofTaskSeq with lazy seq of tasks re-creates tasks on each re-enumeration`` () = task {
        let mutable count = 0

        // a lazy IEnumerable of Task objects — each seq iteration creates fresh Task objects
        let lazyTaskSeq = seq {
            for i in 1..3 do
                yield task {
                    count <- count + 1
                    return i
                }
        }

        let ts = AsyncSeq2.ofTaskSeq lazyTaskSeq
        let! arr1 = ts |> ColdTask.toArrayAsync
        count |> should equal 3

        let! arr2 = ts |> ColdTask.toArrayAsync
        // the underlying seq is re-iterated; new Task objects are created and run
        count |> should equal 6
        arr1 |> should equal arr2
    }

    [<Fact>]
    let ``ofTaskArray does not re-run tasks on re-enumeration; task results are cached`` () = task {
        let mutable count = 0

        // tasks are created upfront; they run synchronously to completion when constructed
        let tasks =
            Array.init 3 (fun i -> task {
                count <- count + 1
                return i + 1
            })

        // all three tasks have already completed synchronously
        count |> should equal 3

        let ts = AsyncSeq2.ofTaskArray tasks
        let! arr1 = ts |> ColdTask.toArrayAsync

        // awaiting already-completed tasks does not re-run them
        count |> should equal 3
        arr1 |> should equal [| 1; 2; 3 |]

        let! arr2 = ts |> ColdTask.toArrayAsync
        // the second enumeration re-awaits the same cached task results
        count |> should equal 3
        arr2 |> should equal arr1
    }
