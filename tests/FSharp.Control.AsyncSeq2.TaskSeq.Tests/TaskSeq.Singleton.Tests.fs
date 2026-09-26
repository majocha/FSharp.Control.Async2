module AsyncSeq2.Tests.Singleton

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

//
// AsyncSeq2.singleton
//

module EmptySeq =

    [<Theory; ClassData(typeof<TestEmptyVariants>)>]
    let ``AsyncSeq2-singleton with empty has length one`` variant =
        asyncSeq2 {
            yield! AsyncSeq2.singleton 10
            yield! Gen.getEmptyVariant variant
        }
        |> ColdTask.exactlyOne
        |> Task.map (should equal 10)

module SideEffects =
    [<Fact>]
    let ``AsyncSeq2-singleton with a mutable value`` () =
        let mutable x = 0
        let ts = AsyncSeq2.singleton x
        x <- x + 1

        // mutable value is dereferenced when passed to a function
        ts |> ColdTask.exactlyOne |> Task.map (should equal 0)

    [<Fact>]
    let ``AsyncSeq2-singleton with a ref cell`` () =
        let x = ref 0
        let ts = AsyncSeq2.singleton x
        x.Value <- x.Value + 1

        ts
        |> ColdTask.exactlyOne
        |> Task.map (fun x -> x.Value |> should equal 1)

module Other =
    [<Fact>]
    let ``AsyncSeq2-singleton creates a sequence of one`` () =
        AsyncSeq2.singleton 42
        |> ColdTask.exactlyOne
        |> Task.map (should equal 42)

    [<Fact>]
    let ``AsyncSeq2-singleton with null as value`` () =
        AsyncSeq2.singleton null
        |> ColdTask.exactlyOne
        |> Task.map (should be Null)

    [<Fact>]
    let ``AsyncSeq2-singleton can be yielded multiple times`` () =
        let singleton = AsyncSeq2.singleton 42

        asyncSeq2 {
            yield! singleton
            yield! singleton
            yield! singleton
            yield! singleton
        }
        |> AsyncSeq2.toListSync
        |> should equal [ 42; 42; 42; 42 ]

    [<Fact>]
    let ``AsyncSeq2-singleton with isEmpty`` () =
        AsyncSeq2.singleton 42
        |> AsyncSeq2.isEmpty |> Async2.StartAsTask
        |> Task.map (should be False)

    [<Fact>]
    let ``AsyncSeq2-singleton with append`` () =
        AsyncSeq2.singleton 42
        |> AsyncSeq2.append (AsyncSeq2.singleton 42)
        |> AsyncSeq2.toListSync
        |> should equal [ 42; 42 ]

    [<Theory; ClassData(typeof<TestImmTaskSeq>)>]
    let ``AsyncSeq2-singleton with collect`` variant =
        Gen.getSeqImmutable variant
        |> AsyncSeq2.collect AsyncSeq2.singleton
        |> verify1To10

    [<Fact>]
    let ``AsyncSeq2-singleton does not throw when getting Current before MoveNext`` () = task {
        let enumerator = (AsyncSeq2.singleton 42).GetAsyncEnumerator()
        let defaultValue = enumerator.Current // should return the default value for int
        defaultValue |> should equal 0
    }

    [<Fact>]
    let ``AsyncSeq2-singleton does not throw when getting Current after last MoveNext`` () = task {
        let enumerator = (AsyncSeq2.singleton 42).GetAsyncEnumerator()
        let! isNext = enumerator.MoveNextAsync()
        isNext |> should be True
        let value = enumerator.Current // the first and only value
        value |> should equal 42

        // move past the end
        let! isNext = enumerator.MoveNextAsync()
        isNext |> should be False
        let defaultValue = enumerator.Current // should return the default value for int
        defaultValue |> should equal 0
    }

    [<Fact>]
    let ``AsyncSeq2-singleton multiple MoveNext is fine`` () = task {
        let enumerator = (AsyncSeq2.singleton 42).GetAsyncEnumerator()
        let! isNext = enumerator.MoveNextAsync()
        isNext |> should be True
        let! _ = enumerator.MoveNextAsync()
        let! _ = enumerator.MoveNextAsync()
        let! _ = enumerator.MoveNextAsync()
        let! isNext = enumerator.MoveNextAsync()
        isNext |> should be False

        // should return the default value for int after moving past the end
        let defaultValue = enumerator.Current
        defaultValue |> should equal 0
    }
