module AsyncSeq2.Tests.``Bug #42 -- synchronous`` // see PR #42

open System
open System.Threading.Tasks
open System.Collections.Generic

open Xunit
open FsUnit.Xunit

open Microsoft.FSharp.Control
open Microsoft.FSharp.Control.AsyncSeq2Implementation

[<Fact>]
let ``CE empty asyncSeq2 with MoveNextAsync -- untyped`` () = task {
    let tskSeq = asyncSeq2 { do () }

    Assert.IsAssignableFrom<IAsyncEnumerable<obj>>(tskSeq)
    |> ignore

    do! Assert.moveNextAndCheck false (tskSeq.GetAsyncEnumerator())
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE empty asyncSeq2 with MoveNextAsync -- typed`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant

    Assert.IsAssignableFrom<IAsyncEnumerable<int>>(tskSeq)
    |> ignore

    do! Assert.moveNextAndCheck false (tskSeq.GetAsyncEnumerator())
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE  empty asyncSeq2, GetAsyncEnumerator multiple times`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant
    use _ = tskSeq.GetAsyncEnumerator()
    use _ = tskSeq.GetAsyncEnumerator()
    use _ = tskSeq.GetAsyncEnumerator()
    ()
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE  empty asyncSeq2, GetAsyncEnumerator multiple times and then MoveNextAsync`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant
    use _ = tskSeq.GetAsyncEnumerator()
    use enumerator = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck false enumerator
}

// FIXED!
// Previously: shaky test. Appears that this occasionally raises a NullReferenceException,
// esp when there's stress (i.e. run all at the same time).
// See https://github.com/fsprojects/FSharp.Control.AsyncSeq2/pull/54
[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE empty asyncSeq2, GetAsyncEnumerator + MoveNextAsync multiple times`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant
    use enumerator1 = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck false enumerator1

    // getting the enumerator again
    use enumerator2 = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck false enumerator1 // original should still work without raising
    do! Assert.moveNextAndCheck false enumerator2 // new hone should also work without raising
}

// FIXED!
// This is the simpler version of the above test.
[<Fact>]
let ``BUG #54 CE with empty asyncSeq2 and Delay, crash after GetAsyncEnumerator + MoveNextAsync 2x`` () = task {
    // See: https://github.com/fsprojects/FSharp.Control.AsyncSeq2/pull/54
    let tskSeq = asyncSeq2 { do! Task.Delay(50) |> Task.ofTask }

    use enumerator1 = tskSeq.GetAsyncEnumerator()
    let! (hasNext: bool) = enumerator1.MoveNextAsync()
    hasNext |> should be False

    use enumerator1 = tskSeq.GetAsyncEnumerator()
    let! (hasNext: bool) = enumerator1.MoveNextAsync()
    hasNext |> should be False

    let! (hasNext: bool) = enumerator1.MoveNextAsync() // fail here
    hasNext |> should be False
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE empty asyncSeq2, GetAsyncEnumerator + MoveNextAsync 100x in a loop`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant

    // let's get the enumerator a few times
    for _ in 0..100 do
        use enumerator = tskSeq.GetAsyncEnumerator()
        do! Assert.moveNextAndCheck false enumerator // these are all empty
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE empty asyncSeq2, call Current before MoveNextAsync`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant
    let enumerator = tskSeq.GetAsyncEnumerator()

    // call Current *before* MoveNextAsync
    let current = enumerator.Current
    current |> should equal 0 // we return Unchecked.defaultof, which is Zero in the case of an integer
}

[<Theory; ClassData(typeof<TestEmptyVariants>)>]
let ``CE empty asyncSeq2, call Current after MoveNextAsync returns false`` variant = task {
    let tskSeq = Gen.getEmptyVariant variant
    let enumerator = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck false enumerator // false for empty seq

    // call Current *after* MoveNextAsync returns false
    enumerator.Current |> should equal 0 // we return Unchecked.defaultof, which is Zero in the case of an integer
}

[<Fact>]
let ``CE asyncSeq2, proper two-item task sequence`` () = task {
    let tskSeq = asyncSeq2 {
        yield "foo"
        yield "bar"
    }

    let enum = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck true enum // first item
    enum.Current |> should equal "foo"
    do! Assert.moveNextAndCheck true enum // second item
    enum.Current |> should equal "bar"
    do! Assert.moveNextAndCheck false enum // third item: false
}

[<Fact>]
let ``CE asyncSeq2, proper two-item task sequence -- async variant`` () = task {
    let tskSeq = asyncSeq2 {
        yield "foo"
        do! longDelay ()
        yield "bar"
    }

    let enum = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck true enum // first item
    enum.Current |> should equal "foo"
    do! Assert.moveNextAndCheck true enum // second item
    enum.Current |> should equal "bar"
    do! Assert.moveNextAndCheck false enum // third item: false
}

[<Fact>]
let ``CE asyncSeq2, call Current before MoveNextAsync`` () = task {
    let tskSeq = asyncSeq2 {
        yield "foo"
        yield "bar"
    }

    let enumerator = tskSeq.GetAsyncEnumerator()

    // call Current before MoveNextAsync
    let current = enumerator.Current
    current |> should be Null // we return Unchecked.defaultof
}

[<Fact>]
let ``CE asyncSeq2, call Current after MoveNextAsync returns false`` () = task {
    let tskSeq = asyncSeq2 {
        yield "foo"
        yield "bar"
    }

    let enum = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck true enum // first item
    do! Assert.moveNextAndCheck true enum // second item
    do! Assert.moveNextAndCheck false enum // third item: false

    // call Current *after* MoveNextAsync returns false
    enum.Current |> should be Null // we return Unchecked.defaultof
}

[<Fact>]
let ``CE asyncSeq2, MoveNext once too far`` () = task {
    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    let enum = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheck true enum // first item
    do! Assert.moveNextAndCheck true enum // second item
    do! Assert.moveNextAndCheck false enum // third item: false
    do! Assert.moveNextAndCheck false enum // this used to be an error, see issue #39 and PR #42
}

[<Fact>]
let ``CE asyncSeq2, MoveNext too far`` () = task {
    let tskSeq = asyncSeq2 {
        yield Guid.NewGuid()
        yield Guid.NewGuid()
    }

    // let's call MoveNext multiple times on an empty sequence
    let enum = tskSeq.GetAsyncEnumerator()

    // first get past the post
    do! Assert.moveNextAndCheck true enum // first item
    do! Assert.moveNextAndCheck true enum // second item
    do! Assert.moveNextAndCheck false enum // third item: false

    // then call it bunch of times to ensure we don't get an InvalidOperationException, see issue #39 and PR #42
    for _ in 0..100 do
        do! Assert.moveNextAndCheck false enum

    // after whatever amount of time MoveNextAsync, we can still safely call Current
    enum.Current |> should equal Guid.Empty // we return Unchecked.defaultof, which is Guid.Empty for guids
}

[<Fact>]
let ``CE asyncSeq2, call GetAsyncEnumerator twice, both should have equal behavior`` () = task {
    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    let enum1 = tskSeq.GetAsyncEnumerator()
    let enum2 = tskSeq.GetAsyncEnumerator()

    // enum1
    do! Assert.moveNextAndCheckCurrent true 1 enum1 // first item
    do! Assert.moveNextAndCheckCurrent true 2 enum1 // second item
    do! Assert.moveNextAndCheckCurrent false 0 enum1 // third item: false
    do! Assert.moveNextAndCheckCurrent false 0 enum1 // this used to be an error, see issue #39 and PR #42

    // enum2
    do! Assert.moveNextAndCheckCurrent true 1 enum2 // first item
    do! Assert.moveNextAndCheckCurrent true 2 enum2 // second item
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // third item: false
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // this used to be an error, see issue #39 and PR #42
}

[<Fact>]
let ``CE seq -- comparison --, call GetEnumerator twice`` () = task {
    // this test is for behavioral comparison between the same Async test above with AsyncSeq2
    let sq = seq {
        yield 1
        yield 2
    }

    let enum1 = sq.GetEnumerator()
    let enum2 = sq.GetEnumerator()

    // enum1
    do Assert.seqMoveNextAndCheckCurrent true 1 enum1 // first item
    do Assert.seqMoveNextAndCheckCurrent true 2 enum1 // second item
    do Assert.seqMoveNextAndCheckCurrent false 0 enum1 // third item: false
    do Assert.seqMoveNextAndCheckCurrent false 0 enum1 // this used to be an error, see issue #39 and PR #42

    // enum2
    do Assert.seqMoveNextAndCheckCurrent true 1 enum2 // first item
    do Assert.seqMoveNextAndCheckCurrent true 2 enum2 // second item
    do Assert.seqMoveNextAndCheckCurrent false 0 enum2 // third item: false
    do Assert.seqMoveNextAndCheckCurrent false 0 enum2 // this used to be an error, see issue #39 and PR #42
}


[<Fact>]
let ``CE asyncSeq2, cal GetAsyncEnumerator twice -- in lockstep`` () = task {
    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    let enum1 = tskSeq.GetAsyncEnumerator()
    let enum2 = tskSeq.GetAsyncEnumerator()

    // enum1 & enum2 in lock step
    do! Assert.moveNextAndCheckCurrent true 1 enum1 // first item
    do! Assert.moveNextAndCheckCurrent true 1 enum2 // first item

    do! Assert.moveNextAndCheckCurrent true 2 enum1 // second item
    do! Assert.moveNextAndCheckCurrent true 2 enum2 // second item

    do! Assert.moveNextAndCheckCurrent false 0 enum1 // third item: false
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // third item: false

    do! Assert.moveNextAndCheckCurrent false 0 enum1 // this used to be an error, see issue #39 and PR #42
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // this used to be an error, see issue #39 and PR #42
}

[<Fact>]
let ``CE asyncSeq2, call GetAsyncEnumerator twice -- after full iteration`` () = task {
    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    // enum1
    let enum1 = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheckCurrent true 1 enum1 // first item
    do! Assert.moveNextAndCheckCurrent true 2 enum1 // second item
    do! Assert.moveNextAndCheckCurrent false 0 enum1 // third item: false
    do! Assert.moveNextAndCheckCurrent false 0 enum1 // this used to be an error, see issue #39 and PR #42

    // enum2
    let enum2 = tskSeq.GetAsyncEnumerator()
    do! Assert.moveNextAndCheckCurrent true 1 enum2 // first item
    do! Assert.moveNextAndCheckCurrent true 2 enum2 // second item
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // third item: false
    do! Assert.moveNextAndCheckCurrent false 0 enum2 // this used to be an error, see issue #39 and PR #42
}

[<Fact>]
let ``CE asyncSeq2, call GetAsyncEnumerator twice -- random mixed iteration`` () = task {
    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
        yield 3
    }

    // enum1
    let enum1 = tskSeq.GetAsyncEnumerator()

    // move #1
    do! Assert.moveNextAndCheckCurrent true 1 enum1 // first item

    // enum2
    let enum2 = tskSeq.GetAsyncEnumerator()
    enum1.Current |> should equal 1 // remains the same
    enum2.Current |> should equal 0 // should be at default location

    // move #2
    do! Assert.moveNextAndCheckCurrent true 1 enum2
    enum1.Current |> should equal 1
    enum2.Current |> should equal 1

    // move #2
    do! Assert.moveNextAndCheckCurrent true 2 enum2
    enum1.Current |> should equal 1
    enum2.Current |> should equal 2

    // move #1
    do! Assert.moveNextAndCheckCurrent true 2 enum1
    enum1.Current |> should equal 2
    enum2.Current |> should equal 2

    // move #1
    do! Assert.moveNextAndCheckCurrent true 3 enum1
    enum1.Current |> should equal 3
    enum2.Current |> should equal 2

    // move #1
    do! Assert.moveNextAndCheckCurrent false 0 enum1
    enum1.Current |> should equal 0
    enum2.Current |> should equal 2

    // move #2
    do! Assert.moveNextAndCheckCurrent true 3 enum2
    enum1.Current |> should equal 0
    enum2.Current |> should equal 3

    // move #2
    do! Assert.moveNextAndCheckCurrent false 0 enum2
    enum1.Current |> should equal 0
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    // let's map once, and then again on the new sequence
    let ts1 = tskSeq |> AsyncSeq2.map (fun i -> i + 1)
    let result1 = AsyncSeq2.toArraySync ts1
    let ts2 = ts1 |> AsyncSeq2.map (fun i -> i + 1)
    let result2 = AsyncSeq2.toArraySync ts2 // NRE here

    tskSeq |> AsyncSeq2.toArraySync |> should equal [| 1; 2 |]
    result1 |> should equal [| 2; 3 |]
    result2 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result - alternative #1`` () = task {
    let tskSeq1 = asyncSeq2 {
        yield 1
        yield 2
    }

    // [ 2; 3]
    let tskSeq2 = asyncSeq2 {
        for i in tskSeq1 do
            yield i + 1
    }

    // [ 3; 4]
    let tskSeq3 = asyncSeq2 {
        for i in tskSeq2 do
            yield i + 1
    }

    let result3 = AsyncSeq2.toArraySync tskSeq3

    result3 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result - alternative #2`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let tskSeq1 = asyncSeq2 {
        yield 1
        yield 2
    }

    let result1 = AsyncSeq2.toArraySync tskSeq1
    result1 |> should equal [| 1; 2 |]

    // [ 2; 3]
    let tskSeq2 = asyncSeq2 {
        for i in tskSeq1 do
            yield i + 1
    }

    let result2 = AsyncSeq2.toArraySync tskSeq2
    result2 |> should equal [| 2; 3 |]

    // [ 3; 4]
    let tskSeq3 = asyncSeq2 {
        for i in tskSeq2 do // NRE here
            yield i + 1
    }

    let! result3 = ColdTask.toArrayAsync tskSeq3 // from here
    result3 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result - alternative #3`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let tskSeq1 = asyncSeq2 {
        yield 1
        yield 2
    }

    let result1 = AsyncSeq2.toArraySync tskSeq1
    result1 |> should equal [| 1; 2 |]

    // [ 2; 3]
    let tskSeq2 = asyncSeq2 {
        yield! asyncSeq2 {
            for i in tskSeq1 do
                yield i + 1
        }
    }

    let result2 = AsyncSeq2.toArraySync tskSeq2
    result2 |> should equal [| 2; 3 |]

    // [ 3; 4]
    let tskSeq3 = asyncSeq2 {
        yield! asyncSeq2 { // NRE here
            for i in tskSeq2 do
                yield i + 1
        }
    }

    let result3 = AsyncSeq2.toArraySync tskSeq3 // from here
    result3 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result - alternative #4`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let sequence = seq {
        yield 1
        yield 2
    }

    // [ 2; 3]
    let tskSeq2 = asyncSeq2 {
        for i in sequence do
            yield i + 1
    }

    let result2 = AsyncSeq2.toArraySync tskSeq2
    result2 |> should equal [| 2; 3 |]

    // [ 3; 4]
    let tskSeq3 = asyncSeq2 {
        for i in tskSeq2 do
            yield i + 1 // NRE here
    }

    let result3 = AsyncSeq2.toArraySync tskSeq3 // NRE from here
    result3 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``CE asyncSeq2, call map multiple times over its own result - alternative #5`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let sequence = seq {
        yield 1
        yield 2
    }

    // [ 2; 3]
    let tskSeq2 = asyncSeq2 {
        yield! asyncSeq2 {
            for i in sequence do
                yield i + 1
        }
    }

    let result2 = AsyncSeq2.toArraySync tskSeq2
    result2 |> should equal [| 2; 3 |]

    // [ 3; 4]
    let tskSeq3 = asyncSeq2 {
        yield! asyncSeq2 { // NRE here
            for i in tskSeq2 do
                yield i + 1
        }
    }

    let result3 = AsyncSeq2.toArraySync tskSeq3 // from here
    result3 |> should equal [| 3; 4 |]
}


[<Fact>]
let ``CE asyncSeq2, call mapAsync multiple times over its own result`` () = task {
    // Bug #42: System.NullReferenceException: Object reference not set to an instance of an object.
    // whether using AsyncSeq2.toArraySync or toArrayAsync, or another version that uses GetAsyncEnumerator() under the hood doesn't matter

    let tskSeq = asyncSeq2 {
        yield 1
        yield 2
    }

    // let's map once, and then again on the new sequence
    let ts1 = tskSeq |> AsyncSeq2.mapAsync (fun i -> async2 { return i + 1 })
    let result1 = AsyncSeq2.toArraySync ts1
    let ts2 = ts1 |> AsyncSeq2.mapAsync (fun i -> async2 { return i + 1 })
    let result2 = AsyncSeq2.toArraySync ts2 // NRE here
    result1 |> should equal [| 2; 3 |]
    result2 |> should equal [| 3; 4 |]
}

[<Fact>]
let ``AsyncSeq2-toArray can be applied multiple times to the same sequence`` () =
    let tq = asyncSeq2 { yield! [ 1..10 ] }
    let (results1: _[]) = tq |> AsyncSeq2.toArraySync
    let (results2: _[]) = tq |> AsyncSeq2.toArraySync
    let (results3: _[]) = tq |> AsyncSeq2.toArraySync
    let (results4: _[]) = tq |> AsyncSeq2.toArraySync
    results1 |> should equal [| 1..10 |]
    results2 |> should equal [| 1..10 |]
    results3 |> should equal [| 1..10 |]
    results4 |> should equal [| 1..10 |]
