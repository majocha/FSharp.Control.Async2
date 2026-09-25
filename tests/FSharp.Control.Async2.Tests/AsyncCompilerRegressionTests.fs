namespace FSharp.Control.Async2.Tests

open Xunit
open Microsoft.FSharp.Control

module AsyncCompilerRegressionTests =

    [<Fact>]
    let ``StartChild result can be consumed repeatedly`` () =
        let result =
            async2 {
                let! child = Async2.StartChild(async2 {
                    do! Async2.Sleep 1
                    return 27
                })
                return! Async2.Parallel [ child; child; child; child ]
            }
            |> Async2.RunSynchronously

        Assert.Equal([| 27; 27; 27; 27 |], result)

    [<Fact>]
    let ``Joining StartChild propagates a sibling failure`` () =
        let join (first: Async2<'T>) (second: Async2<'U>) =
            async2 {
                let! firstChild = Async2.StartChild first
                let! secondChild = Async2.StartChild second
                let! firstResult = firstChild
                let! secondResult = secondChild
                return firstResult, secondResult
            }

        let result =
            try
                Async2.RunSynchronously(
                    join
                        (async2 {
                            do! Async2.Sleep 30
                            failwith "fail"
                            return 6
                        })
                        (async2 {
                            do! Async2.Sleep 30
                            return 4
                        })
                )
            with _ ->
                0, 0

        Assert.Equal((0, 0), result)

    [<Fact>]
    let ``StartChild can be run after its parent returns`` () =
        let child = Async2.StartChild(async2 { return 5 }) |> Async2.RunSynchronously
        Assert.Equal(5, child |> Async2.RunSynchronously)

    [<Fact>]
    let ``Deep StartChild chains do not overflow the stack`` () =
        let rec loop remaining =
            async2 {
                if remaining = 0 then
                    return 0
                else
                    let! child = Async2.StartChild(async2 { return remaining })
                    let! _ = child
                    return! loop (remaining - 1)
            }

        Assert.Equal(0, Async2.RunSynchronously(loop 10_000))

    [<Fact>]
    let ``Immediate entry points drain nested work in a running trampoline`` () =
        let nested = async2 {
            do! Async2.FromContinuations(fun (success, _, _) -> success())
        }

        let parent = async2 {
            do! Async2.FromContinuations(fun (success, _, _) ->
                let mutable immediate = false
                let mutable continued = false

                Async2.StartImmediate(async2 {
                    do! nested
                    immediate <- true
                })

                Async2.StartWithContinuations(
                    nested,
                    (fun () -> continued <- true),
                    (fun error -> raise error),
                    (fun error -> raise error)
                )

                Assert.True(immediate, "StartImmediate deferred work to the enclosing trampoline")
                Assert.True(continued, "StartWithContinuations deferred work to the enclosing trampoline")
                success())
        }

        Async2.RunSynchronouslyImmediate parent
