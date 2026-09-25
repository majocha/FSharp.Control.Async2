namespace FSharp.Core.UnitTests

[<assembly: Xunit.CaptureConsole>]
do ()

open System.Threading

module LibraryTestFx =
    let sleep (milliseconds: int) = Thread.Sleep milliseconds

namespace FSharp.Test

open Xunit

[<CollectionDefinition(nameof NotThreadSafeResourceCollection, DisableParallelization = true)>]
type NotThreadSafeResourceCollection() = class end
