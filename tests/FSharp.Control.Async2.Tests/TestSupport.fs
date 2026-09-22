namespace FSharp.Core.UnitTests

open System.Threading

module LibraryTestFx =
    let sleep (milliseconds: int) = Thread.Sleep milliseconds

namespace FSharp.Test

open Xunit

[<CollectionDefinition(nameof NotThreadSafeResourceCollection, DisableParallelization = true)>]
type NotThreadSafeResourceCollection() = class end
