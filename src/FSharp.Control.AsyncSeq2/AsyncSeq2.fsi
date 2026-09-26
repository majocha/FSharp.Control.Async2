namespace Microsoft.FSharp.Control

/// Operations on asynchronous sequences. Terminal operations return Async2 and
/// use the cancellation token supplied when the computation is started.
[<Sealed; AbstractClass>]
type AsyncSeq2 =
    /// An empty asynchronous sequence.
    static member empty<'T> : unit -> AsyncSeq2<'T>

    /// A sequence containing one element.
    static member singleton: value: 'T -> AsyncSeq2<'T>

    /// Lazily enumerate a synchronous sequence.
    static member ofSeq: source: seq<'T> -> AsyncSeq2<'T>

    /// Concatenate two asynchronous sequences.
    static member append: first: AsyncSeq2<'T> -> second: AsyncSeq2<'T> -> AsyncSeq2<'T>

    /// Apply a projection to every element.
    static member map: projection: ('T -> 'U) -> source: AsyncSeq2<'T> -> AsyncSeq2<'U>

    /// Apply an Async2 projection to every element.
    static member mapAsync: projection: ('T -> Async2<'U>) -> source: AsyncSeq2<'T> -> AsyncSeq2<'U>

    /// Retain elements satisfying a predicate.
    static member filter: predicate: ('T -> bool) -> source: AsyncSeq2<'T> -> AsyncSeq2<'T>

    /// Concatenate sequences produced by a projection.
    static member collect: projection: ('T -> AsyncSeq2<'U>) -> source: AsyncSeq2<'T> -> AsyncSeq2<'U>

    /// Consume the sequence into an array.
    static member toArray: source: AsyncSeq2<'T> -> Async2<'T array>

    /// Consume the sequence into a list.
    static member toList: source: AsyncSeq2<'T> -> Async2<'T list>

    /// Perform an action on each element.
    static member iter: action: ('T -> unit) -> source: AsyncSeq2<'T> -> Async2<unit>

    /// Perform an Async2 action on each element.
    static member iterAsync: action: ('T -> Async2<unit>) -> source: AsyncSeq2<'T> -> Async2<unit>

    /// Check whether the sequence contains no elements.
    static member isEmpty: source: AsyncSeq2<'T> -> Async2<bool>

    /// Count the elements in the sequence.
    static member length: source: AsyncSeq2<'T> -> Async2<int>
