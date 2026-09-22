# FSharp.Control.Async2

A separate copy of the F# Core asynchronous computation expression and `Async` module, renamed to `Async2` and `async2` so it can be developed independently without conflicting with the built-in implementation.

The implementation and unit coverage are copied from the local FSharp.Core checkout at `D:\source\fsharp`. The library targets `net11.0`; tests use xUnit v3 with the Microsoft Testing Platform runner.

The root `Directory.Build.props` selects the local F# compiler at `$(RuntimeAsyncBin)\fsc\Release\net11.0\fsc.dll` and references `$(RuntimeAsyncBin)\FSharp.Core\Release\net10.0\FSharp.Core.dll`, with `RuntimeAsyncBin` defaulting to `D:\source\fsharp\artifacts\bin`. Override `RuntimeAsyncBin` or provide `RuntimeAsyncBitsPath` when building against another local F# checkout.

The focused `AsyncType`, `AsyncModule`, and `AsyncModuleFunctions` suites are accompanied by direct xUnit ports of the older compiler-library regressions for repeated `StartChild`, child joining, and trampoline depth. RuntimeAsync compiler-component tests are intentionally outside this project.
