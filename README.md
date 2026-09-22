# FSharp.Control.Async2

A separate asynchronous computation expression and module, renamed to `Async2` and `async2` so it can be developed independently without conflicting with the built-in implementation.

The current implementation uses the preview runtime-async compiler feature. `Async2<'T>` is a named cold carrier whose start-time `CancellationToken` is passed to a runtime-async `Task<'T>` factory. The library targets `net11.0`; tests use xUnit v3 with the Microsoft Testing Platform runner.

The root `Directory.Build.props` selects the local F# compiler at `$(RuntimeAsyncBin)\fsc\Release\net11.0\fsc.dll` and references `$(RuntimeAsyncBin)\FSharp.Core\Release\net10.0\FSharp.Core.dll`, with `RuntimeAsyncBin` defaulting to `D:\source\fsharp\artifacts\bin`. Override `RuntimeAsyncBin` or provide `RuntimeAsyncBitsPath` when building against another local F# checkout.

The focused `AsyncType`, `AsyncModule`, and `AsyncModuleFunctions` suites are accompanied by direct xUnit ports of older compiler-library regressions. RuntimeAsync compiler-component tests are intentionally outside this project.
