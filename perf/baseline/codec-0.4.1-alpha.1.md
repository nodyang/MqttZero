```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method             | PayloadSize | Segments | Mean        | Error        | StdDev    | Allocated |
|------------------- |------------ |--------- |------------:|-------------:|----------:|----------:|
| **EncodeToWriterFast** | **32**          | **1**        |    **48.82 ns** |    **66.054 ns** |  **3.621 ns** |         **-** |
| **EncodeToWriterFast** | **32**          | **10**       |    **47.25 ns** |     **5.528 ns** |  **0.303 ns** |         **-** |
| **EncodeToWriterFast** | **8192**        | **1**        |   **136.66 ns** |   **172.897 ns** |  **9.477 ns** |         **-** |
| **EncodeToWriterFast** | **8192**        | **10**       |   **134.30 ns** |    **69.964 ns** |  **3.835 ns** |         **-** |
| **EncodeToWriterFast** | **65536**       | **1**        | **1,394.14 ns** | **1,394.377 ns** | **76.431 ns** |         **-** |
| **EncodeToWriterFast** | **65536**       | **10**       | **1,380.13 ns** | **1,349.693 ns** | **73.981 ns** |         **-** |
