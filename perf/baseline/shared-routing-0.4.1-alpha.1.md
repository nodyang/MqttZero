```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1
WarmupCount=3

```
| Method            | Subscribers | Mean        | Error        | StdDev      | Allocated |
|------------------ |------------ |------------:|-------------:|------------:|----------:|
| **SelectSharedGroup** | **1**           |    **109.7 ns** |     **54.53 ns** |     **2.99 ns** |         **-** |
| **SelectSharedGroup** | **100**         |  **2,385.9 ns** |    **424.22 ns** |    **23.25 ns** |         **-** |
| **SelectSharedGroup** | **1000**        | **24,480.6 ns** | **37,322.58 ns** | **2,045.78 ns** |         **-** |
