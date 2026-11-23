```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method | Subscribers | QualityOfServiceLevel | Mean        | Error        | StdDev      | Allocated |
|------- |------------ |---------------------- |------------:|-------------:|------------:|----------:|
| **Route**  | **1**           | **AtMostOnce**            |    **182.0 ns** |     **45.98 ns** |     **2.52 ns** |         **-** |
| **Route**  | **1**           | **AtLeastOnce**           |    **267.5 ns** |     **25.51 ns** |     **1.40 ns** |         **-** |
| **Route**  | **10**          | **AtMostOnce**            |  **1,479.3 ns** |    **374.44 ns** |    **20.52 ns** |         **-** |
| **Route**  | **10**          | **AtLeastOnce**           |  **2,092.6 ns** |  **2,492.62 ns** |   **136.63 ns** |         **-** |
| **Route**  | **100**         | **AtMostOnce**            | **16,783.1 ns** |  **3,159.32 ns** |   **173.17 ns** |         **-** |
| **Route**  | **100**         | **AtLeastOnce**           | **22,720.7 ns** | **25,744.21 ns** | **1,411.13 ns** |         **-** |
