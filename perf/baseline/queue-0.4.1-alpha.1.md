```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1
WarmupCount=3

```
| Method         | Depth | Mean         | Error         | StdDev      | Allocated |
|--------------- |------ |-------------:|--------------:|------------:|----------:|
| **EnqueueDequeue** | **1**     |     **4.869 ns** |     **0.5727 ns** |   **0.0314 ns** |         **-** |
| **EnqueueDequeue** | **1000**  | **6,135.820 ns** | **5,110.0518 ns** | **280.0992 ns** |         **-** |
