```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method | PayloadSize | Mean         | Error        | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|------- |------------ |-------------:|-------------:|-----------:|------:|--------:|----------:|------------:|
| **Copy**   | **32**          |     **2.072 ns** |     **1.176 ns** |  **0.0644 ns** |  **1.00** |    **0.04** |         **-** |          **NA** |
|        |             |              |              |            |       |         |           |             |
| **Copy**   | **8192**        |    **56.919 ns** |    **11.352 ns** |  **0.6222 ns** |  **1.00** |    **0.01** |         **-** |          **NA** |
|        |             |              |              |            |       |         |           |             |
| **Copy**   | **65536**       | **1,171.491 ns** | **1,113.148 ns** | **61.0154 ns** |  **1.00** |    **0.06** |         **-** |          **NA** |
