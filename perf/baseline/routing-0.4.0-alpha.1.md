```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method        | Subscribers | Mean          | Error        | StdDev      | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------- |------------ |--------------:|-------------:|------------:|------:|--------:|----------:|------------:|
| **RouteMqttZero** | **1**           |     **32.977 ns** |    **10.101 ns** |   **0.5537 ns** |  **1.00** |    **0.02** |         **-** |          **NA** |
| MatchMqttNet  | 1           |      6.912 ns |     3.311 ns |   0.1815 ns |  0.21 |    0.01 |         - |          NA |
|               |             |               |              |             |       |         |           |             |
| **RouteMqttZero** | **100**         |  **1,767.864 ns** |   **534.127 ns** |  **29.2773 ns** |  **1.00** |    **0.02** |         **-** |          **NA** |
| MatchMqttNet  | 100         |    735.786 ns |   876.586 ns |  48.0486 ns |  0.42 |    0.02 |         - |          NA |
|               |             |               |              |             |       |         |           |             |
| **RouteMqttZero** | **1000**        | **18,331.680 ns** | **4,620.595 ns** | **253.2704 ns** |  **1.00** |    **0.02** |         **-** |          **NA** |
| MatchMqttNet  | 1000        |  6,544.578 ns | 2,421.512 ns | 132.7312 ns |  0.36 |    0.01 |         - |          NA |
