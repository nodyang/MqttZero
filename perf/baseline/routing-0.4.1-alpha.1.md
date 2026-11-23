```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1
WarmupCount=3

```
| Method        | Subscribers | Mean          | Error      | StdDev     | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------- |------------ |--------------:|-----------:|-----------:|------:|--------:|----------:|------------:|
| **RouteMqttZero** | **1**           |     **26.228 ns** |  **16.305 ns** |  **0.8937 ns** |  **1.00** |    **0.04** |         **-** |          **NA** |
| MatchMqttNet  | 1           |      6.430 ns |   2.344 ns |  0.1285 ns |  0.25 |    0.01 |         - |          NA |
|               |             |               |            |            |       |         |           |             |
| **RouteMqttZero** | **100**         |    **953.423 ns** | **534.678 ns** | **29.3075 ns** |  **1.00** |    **0.04** |         **-** |          **NA** |
| MatchMqttNet  | 100         |    628.781 ns |  55.200 ns |  3.0257 ns |  0.66 |    0.02 |         - |          NA |
|               |             |               |            |            |       |         |           |             |
| **RouteMqttZero** | **1000**        | **10,186.021 ns** | **644.830 ns** | **35.3453 ns** |  **1.00** |    **0.00** |         **-** |          **NA** |
| MatchMqttNet  | 1000        |  6,175.892 ns |  56.072 ns |  3.0735 ns |  0.61 |    0.00 |         - |          NA |
