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
| **RouteMqttZero** | **1**           |     **30.857 ns** |     **1.728 ns** |   **0.0947 ns** |  **1.00** |    **0.00** |         **-** |          **NA** |
| MatchMqttNet  | 1           |      6.904 ns |     3.834 ns |   0.2102 ns |  0.22 |    0.01 |         - |          NA |
|               |             |               |              |             |       |         |           |             |
| **RouteMqttZero** | **100**         |  **1,153.775 ns** | **1,277.224 ns** |  **70.0090 ns** |  **1.00** |    **0.08** |         **-** |          **NA** |
| MatchMqttNet  | 100         |    739.375 ns |   520.393 ns |  28.5245 ns |  0.64 |    0.04 |         - |          NA |
|               |             |               |              |             |       |         |           |             |
| **RouteMqttZero** | **1000**        | **11,361.228 ns** | **7,165.174 ns** | **392.7474 ns** |  **1.00** |    **0.04** |         **-** |          **NA** |
| MatchMqttNet  | 1000        |  7,186.241 ns | 9,082.147 ns | 497.8231 ns |  0.63 |    0.04 |         - |          NA |
