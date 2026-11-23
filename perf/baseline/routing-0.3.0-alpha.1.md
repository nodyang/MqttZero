```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method        | Subscribers | Mean          | Error         | StdDev      | Ratio | RatioSD | Allocated | Alloc Ratio |
|-------------- |------------ |--------------:|--------------:|------------:|------:|--------:|----------:|------------:|
| **RouteMqttZero** | **1**           |     **32.339 ns** |     **9.5325 ns** |   **0.5225 ns** |  **1.00** |    **0.02** |         **-** |          **NA** |
| MatchMqttNet  | 1           |      6.293 ns |     0.5706 ns |   0.0313 ns |  0.19 |    0.00 |         - |          NA |
|               |             |               |               |             |       |         |           |             |
| **RouteMqttZero** | **100**         |  **1,685.367 ns** |   **167.4936 ns** |   **9.1809 ns** |  **1.00** |    **0.01** |         **-** |          **NA** |
| MatchMqttNet  | 100         |    636.827 ns |   280.3255 ns |  15.3656 ns |  0.38 |    0.01 |         - |          NA |
|               |             |               |               |             |       |         |           |             |
| **RouteMqttZero** | **1000**        | **17,969.005 ns** | **2,172.2695 ns** | **119.0694 ns** |  **1.00** |    **0.01** |         **-** |          **NA** |
| MatchMqttNet  | 1000        |  6,183.696 ns |   209.9317 ns |  11.5071 ns |  0.34 |    0.00 |         - |          NA |
