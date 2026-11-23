```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]   : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  ShortRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=ShortRun  IterationCount=3  LaunchCount=1  
WarmupCount=3  

```
| Method                   | Mean     | Error       | StdDev   | Ratio | RatioSD | Gen0   | Allocated | Alloc Ratio |
|------------------------- |---------:|------------:|---------:|------:|--------:|-------:|----------:|------------:|
| PublishQos0              | 368.0 ns |    68.40 ns |  3.75 ns |  0.60 |    0.06 |      - |         - |        0.00 |
| MqttNetDecodeMatchEncode | 620.7 ns | 1,323.07 ns | 72.52 ns |  1.01 |    0.14 | 1.3342 |    8368 B |        1.00 |
