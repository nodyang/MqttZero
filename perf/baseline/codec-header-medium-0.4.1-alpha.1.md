```

BenchmarkDotNet v0.15.8, Windows 11 (10.0.26300.9974)
12th Gen Intel Core i5-12450H 2.00GHz, 1 CPU, 12 logical and 8 physical cores
.NET SDK 10.0.400
  [Host]    : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3
  MediumRun : .NET 10.0.15 (10.0.15, 10.0.1526.53008), X64 RyuJIT x86-64-v3

Job=MediumRun  IterationCount=15  LaunchCount=2  
WarmupCount=10  

```
| Method           | PayloadSize | Segments | Mean     | Error    | StdDev   | Median   | Allocated |
|----------------- |------------ |--------- |---------:|---------:|---------:|---------:|----------:|
| **EncodeHeaderFast** | **32**          | **1**        | **33.99 ns** | **0.922 ns** | **1.323 ns** | **33.67 ns** |         **-** |
| **EncodeHeaderFast** | **32**          | **10**       | **34.04 ns** | **1.761 ns** | **2.636 ns** | **33.49 ns** |         **-** |
| **EncodeHeaderFast** | **8192**        | **1**        | **32.13 ns** | **0.178 ns** | **0.244 ns** | **32.05 ns** |         **-** |
| **EncodeHeaderFast** | **8192**        | **10**       | **32.24 ns** | **0.241 ns** | **0.330 ns** | **32.23 ns** |         **-** |
| **EncodeHeaderFast** | **65536**       | **1**        | **35.62 ns** | **2.230 ns** | **3.126 ns** | **33.45 ns** |         **-** |
| **EncodeHeaderFast** | **65536**       | **10**       | **33.24 ns** | **0.278 ns** | **0.371 ns** | **33.35 ns** |         **-** |
