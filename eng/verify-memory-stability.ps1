param(
    [ValidateRange(3, 50)]
    [int]$Waves = 10,

    [ValidateRange(1, 60)]
    [int]$Seconds = 2
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$project = Join-Path $root 'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'
$outputDirectory = Join-Path $root 'artifacts/memory-stability'
New-Item -ItemType Directory -Force -Path $outputDirectory | Out-Null

dotnet build $project -c Release --no-restore
if ($LASTEXITCODE -ne 0) {
    throw 'Memory stability benchmark build failed.'
}

$assembly = Join-Path $root 'benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll'
$scenarios = @(
    [pscustomobject]@{
        Name = 'qos0-100-8k'
        Port = 58840
        Qos = 0
        Subscribers = 100
        Payload = 8192
    },
    [pscustomobject]@{
        Name = 'qos1-100-64k'
        Port = 58841
        Qos = 1
        Subscribers = 100
        Payload = 65536
    }
)

foreach ($scenario in $scenarios) {
    $output = Join-Path $outputDirectory "$($scenario.Name).json"
    dotnet $assembly memory `
        --port $scenario.Port `
        --qos $scenario.Qos `
        --subscribers $scenario.Subscribers `
        --payload $scenario.Payload `
        --warmup 2 `
        --seconds $Seconds `
        --waves $Waves `
        --max-pending 1024 `
        --output $output
    if ($LASTEXITCODE -ne 0) {
        throw "Memory stability scenario '$($scenario.Name)' failed."
    }

    $result = Get-Content -Raw $output | ConvertFrom-Json
    $wavesResult = @($result.Waves)
    if ($wavesResult.Count -ne $Waves) {
        throw "Memory stability scenario '$($scenario.Name)' returned $($wavesResult.Count) waves."
    }

    foreach ($wave in $wavesResult) {
        if ($wave.FailedOperations -ne 0) {
            throw "Memory wave $($wave.Wave) in '$($scenario.Name)' reported failures."
        }

        if ($wave.Snapshot.BufferCommittedBytes -gt 512MB) {
            throw "Memory wave $($wave.Wave) in '$($scenario.Name)' exceeded MaxBufferedBytes."
        }
    }

    $baseline = $wavesResult[2].Snapshot
    $last = $wavesResult[-1].Snapshot
    $heapLimit = [math]::Floor(($baseline.ManagedHeapBytes * 1.05)) + 1MB
    if ($last.ManagedHeapBytes -gt $heapLimit) {
        throw "Memory stability scenario '$($scenario.Name)' exceeded its managed heap allowance."
    }

    $routeLimit = [math]::Floor(($baseline.RoutePlanRetainedBytes * 1.05)) + 1MB
    if ($last.RoutePlanRetainedBytes -gt $routeLimit) {
        throw "Memory stability scenario '$($scenario.Name)' exceeded its route retention allowance."
    }
}
