[CmdletBinding()]
param(
    [ValidateRange(3, 50)]
    [int] $Waves = 10,

    [ValidateRange(10, 10000)]
    [int] $Messages = 100,

    [long] $BufferBytes = 512MB,

    [int] $ProbeMilliseconds = 100,

    [string] $OutputDirectory = 'artifacts/memory-stability',

    [switch] $Pressure,

    [switch] $InProcess
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$project = Join-Path `
    $root `
    'benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj'
$outputRoot = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    $OutputDirectory
}
else {
    Join-Path $root $OutputDirectory
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

& dotnet build $project -c Release --no-restore
if ($LASTEXITCODE -ne 0) {
    throw 'Memory stability benchmark build failed.'
}

$assembly = Join-Path `
    $root `
    'benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll'

$scenarios = @(
    [pscustomobject]@{
        Name = 'qos0-100-8k'
        Port = 58840
        Qos = 0
        Subscribers = 100
        Payload = 8192
        Quota = $BufferBytes
    },
    [pscustomobject]@{
        Name = 'qos1-100-64k'
        Port = 58841
        Qos = 1
        Subscribers = 100
        Payload = 65536
        Quota = $BufferBytes
    }
)

if ($Pressure) {
    $scenarios += [pscustomobject]@{
        Name = 'qos1-100-64k-pressure'
        Port = 58842
        Qos = 1
        Subscribers = 100
        Payload = 65536
        Quota = 8MB
    }
}

function Get-Median([double[]] $values) {
    if ($values.Count -eq 0) {
        return 0
    }

    $ordered = @($values | Sort-Object)
    $middle = [int][math]::Floor($ordered.Count / 2)
    if ($ordered.Count % 2 -eq 0) {
        return ($ordered[$middle - 1] + $ordered[$middle]) / 2
    }

    return $ordered[$middle]
}

foreach ($scenario in $scenarios) {
    $output = Join-Path $outputRoot "$($scenario.Name).json"
    $arguments = @(
        $assembly,
        'memory',
        '--port', $scenario.Port,
        '--qos', $scenario.Qos,
        '--subscribers', $scenario.Subscribers,
        '--payload', $scenario.Payload,
        '--messages', $Messages,
        '--waves', $Waves,
        '--max-pending', 1024,
        '--buffer-bytes', $scenario.Quota,
        '--memory-probe-ms', $ProbeMilliseconds,
        '--output', $output
    )
    if (-not $InProcess) {
        $arguments += @('--broker-assembly', $assembly)
    }

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Memory stability scenario '$($scenario.Name)' failed."
    }

    $result = Get-Content -Raw -LiteralPath $output | ConvertFrom-Json
    $waveResults = @($result.Waves)
    if ($waveResults.Count -ne $Waves) {
        throw (
            "Memory stability scenario '$($scenario.Name)' " +
            "returned $($waveResults.Count) waves.")
    }

    $quota = if ($scenario.Quota -gt 0) {
        $scenario.Quota
    }
    else {
        512MB
    }

    foreach ($wave in $waveResults) {
        if ($wave.FailedOperations -ne 0) {
            throw (
                "Memory wave $($wave.Wave) in '$($scenario.Name)' " +
                'reported failures.')
        }

        if ($wave.Snapshot.BufferCommittedBytes -gt $quota) {
            throw (
                "Memory wave $($wave.Wave) in '$($scenario.Name)' " +
                'exceeded the committed buffer budget.')
        }

        if ($null -ne $wave.Peak -and
            $wave.Peak.BufferCommittedBytes -gt $quota) {
            throw (
                "Memory wave $($wave.Wave) in '$($scenario.Name)' " +
                'exceeded the peak committed buffer budget.')
        }

        if ($wave.Snapshot.RoutePlanInUseCount -ne 0) {
            throw (
                "Memory wave $($wave.Wave) in '$($scenario.Name)' " +
                'retained route plan buffers.')
        }

    }

    $idleRented = if ($null -ne $result.Run -and
        $null -ne $result.Run.IdleSnapshot) {
        [long]$result.Run.IdleSnapshot.BufferRentedBytes
    }
    else {
        [long]$waveResults[0].Snapshot.BufferRentedBytes
    }

    $lastRented = [long]$waveResults[-1].Snapshot.BufferRentedBytes
    if ($lastRented -gt $idleRented + 64KB) {
        throw (
            "Memory stability scenario '$($scenario.Name)' did not return " +
            "to its idle rented-byte baseline: " +
            "idle=$idleRented last=$lastRented.")
    }

    $baselineStart = [math]::Min(5, $waveResults.Count - 1)
    $baselineEnd = [math]::Min(9, $waveResults.Count - 1)
    $tailStart = [math]::Max(0, $waveResults.Count - 5)
    $baselineMedian = Get-Median @(
        $waveResults[$baselineStart..$baselineEnd] |
        ForEach-Object { [double]$_.Snapshot.ManagedHeapBytes })
    $tailMedian = Get-Median @(
        $waveResults[$tailStart..($waveResults.Count - 1)] |
        ForEach-Object { [double]$_.Snapshot.ManagedHeapBytes })
    $heapLimit = [math]::Floor(($baselineMedian * 1.05)) + 1MB
    if ($tailMedian -gt $heapLimit) {
        throw (
            "Memory stability scenario '$($scenario.Name)' grew the " +
            "managed heap beyond its allowance: " +
            "baseline=$baselineMedian tail=$tailMedian.")
    }

    Write-Output (
        "Scenario $($scenario.Name): waves=$Waves quota=$quota " +
        "heap-baseline=$([math]::Round($baselineMedian)) " +
        "heap-tail=$([math]::Round($tailMedian))")
}

Write-Output 'Memory stability verification passed.'
