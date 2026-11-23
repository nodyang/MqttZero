$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$packageDirectory = Join-Path $repositoryRoot 'artifacts/packages'
New-Item -ItemType Directory -Force -Path $packageDirectory | Out-Null

Push-Location $repositoryRoot
try {
    dotnet pack src/MqttZero.Server/MqttZero.Server.csproj -c Release --no-restore `
        -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.Server pack failed.'
    }

    dotnet pack src/MqttZero.AspNetCore/MqttZero.AspNetCore.csproj -c Release --no-restore `
        -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.AspNetCore pack failed.'
    }

    dotnet pack src/MqttZero.AspNetCore.Dashboard/MqttZero.AspNetCore.Dashboard.csproj `
        -c Release --no-restore -o $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'MqttZero.AspNetCore.Dashboard pack failed.'
    }

    $consumerDirectory = Join-Path ([IO.Path]::GetTempPath()) (
        'mqttzero-package-consumer-' + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Force -Path $consumerDirectory | Out-Null
    $projectPath = Join-Path $consumerDirectory 'PackageConsumer.csproj'
    $programPath = Join-Path $consumerDirectory 'Program.cs'

    @'
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
    <PackageReference Include="MqttZero.AspNetCore.Dashboard" Version="0.4.0-alpha.1" />
  </ItemGroup>
</Project>
'@ | Set-Content -LiteralPath $projectPath -Encoding utf8NoBOM

    @'
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddMqttZero();
builder.Services.AddMqttZeroDashboard(options =>
{
    options.ReadAuthorizationPolicy = "Read";
    options.OperateAuthorizationPolicy = "Operate";
});
WebApplication app = builder.Build();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
'@ | Set-Content -LiteralPath $programPath -Encoding utf8NoBOM

    dotnet restore $projectPath --source $packageDirectory
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh NuGet consumer restore failed.'
    }

    dotnet build $projectPath -c Release --no-restore
    if ($LASTEXITCODE -ne 0) {
        throw 'Fresh NuGet consumer build failed.'
    }
}
finally {
    Pop-Location
}
