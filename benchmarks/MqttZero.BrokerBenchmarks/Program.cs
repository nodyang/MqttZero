using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;
using MqttZero.AspNetCore;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text.Json;

Dictionary<string, string> arguments = ParseArguments(args);
string mode = GetValue(arguments, "mode", args.FirstOrDefault() ?? "compare");

if (mode == "serve")
{
    await ServeAsync(arguments);
}
else if (mode == "load")
{
    object result = await RunLoadAsync(
        GetInt(arguments, "port", 1883),
        GetInt(arguments, "subscribers", 10),
        GetInt(arguments, "publishers", 1),
        GetInt(arguments, "qos", 0),
        GetInt(arguments, "payload", 1024),
        GetInt(arguments, "warmup", 2),
        GetInt(arguments, "seconds", 10));
    Console.WriteLine(JsonSerializer.Serialize(result));
}
else if (mode == "compare")
{
    await CompareAsync(arguments);
}
else
{
    throw new ArgumentException($"Unknown mode '{mode}'.");
}

static async Task ServeAsync(Dictionary<string, string> arguments)
{
    string broker = GetValue(arguments, "broker", "mqttzero");
    int port = GetInt(arguments, "port", 1883);
    CancellationTokenSource lifetime = new();
    Console.CancelKeyPress += (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        lifetime.Cancel();
    };

    if (broker == "mqttzero")
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(
                IPAddress.Loopback,
                port,
                listen => listen.UseMqttZero());
        });
        builder.Services.AddMqttZero(options =>
        {
            options.MaxMessageQueueLength = 100_000;
        });
        await using WebApplication app = builder.Build();
        await app.StartAsync(lifetime.Token);
        Console.WriteLine("READY");
        await WaitForCancellationAsync(lifetime.Token);
        await app.StopAsync(CancellationToken.None);
        return;
    }

    if (broker == "mqttnet")
    {
        MqttServerFactory factory = new();
        MqttServerOptions options = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointBoundIPAddress(IPAddress.Loopback)
            .WithDefaultEndpointPort(port)
            .WithMaxPendingMessagesPerClient(100_000)
            .Build();
        using MqttServer server = factory.CreateMqttServer(options);
        await server.StartAsync();
        Console.WriteLine("READY");
        await WaitForCancellationAsync(lifetime.Token);
        await server.StopAsync();
        return;
    }

    throw new ArgumentException($"Unknown broker '{broker}'.");
}

static async Task CompareAsync(Dictionary<string, string> arguments)
{
    string[] brokers = ["mqttzero", "mqttnet"];
    int[] qosLevels = GetIntArray(arguments, "qos", [0, 1]);
    int[] subscriberCounts = GetIntArray(arguments, "subscribers", [1, 100, 1000]);
    int[] payloadSizes = GetIntArray(arguments, "payload", [32, 8 * 1024, 64 * 1024]);
    int warmup = GetInt(arguments, "warmup", 15);
    int seconds = GetInt(arguments, "seconds", 60);
    int publishers = GetInt(arguments, "publishers", 1);
    List<object> results = [];
    string assemblyPath = Assembly.GetEntryAssembly()!.Location;
    int port = GetInt(arguments, "start-port", 19000);

    foreach (string broker in brokers)
    {
        foreach (int qos in qosLevels)
        {
            foreach (int subscribers in subscriberCounts)
            {
                foreach (int payloadSize in payloadSizes)
                {
                    port++;
                    using Process serverProcess = StartServerProcess(
                        assemblyPath,
                        broker,
                        port);
                    await WaitForReadyAsync(serverProcess);
                    try
                    {
                        object result = await RunLoadAsync(
                            port,
                            subscribers,
                            publishers,
                            qos,
                            payloadSize,
                            warmup,
                            seconds);
                        results.Add(new
                        {
                            Broker = broker,
                            Qos = qos,
                            Subscribers = subscribers,
                            PayloadSize = payloadSize,
                            Result = result
                        });
                    }
                    finally
                    {
                        if (!serverProcess.HasExited)
                        {
                            serverProcess.Kill(entireProcessTree: true);
                        }
                    }
                }
            }
        }
    }

    Console.WriteLine(JsonSerializer.Serialize(results, new JsonSerializerOptions
    {
        WriteIndented = true
    }));
}

static Process StartServerProcess(string assemblyPath, string broker, int port)
{
    ProcessStartInfo startInfo = new()
    {
        FileName = "dotnet",
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true
    };
    startInfo.ArgumentList.Add(assemblyPath);
    startInfo.ArgumentList.Add("serve");
    startInfo.ArgumentList.Add("--broker");
    startInfo.ArgumentList.Add(broker);
    startInfo.ArgumentList.Add("--port");
    startInfo.ArgumentList.Add(port.ToString(CultureInfo.InvariantCulture));
    return Process.Start(startInfo) ??
        throw new InvalidOperationException("Could not start the broker process.");
}

static async Task WaitForReadyAsync(Process process)
{
    using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(20));
    string? line = await process.StandardOutput.ReadLineAsync(timeout.Token);
    if (line == "READY")
    {
        return;
    }

    string error = await process.StandardError.ReadToEndAsync(timeout.Token);
    throw new InvalidOperationException(
        $"Broker process did not become ready. Output: {line}. Error: {error}");
}

static async Task<object> RunLoadAsync(
    int port,
    int subscribers,
    int publishers,
    int qos,
    int payloadSize,
    int warmupSeconds,
    int measurementSeconds)
{
    MqttClientFactory factory = new();
    MqttQualityOfServiceLevel qualityOfServiceLevel = qos == 0
        ? MqttQualityOfServiceLevel.AtMostOnce
        : MqttQualityOfServiceLevel.AtLeastOnce;
    List<IMqttClient> subscriberClients = [];
    List<IMqttClient> publisherClients = [];
    long receivedMessages = 0;
    long failedOperations = 0;
    long publishedMessages = 0;
    ConcurrentQueue<string> errors = new();
    try
    {
        for (int index = 0; index < subscribers; index++)
        {
            IMqttClient subscriber = factory.CreateMqttClient();
            subscriber.ApplicationMessageReceivedAsync += _ =>
            {
                Interlocked.Increment(ref receivedMessages);
                return Task.CompletedTask;
            };
            subscriberClients.Add(subscriber);
            await subscriber.ConnectAsync(
                CreateClientOptions(port, $"subscriber-{index}"),
                CancellationToken.None);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("bench/topic", qualityOfServiceLevel)
                    .Build(),
                CancellationToken.None);
        }

        for (int index = 0; index < publishers; index++)
        {
            IMqttClient publisher = factory.CreateMqttClient();
            publisherClients.Add(publisher);
            await publisher.ConnectAsync(
                CreateClientOptions(port, $"publisher-{index}"),
                CancellationToken.None);
        }

        byte[] payload = new byte[payloadSize];
        MqttApplicationMessage message = new MqttApplicationMessageBuilder()
            .WithTopic("bench/topic")
            .WithPayload(payload)
            .WithQualityOfServiceLevel(qualityOfServiceLevel)
            .Build();
        using CancellationTokenSource warmup = new(TimeSpan.FromSeconds(warmupSeconds));
        await RunPublishersAsync(
            publisherClients,
            message,
            warmup.Token,
            qualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce
                ? () =>
                    Interlocked.Read(ref publishedMessages) * subscribers >
                    Interlocked.Read(ref receivedMessages) + 1024
                : null,
            null,
            exception =>
            {
                Interlocked.Increment(ref failedOperations);
                errors.Enqueue(exception.ToString());
            },
            () => Interlocked.Increment(ref publishedMessages));
        Interlocked.Exchange(ref receivedMessages, 0);
        Interlocked.Exchange(ref publishedMessages, 0);

        long[] latencySamples = new long[100_000];
        int latencySampleCount = 0;
        long measuredPublishes = 0;
        Stopwatch stopwatch = Stopwatch.StartNew();
        using CancellationTokenSource measurement = new(TimeSpan.FromSeconds(measurementSeconds));
        await RunPublishersAsync(
            publisherClients,
            message,
            measurement.Token,
            qualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce
                ? () =>
                    Interlocked.Read(ref publishedMessages) * subscribers >
                    Interlocked.Read(ref receivedMessages) + 1024
                : null,
            elapsedTicks =>
            {
                int index = Interlocked.Increment(ref latencySampleCount) - 1;
                if (index < latencySamples.Length)
                {
                    latencySamples[index] = elapsedTicks;
                }
            },
            exception =>
            {
                Interlocked.Increment(ref failedOperations);
                errors.Enqueue(exception.ToString());
            },
            () =>
            {
                Interlocked.Increment(ref measuredPublishes);
                Interlocked.Increment(ref publishedMessages);
            });
        stopwatch.Stop();

        Array.Sort(latencySamples, 0, Math.Min(latencySampleCount, latencySamples.Length));
        int sampleCount = Math.Min(latencySampleCount, latencySamples.Length);
        return new
        {
            PublishedMessages = measuredPublishes,
            ReceivedMessages = Interlocked.Read(ref receivedMessages),
            FailedOperations = Interlocked.Read(ref failedOperations),
            ThroughputPerSecond = measuredPublishes / stopwatch.Elapsed.TotalSeconds,
            LatencyP50Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.50),
            LatencyP95Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.95),
            LatencyP99Microseconds = PercentileMicroseconds(latencySamples, sampleCount, 0.99),
            FirstError = errors.TryPeek(out string? firstError) ? firstError : null
        };
    }
    finally
    {
        foreach (IMqttClient client in publisherClients.Concat(subscriberClients))
        {
            if (client.IsConnected)
            {
                await client.DisconnectAsync();
            }

            client.Dispose();
        }
    }
}

static MqttClientOptions CreateClientOptions(int port, string clientId)
{
    return new MqttClientOptionsBuilder()
        .WithTcpServer("127.0.0.1", port)
        .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
        .WithClientId(clientId)
        .WithCleanStart()
        .Build();
}

static async Task RunPublishersAsync(
    IReadOnlyList<IMqttClient> publishers,
    MqttApplicationMessage message,
    CancellationToken cancellationToken,
    Func<bool>? shouldPause,
    Action<long>? recordLatency,
    Action<Exception> recordFailure,
    Action? recordPublish = null)
{
    Task[] tasks = new Task[publishers.Count];
    for (int index = 0; index < publishers.Count; index++)
    {
        IMqttClient publisher = publishers[index];
        tasks[index] = Task.Run(
            async () =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    while (shouldPause?.Invoke() == true)
                    {
                        await Task.Delay(
                            TimeSpan.FromMilliseconds(1),
                            cancellationToken);
                    }

                    long started = Stopwatch.GetTimestamp();
                    try
                    {
                        await publisher.PublishAsync(message, CancellationToken.None);
                        recordLatency?.Invoke(Stopwatch.GetTimestamp() - started);
                        recordPublish?.Invoke();
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        recordFailure(exception);
                    }
                }
            },
            cancellationToken);
    }

    try
    {
        await Task.WhenAll(tasks);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
    }
}

static double PercentileMicroseconds(long[] samples, int count, double percentile)
{
    if (count == 0)
    {
        return 0;
    }

    int index = Math.Clamp((int)Math.Ceiling(percentile * count) - 1, 0, count - 1);
    return samples[index] * 1_000_000d / Stopwatch.Frequency;
}

static async Task WaitForCancellationAsync(CancellationToken cancellationToken)
{
    try
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
    catch (OperationCanceledException)
    {
    }
}

static Dictionary<string, string> ParseArguments(string[] args)
{
    Dictionary<string, string> result = new(StringComparer.OrdinalIgnoreCase);
    if (args.Length > 0 && !args[0].StartsWith("--", StringComparison.Ordinal))
    {
        result["mode"] = args[0];
    }

    for (int index = 0; index < args.Length - 1; index++)
    {
        if (args[index].StartsWith("--", StringComparison.Ordinal))
        {
            result[args[index][2..]] = args[index + 1];
            index++;
        }
    }

    return result;
}

static string GetValue(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    string defaultValue)
{
    return arguments.TryGetValue(name, out string? value) ? value : defaultValue;
}

static int GetInt(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    int defaultValue)
{
    return arguments.TryGetValue(name, out string? value)
        ? int.Parse(value, CultureInfo.InvariantCulture)
        : defaultValue;
}

static int[] GetIntArray(
    IReadOnlyDictionary<string, string> arguments,
    string name,
    int[] defaultValue)
{
    if (!arguments.TryGetValue(name, out string? value))
    {
        return defaultValue;
    }

    return value.Split(
        ',',
        StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(item => int.Parse(item, CultureInfo.InvariantCulture))
        .ToArray();
}
