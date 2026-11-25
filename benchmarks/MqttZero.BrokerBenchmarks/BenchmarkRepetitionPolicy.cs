namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkRepetitionPolicy
{
    private const double MaximumThroughputSpread = 0.10;

    public static bool RequiresThirdRepetition(
        IReadOnlyList<BrokerComparisonResult> results)
    {
        ArgumentNullException.ThrowIfNull(results);
        BrokerComparisonResult[] mqttZero = results
            .Where(static result => result.Broker == "mqttzero")
            .ToArray();
        BrokerComparisonResult[] mqttNet = results
            .Where(static result => result.Broker == "mqttnet")
            .ToArray();
        if (mqttZero.Length != 2 ||
            mqttNet.Length != 2 ||
            results.Count != 4)
        {
            return true;
        }

        return HasInvalidResult(mqttZero) ||
            HasInvalidResult(mqttNet) ||
            HasExcessiveSpread(mqttZero) ||
            HasExcessiveSpread(mqttNet);
    }

    private static bool HasInvalidResult(
        BrokerComparisonResult[] results)
    {
        foreach (BrokerComparisonResult result in results)
        {
            long expectedReceived =
                result.Result.PublishedMessages *
                result.Subscribers;
            if (result.Result.FailedOperations != 0 ||
                result.Result.MeasurementReceivedMessages !=
                    expectedReceived ||
                result.Result.EndToEndThroughputPerSecond <= 0)
            {
                return true;
            }
        }

        return false;
    }

    private static bool HasExcessiveSpread(
        BrokerComparisonResult[] results)
    {
        double first = results[0].Result.EndToEndThroughputPerSecond;
        double second = results[1].Result.EndToEndThroughputPerSecond;
        double maximum = Math.Max(first, second);
        return maximum <= 0 ||
            Math.Abs(first - second) / maximum >
                MaximumThroughputSpread;
    }
}
