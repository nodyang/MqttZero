namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Resolves shared options with per-variant overrides.
/// </summary>
internal static class BenchmarkVariantArguments
{
    public static Dictionary<string, string> Build(
        IReadOnlyDictionary<string, string> arguments,
        string variant)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        string prefix = variant + "-";
        Dictionary<string, string> scoped = new(
            StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, string> pair in arguments)
        {
            if (pair.Key.StartsWith("baseline-", StringComparison.OrdinalIgnoreCase) ||
                pair.Key.StartsWith("candidate-", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            scoped[pair.Key] = pair.Value;
        }

        foreach (KeyValuePair<string, string> pair in arguments)
        {
            if (pair.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                scoped[pair.Key[prefix.Length..]] = pair.Value;
            }
        }

        return scoped;
    }
}
