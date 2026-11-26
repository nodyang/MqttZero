using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkVariantArgumentsTests
{
    [Fact]
    public void VariantOverridesReplaceSharedValues()
    {
        Dictionary<string, string> arguments = new(
            StringComparer.OrdinalIgnoreCase)
        {
            ["event-loop-shards"] = "0",
            ["baseline-event-loop-shards"] = "12",
            ["candidate-event-loop-shards"] = "6",
            ["baseline-commit"] = "1111111",
            ["candidate-commit"] = "2222222"
        };

        Dictionary<string, string> baseline =
            BenchmarkVariantArguments.Build(arguments, "baseline");
        Dictionary<string, string> candidate =
            BenchmarkVariantArguments.Build(arguments, "candidate");

        Assert.Equal("12", baseline["event-loop-shards"]);
        Assert.Equal("6", candidate["event-loop-shards"]);
        Assert.DoesNotContain("baseline-commit", baseline.Keys);
        Assert.DoesNotContain("baseline-event-loop-shards", candidate.Keys);
    }

    [Fact]
    public void SharedValuesApplyWhenVariantDoesNotOverrideThem()
    {
        Dictionary<string, string> arguments = new(
            StringComparer.OrdinalIgnoreCase)
        {
            ["outbound-batch-messages"] = "128",
            ["candidate-outbound-batch-messages"] = "64"
        };

        Dictionary<string, string> baseline =
            BenchmarkVariantArguments.Build(arguments, "baseline");
        Dictionary<string, string> candidate =
            BenchmarkVariantArguments.Build(arguments, "candidate");

        Assert.Equal("128", baseline["outbound-batch-messages"]);
        Assert.Equal("64", candidate["outbound-batch-messages"]);
    }
}
