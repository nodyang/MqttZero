using System.Text;

namespace MqttZero;

internal sealed class MqttZeroSubscription
{
    private static readonly byte[] SharedPrefix = "$share/"u8.ToArray();

    public MqttZeroSubscription(
        byte[] topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool noLocal,
        bool retainAsPublished,
        uint? subscriptionIdentifier)
    {
        TopicFilter = topicFilter;
        QualityOfServiceLevel = qualityOfServiceLevel;
        NoLocal = noLocal;
        RetainAsPublished = retainAsPublished;
        SubscriptionIdentifier = subscriptionIdentifier;

        if (topicFilter.AsSpan().StartsWith(SharedPrefix))
        {
            int groupStart = SharedPrefix.Length;
            int groupEnd = topicFilter.AsSpan(groupStart).IndexOf((byte)'/');
            if (groupEnd > 0)
            {
                IsShared = true;
                SharedGroup = Encoding.UTF8.GetString(topicFilter, groupStart, groupEnd);
                MatchFilter = topicFilter[(groupStart + groupEnd + 1)..];
                return;
            }
        }

        MatchFilter = topicFilter;
    }

    public byte[] TopicFilter { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public bool NoLocal { get; }

    public bool RetainAsPublished { get; }

    public uint? SubscriptionIdentifier { get; }

    public bool IsShared { get; }

    public string? SharedGroup { get; }

    public byte[] MatchFilter { get; }
}
