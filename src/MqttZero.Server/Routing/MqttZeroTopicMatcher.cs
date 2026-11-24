using MqttZero.Protocol;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Routing;

/// <summary>
/// Matches MQTT topic filters against topic names without creating strings.
/// </summary>
public static class MqttZeroTopicMatcher
{
    /// <summary>
    /// Determines whether an MQTT topic filter matches a topic name.
    /// </summary>
    /// <param name="filter">The MQTT topic filter.</param>
    /// <param name="topic">The MQTT topic name.</param>
    /// <returns><see langword="true"/> when the filter matches the topic.</returns>
    public static bool Matches(ReadOnlySpan<byte> filter, ReadOnlySpan<byte> topic)
    {
        int wildcardIndex = filter.IndexOfAny((byte)'+', (byte)'#');
        if (wildcardIndex < 0)
        {
            return filter.SequenceEqual(topic);
        }

        if (filter[wildcardIndex] == (byte)'#' &&
            wildcardIndex == filter.Length - 1)
        {
            if (wildcardIndex == 0)
            {
                return true;
            }

            if (filter[wildcardIndex - 1] == (byte)'/')
            {
                ReadOnlySpan<byte> prefix = filter[..(wildcardIndex - 1)];
                return topic.SequenceEqual(prefix) ||
                    (topic.Length > prefix.Length &&
                        topic.StartsWith(prefix) &&
                        topic[prefix.Length] == (byte)'/');
            }
        }

        int filterOffset = 0;
        int topicOffset = 0;

        while (filterOffset < filter.Length)
        {
            int filterEnd = IndexOf(filter, filterOffset, (byte)'/');
            int topicEnd = IndexOf(topic, topicOffset, (byte)'/');
            ReadOnlySpan<byte> filterLevel = SliceLevel(filter, filterOffset, filterEnd);
            ReadOnlySpan<byte> topicLevel = SliceLevel(topic, topicOffset, topicEnd);

            if (filterLevel.Length == 1 && filterLevel[0] == (byte)'#')
            {
                return true;
            }

            if (!(filterLevel.Length == 1 && filterLevel[0] == (byte)'+') &&
                !filterLevel.SequenceEqual(topicLevel))
            {
                return false;
            }

            bool filterComplete = filterEnd == filter.Length;
            bool topicComplete = topicEnd == topic.Length;
            if (topicComplete && !filterComplete && IsRemainingMultilevelWildcard(filter, filterEnd + 1))
            {
                return true;
            }

            if (filterComplete || topicComplete)
            {
                return filterComplete && topicComplete;
            }

            filterOffset = filterEnd + 1;
            topicOffset = topicEnd + 1;
        }

        return topicOffset == topic.Length;
    }

    private static int IndexOf(ReadOnlySpan<byte> value, int start, byte delimiter)
    {
        int relativeOffset = value[start..].IndexOf(delimiter);
        if (relativeOffset < 0)
        {
            return value.Length;
        }

        return start + relativeOffset;
    }

    private static ReadOnlySpan<byte> SliceLevel(ReadOnlySpan<byte> value, int start, int end)
    {
        if (start >= value.Length || end < start)
        {
            return ReadOnlySpan<byte>.Empty;
        }

        return value[start..end];
    }

    private static bool IsRemainingMultilevelWildcard(ReadOnlySpan<byte> filter, int start)
    {
        return start < filter.Length &&
            filter[start..].SequenceEqual("#"u8);
    }
}
