using System.Buffers;
using System.Text;

namespace MqttZero.Protocol;

internal static class MqttZeroProtocolValidator
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static bool IsValidTopicName(ReadOnlySequence<byte> topic)
    {
        if (topic.IsEmpty || !IsValidUtf8(topic))
        {
            return false;
        }

        foreach (ReadOnlyMemory<byte> segment in topic)
        {
            if (segment.Span.IndexOfAny((byte)'+', (byte)'#') >= 0)
            {
                return false;
            }
        }

        return true;
    }

    public static bool IsValidTopicFilter(ReadOnlySequence<byte> topicFilter)
    {
        if (topicFilter.IsEmpty || !IsValidUtf8(topicFilter))
        {
            return false;
        }

        int length = checked((int)topicFilter.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
        try
        {
            topicFilter.CopyTo(buffer);
            return IsValidTopicFilterBuffer(buffer.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static bool IsValidPubAckReasonCode(MqttReasonCode reasonCode)
    {
        return reasonCode is
            MqttReasonCode.Success or
            MqttReasonCode.NoMatchingSubscribers or
            MqttReasonCode.UnspecifiedError or
            MqttReasonCode.ImplementationSpecificError or
            MqttReasonCode.NotAuthorized or
            MqttReasonCode.TopicNameInvalid or
            MqttReasonCode.PacketIdentifierInUse or
            MqttReasonCode.QuotaExceeded or
            MqttReasonCode.PayloadFormatInvalid;
    }

    public static bool IsValidPubRelReasonCode(MqttReasonCode reasonCode)
    {
        return reasonCode is
            MqttReasonCode.Success or
            MqttReasonCode.PacketIdentifierNotFound;
    }

    public static bool IsValidConnAckReasonCode(MqttReasonCode reasonCode)
    {
        return reasonCode is
            MqttReasonCode.Success or
            MqttReasonCode.UnspecifiedError or
            MqttReasonCode.MalformedPacket or
            MqttReasonCode.ProtocolError or
            MqttReasonCode.ImplementationSpecificError or
            MqttReasonCode.UnsupportedProtocolVersion or
            MqttReasonCode.ClientIdentifierNotValid or
            MqttReasonCode.BadUserNameOrPassword or
            MqttReasonCode.NotAuthorized or
            MqttReasonCode.ServerUnavailable or
            MqttReasonCode.ServerBusy or
            MqttReasonCode.Banned or
            MqttReasonCode.BadAuthenticationMethod or
            MqttReasonCode.TopicNameInvalid or
            MqttReasonCode.PacketTooLarge or
            MqttReasonCode.QuotaExceeded or
            MqttReasonCode.PayloadFormatInvalid or
            MqttReasonCode.UseAnotherServer or
            MqttReasonCode.ServerMoved or
            MqttReasonCode.ConnectionRateExceeded;
    }

    public static bool IsValidDisconnectReasonCode(MqttReasonCode reasonCode)
    {
        return reasonCode is
            MqttReasonCode.Success or
            MqttReasonCode.DisconnectWithWill or
            MqttReasonCode.UnspecifiedError or
            MqttReasonCode.MalformedPacket or
            MqttReasonCode.ProtocolError or
            MqttReasonCode.ImplementationSpecificError or
            MqttReasonCode.NotAuthorized or
            MqttReasonCode.ServerBusy or
            MqttReasonCode.ServerShuttingDown or
            MqttReasonCode.KeepAliveTimeout or
            MqttReasonCode.SessionTakenOver or
            MqttReasonCode.TopicFilterInvalid or
            MqttReasonCode.TopicNameInvalid or
            MqttReasonCode.ReceiveMaximumExceeded or
            MqttReasonCode.TopicAliasInvalid or
            MqttReasonCode.PacketTooLarge or
            MqttReasonCode.MessageRateTooHigh or
            MqttReasonCode.QuotaExceeded or
            MqttReasonCode.AdministrativeAction or
            MqttReasonCode.PayloadFormatInvalid or
            MqttReasonCode.RetainNotSupported or
            MqttReasonCode.QosNotSupported or
            MqttReasonCode.UseAnotherServer or
            MqttReasonCode.ServerMoved or
            MqttReasonCode.SharedSubscriptionsNotSupported or
            MqttReasonCode.ConnectionRateExceeded or
            (MqttReasonCode)0xA0 or
            (MqttReasonCode)0xA1 or
            (MqttReasonCode)0xA2;
    }

    public static bool IsValidSubAckReasonCode(byte reasonCode)
    {
        return reasonCode is
            0x00 or 0x01 or 0x02 or 0x80 or 0x83 or 0x87 or 0x8F or 0x91 or 0x97 or
            0x9E or 0xA1 or 0xA2;
    }

    public static bool IsValidUnsubAckReasonCode(byte reasonCode)
    {
        return reasonCode is 0x00 or 0x11 or 0x80 or 0x83 or 0x87 or 0x8F or 0x91;
    }

    private static bool IsValidTopicFilterBuffer(ReadOnlySpan<byte> topicFilter)
    {
        if (topicFilter.StartsWith("$share/"u8))
        {
            ReadOnlySpan<byte> remainder = topicFilter["$share/".Length..];
            int separator = remainder.IndexOf((byte)'/');
            if (separator <= 0 || separator == remainder.Length - 1)
            {
                return false;
            }

            topicFilter = remainder[(separator + 1)..];
        }

        int levelStart = 0;
        for (int index = 0; index <= topicFilter.Length; index++)
        {
            if (index < topicFilter.Length && topicFilter[index] != (byte)'/')
            {
                continue;
            }

            ReadOnlySpan<byte> level = topicFilter[levelStart..index];
            if (level.Length > 0 &&
                level[0] == (byte)'#')
            {
                if (level.Length != 1 || index != topicFilter.Length)
                {
                    return false;
                }
            }
            else if (level.Length > 0 &&
                level[0] == (byte)'+')
            {
                if (level.Length != 1)
                {
                    return false;
                }
            }
            else if (level.Length > 0 &&
                level.IndexOfAny((byte)'+', (byte)'#') >= 0)
            {
                return false;
            }

            levelStart = index + 1;
        }

        return true;
    }

    public static bool IsValidUtf8(ReadOnlySequence<byte> value)
    {
        if (value.IsEmpty)
        {
            return true;
        }

        int length = checked((int)value.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(Math.Max(length, 1));
        try
        {
            Span<byte> bytes = buffer.AsSpan(0, length);
            value.CopyTo(bytes);
            if (bytes.IndexOf((byte)0) >= 0)
            {
                return false;
            }

            try
            {
                _ = StrictUtf8.GetCharCount(bytes);
                return true;
            }
            catch (DecoderFallbackException)
            {
                return false;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
