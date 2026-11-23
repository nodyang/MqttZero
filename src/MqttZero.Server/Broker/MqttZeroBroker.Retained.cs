using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Broker;

/// <summary>
/// Implements retained message storage and removal.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private void StoreRetainedMessage(MqttZeroMessageLease lease)
    {
        string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
        if (lease.PayloadSequence.IsEmpty)
        {
            RemoveRetainedMessage(topicKey);
            return;
        }

        lease.EnsureOwned();
        lock (_sessionGate)
        {
            _retainedMessages.TryGetValue(topicKey, out MqttZeroRetainedMessage? existing);
            if (existing == null && _retainedMessages.Count >= _options.MaxRetainedMessages)
            {
                throw new InvalidOperationException("The retained message count limit was reached.");
            }

            long previousBytes = existing?.ByteLength ?? 0;
            long replacementBytes =
                lease.TopicSequence.Length + lease.PayloadSequence.Length;
            long retainedBytes = _retainedBytes - previousBytes + replacementBytes;
            if (retainedBytes > _options.MaxRetainedBytes)
            {
                throw new InvalidOperationException("The retained message byte limit was reached.");
            }

            MqttZeroRetainedMessage replacement = new(
                lease,
                lease.QualityOfServiceLevel,
                lease.MessageExpiryInterval.HasValue
                    ? _timeProvider.GetUtcNow().AddSeconds(
                        lease.MessageExpiryInterval.Value)
                    : null);
            _retainedMessages[topicKey] = replacement;
            _retainedBytes = retainedBytes;
            existing?.Release();
        }
    }

    private void RemoveRetainedMessage(string topicKey)
    {
        MqttZeroRetainedMessage? removed = null;
        lock (_sessionGate)
        {
            if (_retainedMessages.TryRemove(topicKey, out removed))
            {
                _retainedBytes -= removed.ByteLength;
            }
        }

        removed?.Release();
    }

}
