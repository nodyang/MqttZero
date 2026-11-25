using MqttZero.Memory;
using MqttZero.Server.Egress;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressControlFramePoolTests
{
    [Fact]
    public void FramePoolCapsRetainedFrames()
    {
        using MqttZeroBufferPool buffers = new(1024 * 1024);
        using MqttZeroEgressControlFramePool pool = new(buffers);
        MqttZeroEgressControlFrame[] frames =
            Enumerable.Range(0, 65)
                .Select(_ => pool.Rent(1))
                .ToArray();

        for (int index = 0; index < frames.Length; index++)
        {
            pool.Return(frames[index]);
        }

        Assert.Equal(64, pool.RetainedCount);
    }
}
