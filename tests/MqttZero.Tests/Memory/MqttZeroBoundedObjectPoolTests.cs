using MqttZero.Memory;

namespace MqttZero.Tests.Memory;

public sealed class MqttZeroBoundedObjectPoolTests
{
    [Fact]
    public void PoolDropsObjectsAboveCapacity()
    {
        MqttZeroBoundedObjectPool<object> pool = new(2);
        object first = new();
        object second = new();
        object third = new();

        Assert.True(pool.Return(first));
        Assert.True(pool.Return(second));
        Assert.False(pool.Return(third));
        Assert.Equal(2, pool.Count);

        Assert.True(pool.TryRent(out object? rentedFirst));
        Assert.Same(first, rentedFirst);
        Assert.True(pool.TryRent(out object? rentedSecond));
        Assert.Same(second, rentedSecond);
        Assert.False(pool.TryRent(out _));
    }
}
