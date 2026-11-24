using MqttZero.Server;
namespace MqttZero.Tests.Architecture
{
    public sealed class AssemblyBoundaryTests
    {
        [Fact]
        public void CommonTypesArePublishedFromSharedAssembly()
        {
            AssertAssembly("MqttZero.Publishing.MqttZeroPublishMessage, MqttZero", "MqttZero");
            AssertAssembly("MqttZero.MqttZeroPayloadOwner, MqttZero", "MqttZero");
        }

        [Fact]
        public void ServerTypesArePublishedFromServerAssembly()
        {
            AssertAssembly("MqttZero.Server.MqttZeroOptions, MqttZero.Server", "MqttZero.Server");
            AssertAssembly(
                "MqttZero.Server.Broker.MqttZeroBroker, MqttZero.Server",
                "MqttZero.Server");
        }

        private static void AssertAssembly(string assemblyQualifiedTypeName, string expectedAssembly)
        {
            Type? type = Type.GetType(assemblyQualifiedTypeName, false);
            Assert.NotNull(type);
            Assert.Equal(expectedAssembly, type.Assembly.GetName().Name);
        }
    }
}
