namespace MqttZero.Server.Sessions;

internal enum MqttZeroOutboundState
{
    AwaitPubAck,
    AwaitPubRec,
    AwaitPubComp
}
