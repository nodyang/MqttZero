namespace MqttZero.Sessions;

internal enum MqttZeroOutboundState
{
    AwaitPubAck,
    AwaitPubRec,
    AwaitPubComp
}
