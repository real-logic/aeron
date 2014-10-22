package uk.co.real_logic.aeron.driver;

/**
 * .
 */
public interface ReceiverProxy
{
    void addSubscription(ReceiveChannelEndpoint mediaEndpoint, int streamId);

    void removeSubscription(ReceiveChannelEndpoint mediaEndpoint, int streamId);

    void newConnection(ReceiveChannelEndpoint channelEndpoint, DriverConnection connection);

    void removeConnection(DriverConnection connection);

    void registerMediaEndpoint(ReceiveChannelEndpoint channelEndpoint);

    void closeMediaEndpoint(ReceiveChannelEndpoint channelEndpoint);

    void removePendingSetup(ReceiveChannelEndpoint channelEndpoint, int sessionId, int streamId);

    void closeSubscription(DriverSubscription subscription);
}
