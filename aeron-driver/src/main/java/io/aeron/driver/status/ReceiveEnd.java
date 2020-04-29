package io.aeron.driver.status;

import io.aeron.status.ChannelEndStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

public class ReceiveEnd
{
    public static final String NAME = "rcv-end";

    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final int channelStatusId)
    {
        return ChannelEndStatus.allocate(
            tempBuffer, countersManager, channelStatusId, NAME, ChannelEndStatus.RECEIVE_END_STATUS_TYPE_ID);
    }
}
