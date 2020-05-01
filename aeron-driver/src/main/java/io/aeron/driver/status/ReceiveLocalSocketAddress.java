package io.aeron.driver.status;

import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

public class ReceiveLocalSocketAddress
{
    public static final String NAME = "rcv-local-sockaddr";

    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final CountersManager countersManager,
        final int channelStatusId)
    {
        return LocalSocketAddressStatus.allocate(
            tempBuffer,
            countersManager,
            channelStatusId,
            NAME,
            LocalSocketAddressStatus.LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID);
    }
}
