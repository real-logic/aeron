package uk.co.real_logic.aeron.mediadriver;

import java.nio.ByteBuffer;

/**
 * Encapsulates the information associated with a channel
 * to send on. Processed in the SenderThread.
 */
public class SenderChannel
{

    private final UdpDestination destination;
    private final UdpTransport transport;

    public SenderChannel(final UdpDestination destination, final UdpTransport transport)
    {
        this.destination = destination;
        this.transport = transport;
    }

    public void process()
    {
        // TODO: blocking due to flow control
        // read from term buffer
        final ByteBuffer buffer = null;
        try
        {
            transport.sendTo(buffer, destination.remote());
        }
        catch (final Exception e)
        {
            // TODO: error logging
            e.printStackTrace();
        }
    }

}
