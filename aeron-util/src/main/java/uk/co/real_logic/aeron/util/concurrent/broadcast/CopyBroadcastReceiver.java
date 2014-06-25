package uk.co.real_logic.aeron.util.concurrent.broadcast;

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.MessageHandler;

/**
 * .
 */
public class CopyBroadcastReceiver
{
    private static final int SCRATCH_BUFFER_SIZE = 4096;

    private final BroadcastReceiver receiver;
    private final AtomicBuffer scratchBuffer;

    public CopyBroadcastReceiver(final BroadcastReceiver receiver)
    {
        this.receiver = receiver;
        scratchBuffer = new AtomicBuffer(new byte[SCRATCH_BUFFER_SIZE]);
    }

    public int receive(final MessageHandler handler)
    {
        int messagesReceived = 0;
        final long lastSeenLappedCount = receiver.lappedCount();
        while(receiver.receiveNext())
        {
            if (lastSeenLappedCount != receiver.lappedCount())
            {
                throw new IllegalStateException("Unable to keep up with broadcast buffer");
            }

            final int length = receiver.length();
            final int capacity = scratchBuffer.capacity();
            if (length > capacity)
            {
                String msg = String.format("Buffer required size %d but only has %d", length, capacity);
                throw new IllegalStateException(msg);
            }

            final int msgTypeId = receiver.typeId();
            scratchBuffer.putBytes(0, receiver.buffer(), receiver.offset(), length);

            if (!receiver.validate())
            {
                throw new IllegalStateException("Unable to keep up with broadcast buffer");
            }

            handler.onMessage(msgTypeId, scratchBuffer, 0, length);

            messagesReceived++;
        }
        return messagesReceived;
    }

}
