package io.aeron.samples.mdc;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class MsgPublication
{
    private static final ThreadLocal<BufferClaim> BUFFER_CLAIMS = ThreadLocal.withInitial(BufferClaim::new);

    private final int sessionId;
    private final Publication publication;

    public MsgPublication(int sessionId, Publication publication)
    {
        this.sessionId = sessionId;
        this.publication = publication;
    }

    public int send(DirectBuffer buffer)
    {
        long result = publish(buffer);

        if (result > 0)
        {
            return 1;
        }

        if (result < Publication.ADMIN_ACTION)
        {
            System.err.println("Publication@" + sessionId + " received result: " + result);
        }
        return 0;
    }

    private long publish(DirectBuffer buffer)
    {
        int length = buffer.capacity();

        if (length < publication.maxPayloadLength())
        {
            BufferClaim bufferClaim = BUFFER_CLAIMS.get();
            long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                try
                {
                    MutableDirectBuffer directBuffer = bufferClaim.buffer();
                    int offset = bufferClaim.offset();
                    directBuffer.putBytes(offset, buffer, 0, length);
                    bufferClaim.commit();
                } catch (Exception ex)
                {
                    bufferClaim.abort();
                    throw ex;
                }
            }
            return result;
        } else
        {
            return publication.offer(new UnsafeBuffer(buffer, 0, length));
        }
    }

    void close()
    {
        publication.close();
    }

    int sessionId()
    {
        return sessionId;
    }
}
