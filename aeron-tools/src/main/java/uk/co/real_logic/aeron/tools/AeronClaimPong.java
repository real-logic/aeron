package uk.co.real_logic.aeron.tools;

import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.*;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by philip on 3/27/15.
 */
public class AeronClaimPong implements PongImpl
{
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pongPub = null;
    private Subscription pingSub = null;
    private int pingStreamId = 10;
    private int pongStreamId = 11;
    private String pingChannel = "udp://localhost:44444";
    private String pongChannel = "udp://localhost:55555";
    private int fragmentCountLimit;
    private BusySpinIdleStrategy idle = new BusySpinIdleStrategy();
    private AtomicBoolean running = new AtomicBoolean(true);
    private BufferClaim bufferClaim = null;

    public AeronClaimPong()
    {

    }

    public void prepare()
    {
        ctx = new Aeron.Context();
        dataHandler = new FragmentAssemblyAdapter(this::pingHandler);
        aeron = Aeron.connect(ctx);
        pongPub = aeron.addPublication(pongChannel, pongStreamId);
        pingSub = aeron.addSubscription(pingChannel, pingStreamId, dataHandler);
        fragmentCountLimit = 1;
        bufferClaim = new BufferClaim();
    }

    public void run()
    {
        while (running.get())
        {
            int fragmentsRead = pingSub.poll(fragmentCountLimit);
            idle.idle(fragmentsRead);
        }
    }

    public void shutdown()
    {
        running.set(false);
    }

    public void pingHandler(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (pongPub.tryClaim(length, bufferClaim))
        {
            try
            {
                MutableDirectBuffer newBuffer = bufferClaim.buffer();
                int newOffset = bufferClaim.offset();
                //newBuffer.putBytes(offset, buffer, newOffset, length);
                newBuffer.putBytes(newOffset, buffer, offset, length);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                bufferClaim.commit();
            }
        }
        else
        {
            pingHandler(buffer, offset, length, header);
        }
    }
}
