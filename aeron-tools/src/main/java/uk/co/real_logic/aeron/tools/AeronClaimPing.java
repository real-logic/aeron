package uk.co.real_logic.aeron.tools;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.*;

/**
 * Created by philip on 3/27/15.
 */
public class AeronClaimPing implements PingImpl
{
    private MediaDriver driver = null;
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pingPub = null;
    private Subscription pongSub = null;
    private CountDownLatch pongConnectionLatch = null;
    private CountDownLatch pongedMessageLatch = null;
    private int pingStreamId = 10;
    private int pongStreamId = 11;
    private String pingChannel = "udp://localhost:44444";
    private String pongChannel = "udp://localhost:55555";
    private long rtt;
    private int fragmentCountLimit;
    private BufferClaim bufferClaim;

    public AeronClaimPing()
    {

    }

    public void prepare()
    {
        ctx = new Aeron.Context()
                .newConnectionHandler(this::newPongConnectionHandler);
        dataHandler = new FragmentAssemblyAdapter(this::pongHandler);
        aeron = Aeron.connect(ctx);
        pingPub = aeron.addPublication(pingChannel, pingStreamId);
        pongSub = aeron.addSubscription(pongChannel, pongStreamId, dataHandler);
        pongConnectionLatch = new CountDownLatch(1);

        fragmentCountLimit = 1;
        bufferClaim = new BufferClaim();
    }

    public void connect()
    {
        try
        {
            pongConnectionLatch.await();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public long sendPingAndReceivePong(int msgLen)
    {
        IdleStrategy idle = new BusySpinIdleStrategy();
        pongedMessageLatch = new CountDownLatch(1);

        if (pingPub.tryClaim(msgLen, bufferClaim))
        {
            try
            {
                MutableDirectBuffer buffer = bufferClaim.buffer();
                int offset = bufferClaim.offset();
                buffer.putLong(offset, System.nanoTime());
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return -1;
            }
            finally
            {
                bufferClaim.commit();

                while (pongSub.poll(fragmentCountLimit) <= 0)
                {
                    idle.idle(0);
                }

                try
                {
                    if (pongedMessageLatch.await(10, TimeUnit.SECONDS))
                    {
                        return rtt >> 1;
                    }
                    else
                    {
                        return -1;
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    return -1;
                }
            }
        }
        else
        {
            return sendPingAndReceivePong(msgLen);
        }
    }

    public void shutdown()
    {

    }

    private void newPongConnectionHandler(String channel, int streamId,
                                          int sessionId, String sourceInfo)
    {
        if (channel.equals(pongChannel) && pongStreamId == streamId)
        {
            try
            {
                pongConnectionLatch.countDown();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private void pongHandler(DirectBuffer buffer, int offset, int legnth,
                             Header header)
    {
        long pingTimestamp = buffer.getLong(offset);

        rtt = System.nanoTime() - pingTimestamp;
        try
        {
            pongedMessageLatch.countDown();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
