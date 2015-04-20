package uk.co.real_logic.aeron.tools.perf_tools;

import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by philip on 4/7/15.
 */
public class AeronPong
{
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pongPub = null;
    private Subscription pingSub = null;
    private final int pingStreamId = 10;
    private final int pongStreamId = 11;
    private final String pingChannel = "udp://localhost:44444";
    private final String pongChannel = "udp://localhost:55555";
    private final AtomicBoolean running = new AtomicBoolean(true);
    private boolean claim = false;
    private BufferClaim bufferClaim = null;

    public AeronPong(final boolean claim)
    {
        ctx = new Aeron.Context();
        if (claim)
        {
            dataHandler = new FragmentAssemblyAdapter(this::pingHandlerClaim);
        }
        else
        {
            dataHandler = new FragmentAssemblyAdapter(this::pingHandler);
        }
        aeron = Aeron.connect(ctx);
        pongPub = aeron.addPublication(pongChannel, pongStreamId);
        pingSub = aeron.addSubscription(pingChannel, pingStreamId, dataHandler);
        this.claim = claim;
        if (claim)
        {
            bufferClaim = new BufferClaim();
        }
    }

    public void run()
    {
        while (running.get())
        {
            pingSub.poll(1);
        }
    }

    public void shutdown()
    {
        //ctx.close();
        //pongPub.close();
        //pingSub.close();
        aeron.close();
    }

    private void pingHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (buffer.getByte(offset + 0) == (byte)'q')
        {
            running.set(false);
            return;
        }
        while (pongPub.offer(buffer, offset, length) < 0L)
        {
        }
    }

    private void pingHandlerClaim(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (buffer.getByte(offset + 0) == (byte)'q')
        {
            running.set(false);
            return;
        }
        if (pongPub.tryClaim(length, bufferClaim) >= 0)
        {
            try
            {
                final MutableDirectBuffer newBuffer = bufferClaim.buffer();
                newBuffer.putBytes(bufferClaim.offset(), buffer, offset, length);
            }
            catch (final Exception e)
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
            pingHandlerClaim(buffer, offset, length, header);
        }
    }

    public static void main(final String[] args)
    {
        AeronPong pong = null;

        if (args.length == 0)
        {
            pong = new AeronPong(false);
        }
        else
        {
            if (args[0].equalsIgnoreCase("--claim") || args[0].equalsIgnoreCase("-c"))
            {
                pong = new AeronPong(true);
            }
            else
            {
                pong = new AeronPong(false);
            }
        }

        pong.run();
        pong.shutdown();
    }
}
