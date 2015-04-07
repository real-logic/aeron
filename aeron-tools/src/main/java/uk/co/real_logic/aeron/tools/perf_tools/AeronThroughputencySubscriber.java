package uk.co.real_logic.aeron.tools.perf_tools;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;

import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * Created by philipjohnson1 on 4/2/15.
 */
public class AeronThroughputencySubscriber
{
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pub = null;
    private Subscription sub = null;
    private int pubStreamId = 11;
    private int subStreamId = 10;
    private String subChannel = "udp://localhost:44444";
    private String pubChannel = "udp://localhost:55555";
    private int fragmentCountLimit;
    private IdleStrategy idle = new NoOpIdleStrategy();
    private boolean running = true;
    private long timestamps[] = new long[41111100];
    private BufferClaim bufferClaim = null;
    public AeronThroughputencySubscriber()
    {
        ctx = new Aeron.Context();
        dataHandler = new FragmentAssemblyAdapter(this::msgHandler);
        aeron = Aeron.connect(ctx);
        pub = aeron.addPublication(pubChannel, pubStreamId);
        sub = aeron.addSubscription(subChannel, subStreamId, dataHandler);
        fragmentCountLimit = 2;
        bufferClaim = new BufferClaim();

        while (running)
        {
            int fragmentsRead = sub.poll(fragmentCountLimit);
            //idle.idle(fragmentsRead);
        }

        try
        {
            PrintWriter out = new PrintWriter(new FileOutputStream("sub.ts"));
            for (int i = 1; i < timestamps.length; i++)
            {
                out.println(timestamps[i] - timestamps[i - 1]);
            }
            out.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        sub.close();
        pub.close();
        ctx.close();
        aeron.close();
    }

    public void msgHandler(DirectBuffer buffer, int offset, int length, Header header)
    {
        if (buffer.getByte(offset) == (byte)'q')
        {
            running = false;
            return;
        }
        else
        {
            if (pub.tryClaim(length, bufferClaim))
            {
                try
                {
                    MutableDirectBuffer newBuffer = bufferClaim.buffer();
                    int newOffset = bufferClaim.offset();

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
                msgHandler(buffer, offset, length, header);
            }
        }
    }

    public static void main(String[] args)
    {
        new AeronThroughputencySubscriber();
    }
}
