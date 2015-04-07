package uk.co.real_logic.aeron.tools.perf_tools;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by philip on 4/7/15.
 */
public class AeronPing
{
    private int numMsgs = 10000000;
    private int numWarmupMsgs = 100000;
    private int msgLen = 32;
    private long[][] timestamps = null;
    private boolean warmedUp = false;
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pub = null;
    private Subscription sub = null;
    private CountDownLatch connectionLatch = null;
    private int pingStreamId = 10;
    private int pongStreamId = 11;
    private String pingChannel = "udp://localhost:44444";
    private String pongChannel = "udp://localhost:55555";
    private UnsafeBuffer buffer = null;
    private int msgCount = 0;
    private boolean claim = false;
    private BufferClaim bufferClaim = null;

    public AeronPing(boolean claim)
    {
        ctx = new Aeron.Context()
                .newConnectionHandler(this::connectionHandler);
        dataHandler = new FragmentAssemblyAdapter(this::pongHandler);
        aeron = Aeron.connect(ctx);
        pub = aeron.addPublication(pingChannel, pingStreamId);
        sub = aeron.addSubscription(pongChannel, pongStreamId, dataHandler);
        connectionLatch = new CountDownLatch(1);
        buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(msgLen));
        timestamps = new long[2][numMsgs];
        this.claim = claim;
        if (claim)
        {
            bufferClaim = new BufferClaim();
        }
    }

    public void connect()
    {
        try
        {
            connectionLatch.await();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void run()
    {
        for (int i = 0; i < numWarmupMsgs; i++)
        {
            if (claim)
            {
                sendPingAndReceivePong();
            }
            else
            {
                sendPingAndReceivePongClaim();
            }

            while (sub.poll(1) <= 0)
            {

            }
        }
    }

    public void shutdown()
    {
        ctx.close();
        aeron.close();
        pub.close();
        sub.close();
    }

    public void generateGraphs()
    {

    }

    private void connectionHandler(String channel, int streamId,
                                   int sessionId, String sourceInfo)
    {
        if (channel.equals(pongChannel) && pongStreamId == streamId)
        {
            connectionLatch.countDown();
        }
    }

    private void pongHandler(DirectBuffer buffer, int offset, int length,
                             Header header)
    {
        if (buffer.getByte(offset + 0) == (byte)'p')
        {
            timestamps[1][buffer.getInt(offset + 1)] = System.nanoTime();
        }
    }

    private void sendPingAndReceivePong()
    {
        if (!warmedUp)
        {
            buffer.putByte(0, (byte) 'w');
        }
        else
        {
            buffer.putByte(0, (byte)'p');
            buffer.putInt(1, msgCount);
            timestamps[0][msgCount++] = System.nanoTime();
        }
        while (!pub.offer(buffer, 0, msgLen))
        {

        }
    }

    private void sendPingAndReceivePongClaim()
    {
        if (pub.tryClaim(msgLen, bufferClaim))
        {
            try
            {
                MutableDirectBuffer buffer = bufferClaim.buffer();
                int offset = bufferClaim.offset();
                if (!warmedUp)
                {
                    buffer.putByte(offset + 0, (byte) 'w');
                }
                else
                {
                    buffer.putByte(offset + 0, (byte) 'p');
                    buffer.putInt(offset + 1, msgCount);
                    timestamps[0][msgCount++] = System.nanoTime();
                }
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
            sendPingAndReceivePongClaim();
        }
    }

    public static void main(String[] args)
    {
        AeronPing ping = null;

        if (args.length == 0)
        {
            ping = new AeronPing(false);
        }

        ping.connect();
        ping.run();
        ping.shutdown();
        ping.generateGraphs();
    }
}
