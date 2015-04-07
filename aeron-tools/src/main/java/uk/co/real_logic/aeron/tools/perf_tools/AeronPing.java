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

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    private double sorted[] =null;
    private double tmp[] = null;

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
        System.out.println("Sending Warmup Messages");
        for (int i = 0; i < numWarmupMsgs; i++)
        {
            if (!claim)
            {
                sendPingAndReceivePong();
            }
            else
            {
                sendPingAndReceivePongClaim();
            }
        }
        warmedUp = true;

        System.out.println("Sending Real Messages");
        for (int i = 0; i < numMsgs; i++)
        {
            if (!claim)
            {
                sendPingAndReceivePong();
            }
            else
            {
                sendPingAndReceivePongClaim();
            }
        }

        buffer.putByte(0, (byte) 'q');
        while (!pub.offer(buffer, 0, msgLen))
        {

        }
        System.out.println("Done");
    }

    public void shutdown()
    {
        //ctx.close();
        //pub.close();
        //sub.close();
        aeron.close();
    }

    public void generateGraphs()
    {
        double sum = 0.0;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        int maxIdx = 0;
        int minIdx = 0;
        double mean = 0.0;
        double stdDev = 0.0;

        tmp = new double[timestamps[0].length];
        sorted = new double[tmp.length];

        for (int i = 0; i < tmp.length; i++)
        {
            tmp[i] = (timestamps[1][i] - timestamps[0][i]) / 1000.0;
            if (tmp[i] > max)
            {
                max = tmp[i];
                maxIdx = i;
            }
            if (tmp[i] < min)
            {
                min = tmp[i];
                minIdx = i;
            }
            sum += tmp[i];
        }

        mean = sum / tmp.length;

        System.arraycopy(tmp, 0, sorted, 0, tmp.length);
        Arrays.sort(sorted);

        sum = 0;
        for (int i = 0; i < tmp.length; i++)
        {
            sum += Math.pow(mean - tmp[i], 2);
        }
        stdDev = Math.sqrt(sum / tmp.length);

        BufferedImage image = new BufferedImage(1800, 1000, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = image.createGraphics();
        g.setColor(Color.white);
        g.fillRect(0, 0, 1800, 1000);
        generateScatterPlot(g, 0, 0, min, max, .9);
        generateScatterPlot(g, 600, 0, min, max, .99);
        generateScatterPlot(g, 1200, 0, min, max, .999);
        generateScatterPlot(g, 0, 500, min, max, .9999);
        generateScatterPlot(g, 600, 500, min, max, .99999);
        generateScatterPlot(g, 1200, 500, min, max, .999999);

        g.setColor(Color.black);
        g.drawString(String.format("Mean: %.3fus Std. Dev %.3fus", mean, stdDev), 20, 940);
        g.drawString("Min: " + min + " Index: " + minIdx, 20, 960);
        g.drawString("Max: " + max + " Index: " + maxIdx, 20, 980);

        String filename = "Aeron_RTT.png";
        File imageFile = new File(filename);
        try
        {
            ImageIO.write(image, "png", imageFile);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("Completed");
    }

    private void generateScatterPlot(Graphics2D g, int x, int y, double min, double max, double percentile)
    {
        FontMetrics fm = g.getFontMetrics();
        int width = 390;
        int height = 370;
        int num = (int)((numMsgs - 1) * percentile);
        double newMax = sorted[num];
        double stepY = (double)(height / (newMax - min));
        double stepX = (double)width / (double)num;
        String title = "Latency Scatterplot (us) " + percentile + " percentile";

        System.out.println("Generating graph for " + percentile + " percentile");
        g.setColor(Color.black);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.drawString(title, x + 100 + width / 2 - fm.stringWidth(title) / 2, y + 20);
        g.drawString("" + newMax, x + 10, y + 20);
        g.drawString("" + min, x + 10, y + 390);
        g.drawLine(x + 100, y + 20, x + 100, y + 390);
        g.drawLine(x + 100, y + 390, x + 490, y + 390);

        g.setColor(Color.red);
        int idx = 0;
        for (int i = 0; i < numMsgs; i++)
        {
            if (tmp[i] <= newMax)
            {
                int posX = x + 100 + (int)(stepX * (double)idx);
                int posY = y + 390 - (int)(stepY * (tmp[i] - min));
                g.fillRect(posX, posY, 1, 1);
                idx++;
            }
        }
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

        while (sub.poll(1) <= 0)
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
                while (sub.poll(1) <= 0)
                {

                }
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
        else
        {
            if (args[0].equalsIgnoreCase("--claim") || args[0].equalsIgnoreCase("-c"))
            {
                ping = new AeronPing(true);
            }
            else
            {
                ping = new AeronPing(false);
            }
        }

        ping.connect();
        ping.run();
        ping.shutdown();
        ping.generateGraphs();
    }
}
