package uk.co.real_logic.aeron.tools.perf_tools;

import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by philip on 4/7/15.
 */
public class AeronPing implements NewConnectionHandler
{
    private int numMsgs = 1000000;
    private int numWarmupMsgs = 50000;
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
    private double sorted[] = null;
    private double tmp[] = null;

    public AeronPing(boolean claim)
    {
        ctx = new Aeron.Context()
                .newConnectionHandler(this);
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
        while (pub.offer(buffer, 0, msgLen) <= 0)
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

        final BufferedImage image = new BufferedImage(1800, 1000, BufferedImage.TYPE_INT_ARGB);
        final Graphics2D g = image.createGraphics();
        g.setColor(Color.white);
        g.fillRect(0, 0, 1800, 1000);
        generateScatterPlot(min, max, .9);
        generateScatterPlot(min, max, .99);
        generateScatterPlot(min, max, .999);
        generateScatterPlot(min, max, .9999);
        generateScatterPlot(min, max, .99999);
        generateScatterPlot(min, max, .999999);

        g.setColor(Color.black);
        g.drawString(String.format("Mean: %.3fus Std. Dev %.3fus", mean, stdDev), 20, 940);
        g.drawString("Min: " + min + " Index: " + minIdx, 20, 960);
        g.drawString("Max: " + max + " Index: " + maxIdx, 20, 980);

        final String filename = "Aeron_RTT.png";
        final File imageFile = new File(filename);
        try
        {
            ImageIO.write(image, "png", imageFile);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        System.out.println("Completed");
        System.out.println("Num Messages: " + numMsgs);
        System.out.println("Message Length: " + msgLen);
        System.out.format("Mean: %.3fus\n", mean);
        System.out.format("Standard Deviation: %.3fus\n", stdDev);

        System.out.println("Min: " + min + " Index: " + minIdx);
        System.out.println("Max: " + max + " Index: " + maxIdx);
    }

    private void generateScatterPlot(double min, double max, double percentile)
    {
        final int width = 390;
        final int height = 370;
        final int num = (int)((numMsgs - 1) * percentile);
        final double newMax = sorted[num];
        final String title = "Latency Scatterplot (us) " + percentile + " percentile";



        final File file = new File(percentile + "_percentile.dat");
        try
        {
            final PrintWriter out = new PrintWriter(file);
            for (int i = 0; i < numMsgs; i++)
            {
                if (tmp[i] <= newMax)
                {
                    out.println(i + "\t" + tmp[i]);
                }
            }
            out.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void onNewConnection(String channel, int streamId,
                                   int sessionId, final long position, String sourceInfo)
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
        while (pub.offer(buffer, 0, msgLen) <= 0)
        {

        }

        while (sub.poll(1) <= 0)
        {

        }
    }

    private void sendPingAndReceivePongClaim()
    {
        if (pub.tryClaim(msgLen, bufferClaim) > 0)
        {
            try
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
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

    public void generateHistogram()
    {
        final HashMap<Double, Integer> histogram = new HashMap<Double, Integer>();
        int num = 0;
        int maxNum = 0;
        double maxVal = 0;
        for (int i = 0; i < tmp.length; i++)
        {
            final double val = round(tmp[i], 6, BigDecimal.ROUND_HALF_UP);
            if (val > maxVal)
            {
                maxVal = val;
            }
            if (histogram.containsKey(val))
            {
                num = histogram.get(val);
            }
            else
            {
                num = 0;
            }
            num++;
            if (num > maxNum)
            {
                maxNum = num;
            }
            histogram.put(val, num);
        }

        System.out.println("There are " + histogram.size() + " histogram values");
        final BufferedImage image = new BufferedImage(1800, 1000, BufferedImage.TYPE_INT_ARGB);
        final Graphics2D g = image.createGraphics();
        final FontMetrics fm = g.getFontMetrics();
        g.setColor(Color.white);
        g.fillRect(0, 0, 1800, 1000);
        final double stepX = 1600 / maxVal;
        final double stepY = 900.0 / maxNum;
        g.setColor(Color.black);
        g.drawLine(100, 50, 100, 950);
        g.drawLine(100, 950, 1700, 950);
        g.drawString("0.0", 100, 960);
        g.drawString(maxVal + "", 1700, 960);
        g.drawString("RTT Latency Value (microseconds)", 900 - fm.stringWidth("RTT Latency Value (microseconds)") / 2, 970);
        g.drawString(maxNum + "", 10, 50);
        final AffineTransform orig = g.getTransform();

        g.translate(60, 400);
        g.rotate(-Math.PI / 2);
        g.drawString("Number of occurrences", -fm.stringWidth("Number of occurrences") / 2, 0);
        g.rotate(Math.PI / 2);
        g.translate(-60, -400);

        g.setColor(Color.red);
        for (Map.Entry<Double, Integer> entry : histogram.entrySet())
        {
            final int xPos = 100 + (int)(entry.getKey() * stepX);
            final int yPos = 950 - ((int)(entry.getValue() * stepY) + 1);
            g.fillRect(xPos, yPos, 1, (int)(entry.getValue() * stepY) + 1);
        }

        final String filename = "PingHistogram.png";
        final File imageFile = new File(filename);
        try
        {
            ImageIO.write(image, "png", imageFile);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private double round(double unrounded, int precision, int roundingMode)
    {
        final BigDecimal bd = new BigDecimal(unrounded);
        final BigDecimal rounded = bd.setScale(precision, roundingMode);
        return rounded.doubleValue();
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
        ping.generateHistogram();
    }
}
