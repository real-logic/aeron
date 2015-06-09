/*
 * Copyright 2015 Kaazing Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.tools.perf_tools;

import org.apache.commons.cli.*;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.tools.MessagesAtMessagesPerSecondInterval;
import uk.co.real_logic.aeron.tools.RateController;
import uk.co.real_logic.aeron.tools.RateControllerInterval;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AeronLatencyUnderLoadPublisher implements RateController.Callback
{
    private Publication pub = null;
    private Subscription sub = null;
    private CountDownLatch connectionLatch = null;
    private FragmentHandler fragmentHandler = null;
    private final int pubStreamId = 10;
    private final int subStreamId = 11;
    private String pubChannel = "udp://localhost:44444";
    private String reflectChannel = "udp://localhost:55555";
    private boolean running = true;
    private final int msgLen = 20;
    private UnsafeBuffer buffer = null;
    private final long timestamps[] = new long[41111100];
    private int msgCount = 0;
    private final BufferClaim bufferClaim;
    private int warmups = 0;
    private final double means[] = new double[7];

    public AeronLatencyUnderLoadPublisher(final String[] args)
    {
        try
        {
            parseArgs(args);
        }
        catch (final ParseException e)
        {
            throw new RuntimeException(e);
        }
        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(this::connectionHandler);
        fragmentHandler = new FragmentAssemblyAdapter(this::msgHandler);
        final Aeron aeron = Aeron.connect(ctx);
        System.out.println("Reflect: " + reflectChannel + " Pub: " + pubChannel);
        pub = aeron.addPublication(pubChannel, pubStreamId);
        sub = aeron.addSubscription(reflectChannel, subStreamId);
        connectionLatch = new CountDownLatch(1);
        final IdleStrategy idle = new BusySpinIdleStrategy();
        bufferClaim = new BufferClaim();

        final List<RateControllerInterval> intervals = new ArrayList<>();
        intervals.add(new MessagesAtMessagesPerSecondInterval(100, 10));
        intervals.add(new MessagesAtMessagesPerSecondInterval(1000, 100));
        intervals.add(new MessagesAtMessagesPerSecondInterval(10000, 1000));
        intervals.add(new MessagesAtMessagesPerSecondInterval(100000, 10000));
        intervals.add(new MessagesAtMessagesPerSecondInterval(1000000, 100000));
        intervals.add(new MessagesAtMessagesPerSecondInterval(10000000, 1000000));
        intervals.add(new MessagesAtMessagesPerSecondInterval(30000000, 3000000));
        buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(msgLen));
        msgCount = 0;

        RateController rateController = null;
        try
        {
            rateController = new RateController(this, intervals);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }

        final Runnable task = () -> {
            while (running)
            {
                while (sub.poll(fragmentHandler, 1) <= 0 && running)
                {
                }
            }
            System.out.println("Done");
        };
        final Thread subThread = new Thread(task);
        subThread.start();

        try
        {
            connectionLatch.await();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }

        final int warmUpMsgs = 100000;
        for (int i = 0; i < warmUpMsgs; i++)
        {
            while (pub.tryClaim(buffer.capacity(), bufferClaim) < 0L)
            {
                idle.idle(1);
            }
            final MutableDirectBuffer buffer = bufferClaim.buffer();
            final int offset = bufferClaim.offset();
            buffer.putByte(offset, (byte) 'w');
            bufferClaim.commit();
        }
        try
        {
            Thread.sleep(1000);
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }
        System.out.println("warmup msgs received: " + warmups);
        final int start = (int)System.currentTimeMillis();
        while (rateController.next())
        {

        }
        final int total = (int)(System.currentTimeMillis() - start) / 1000;
        buffer.putByte(0, (byte)'q');

        while (pub.offer(buffer, 0, buffer.capacity()) < 0L)
        {
            idle.idle(0);
        }

        System.out.println("Duration: " + total + " seconds");
        try
        {
            Thread.sleep(1000);
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }

        running = false;

        try
        {
            subThread.join();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }

        aeron.close();

        try
        {
            computeStats();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public int onNext()
    {
        while (pub.tryClaim(buffer.capacity(), bufferClaim) < 0L)
        {
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();
        buffer.putByte(offset, (byte) 'p');
        buffer.putInt(offset + 1, msgCount++);
        buffer.putLong(offset + 5, System.nanoTime());
        bufferClaim.commit();
        return msgLen;
    }

    private void connectionHandler(
        final String channel,
        final int streamId,
        final int sessionId,
        final long position,
        final String sourceIdentity)
    {
        System.out.println(channel + " " + streamId);
        if (channel.equals(reflectChannel) && subStreamId == streamId)
        {
            connectionLatch.countDown();
            System.out.println("Connected");
        }
    }

    private void msgHandler(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (buffer.getByte(offset) == (byte)'p')
        {
            timestamps[buffer.getInt(offset + 1)] = System.nanoTime() - buffer.getLong(offset + 5);
        }
        else
        {
            warmups++;
        }
    }

    private void parseArgs(final String[] args) throws ParseException
    {
        final Options options = new Options();
        options.addOption("c", "claim", false, "Use Try/Claim");
        options.addOption("", "pubChannel", true, "Primary publishing channel");
        options.addOption("", "reflectChannel", true, "Reflection channel");

        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);


        if (command.hasOption("pubChannel"))
        {
            pubChannel = command.getOptionValue("pubChannel", "udp://localhost:44444");
        }

        if (command.hasOption("reflectChannel"))
        {
            reflectChannel = command.getOptionValue("reflectChannel", "udp://localhost:55555");
        }
    }

    private void computeStats() throws IOException
    {
        means[0] = computeStats(0, 100, "10mps");
        means[1] = computeStats(100, 1100, "100mps");
        means[2] = computeStats(1100, 11000, "1Kmps");
        means[3] = computeStats(11000, 111000, "10Kmps");
        means[4] = computeStats(111000, 1111000, "100Kmps");
        means[5] = computeStats(1111000, 11111000, "1Mmps");
        means[6] = computeStats(11111000, 41111000, "3Mmps");

        generateScatterPlot();
    }

    private double computeStats(final int start, final int end, final String title)
    {
        double sum = 0.0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

        for (int i = start; i < end; i++)
        {
            final double ts = timestamps[i] / 1000.0;
            if (ts < 0)
            {
                System.exit(1);
            }
            sum += ts;
            if (ts  < min)
            {
                min = ts;
            }
            if (ts > max)
            {
                max = ts;
            }
        }
        System.out.println("Mean latency for " + title + ": " + sum / (end - start));
        return sum / (end - start);
    }

    private void generateScatterPlot() throws IOException
    {
        final BufferedImage image = new BufferedImage(1800, 1000, BufferedImage.TYPE_INT_ARGB);
        final Graphics2D g2 = image.createGraphics();
        final FontMetrics fm = g2.getFontMetrics();
        final String filename = "throughputency.png";
        final File imageFile = new File(filename);

        final int height = 940;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for (final long timestamp : timestamps)
        {
            final double ts = timestamp / 1000.0;
            if (ts < min)
            {
                min = ts;
            }
            if (ts > max)
            {
                max = ts;
            }
        }
        final double stepY = height / max;

        g2.setColor(Color.white);
        g2.fillRect(0, 0, 1800, 1000);
        g2.setColor(Color.black);
        g2.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g2.drawString("Latency ScatterPlot (microseconds)",
                900 - fm.stringWidth("Latency ScatterPlot (microseconds)") / 2, 20);
        g2.drawString("" + max, 10, 20);
        g2.drawLine(100, 20, 100, 960);
        g2.drawLine(100, 960, 1790, 960);
        int start = 0;
        int end = 100;
        final double width = 1690.0 / 7.0;
        g2.setColor(Color.red);
        plotSubset(g2, start, end, "10 msgs/sec", 100, width, stepY, means[0]);

        start = 100;
        end = 1100;
        g2.setColor(Color.green);
        plotSubset(g2, start, end, "100 msgs/sec", 100 + width, width, stepY, means[1]);

        start = 1100;
        end = 11100;
        g2.setColor(Color.blue);
        plotSubset(g2, start, end, "1K msgs/sec", 100 + width * 2, width, stepY, means[2]);

        start = 11100;
        end = 111100;
        g2.setColor(Color.cyan);
        plotSubset(g2, start, end, "10K msgs/sec", 100 + width * 3, width, stepY, means[3]);

        start = 111100;
        end = 1111100;
        g2.setColor(Color.magenta);
        plotSubset(g2, start, end, "100K msgs/sec", 100 + width * 4, width, stepY, means[4]);

        start = 1111100;
        end = 11111100;
        g2.setColor(Color.yellow);
        plotSubset(g2, start, end, "1M msgs/sec", 100 + width * 5, width, stepY, means[5]);

        start = 11111100;
        end = 41111100;
        g2.setColor(Color.orange);
        plotSubset(g2, start, end, "3M msgs/sec", 100 + width * 6, width, stepY, means[6]);


        ImageIO.write(image, "png", imageFile);
    }

    private void plotSubset(
        final Graphics2D g,
        final int start,
        final int end,
        final String title,
        final double startX,
        final double width,
        final double stepY,
        final double mean)
    {
        final FontMetrics fm = g.getFontMetrics();
        final Color color = g.getColor();
        g.setColor(Color.black);
        g.drawString(title, (int)(startX + width / 2 - fm.stringWidth(title) / 2), 975);
        final String tmp = String.format("Mean: %.3fus", mean);
        g.drawString(tmp, (int)(startX + width / 2 - fm.stringWidth(tmp) / 2), 990);
        g.setColor(color);
        final double stepX = width / (end - start);
        for (int i = start; i < end; i++)
        {
            final int posX = (int)(startX + stepX * (i - start));
            final int posY = 960 - ((int)(stepY * (timestamps[i] / 1000.0)) + 1);
            g.drawLine(posX, posY, posX, 960);
        }
    }
    public static void main(final String[] args)
    {
        new AeronLatencyUnderLoadPublisher(args);
    }
}
