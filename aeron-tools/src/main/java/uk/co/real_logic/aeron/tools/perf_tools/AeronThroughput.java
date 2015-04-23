package uk.co.real_logic.aeron.tools.perf_tools;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.CommonContext;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.console.ContinueBarrier;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by philip on 4/22/15.
 */
public class AeronThroughput
{
    private static final int STREAM_ID = 10;
    private static final String CHANNEL = "udp://localhost:55555";
    private static final int MESSAGE_LENGTH = 20;
    private static final long NUMBER_OF_MESSAGES = 1000000000;
    private static final long LINGER_TIMEOUT_MS = 2000;
    private static final int FRAGMENT_COUNT_LIMIT = 1;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final IdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static long iterations = 0;

    public static void printRate(
            final double messagesPerSec,
            final double bytesPerSec,
            final long totalMessages,
            final long totalBytes)
    {
       // System.out.println(
         //       String.format(
           //             "%s msgs/sec, %s/sec, totals %s messages %s",
             //           humanReadableCount((long)messagesPerSec, true),
               //         humanReadableByteCount((long)bytesPerSec, false),
                 //       humanReadableCount(totalMessages, true), humanReadableByteCount(totalBytes, false)));
    }

    public static DataHandler rateReporterHandler(final RateReporter reporter)
    {
        return (buffer, offset, length, header) -> reporter.onMessage(1, length);
    }

    public static Consumer<Subscription> subscriberLoop(final int limit, final AtomicBoolean running)
    {
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        return subscriberLoop(limit, running, idleStrategy);
    }

    public static Consumer<Subscription> subscriberLoop(
            final int limit, final AtomicBoolean running, final IdleStrategy idleStrategy)
    {
        return
                (subscription) ->
                {
                    try
                    {
                        while (running.get())
                        {
                            final int fragmentsRead = subscription.poll(limit);
                            idleStrategy.idle(fragmentsRead);
                        }
                    }
                    catch (final Exception ex)
                    {
                        LangUtil.rethrowUnchecked(ex);
                    }
                };
    }

    private static String humanReadableByteCount(final long bytes, final boolean si)
    {
        final int unit = si ? 1000 : 1024;
        if (bytes < unit)
        {
            return bytes + "B";
        }
        final int exp = (int)(Math.log(bytes) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.2f%sB", bytes / Math.pow(unit, exp), pre);
    }

    private static String humanReadableCount(final long val, final boolean si)
    {
        final int unit = si ? 1000 : 1024;
        if (val < unit)
        {
            return val + "";
        }

        final int exp = (int)(Math.log(val) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.3f%s", val / Math.pow(unit, exp), pre);
    }

    public static void main(final String[] args) throws Exception
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new NoOpIdleStrategy())
                .receiverIdleStrategy(new NoOpIdleStrategy())
                .senderIdleStrategy(new NoOpIdleStrategy())
                .dirsDeleteOnExit(true);

        final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), AeronThroughput::printRate);
        final DataHandler rateReporterHandler = rateReporterHandler(reporter);
        final ExecutorService executor = Executors.newFixedThreadPool(3);

        final String embeddedDirName = CommonContext.generateEmbeddedDirName();
        ctx.dirName(embeddedDirName);
        final Aeron.Context context = new Aeron.Context();
        context.dirName(embeddedDirName);

        final AtomicBoolean running = new AtomicBoolean(true);

        try (final MediaDriver ignore = MediaDriver.launch(ctx);
             final Aeron aeron = Aeron.connect(context, executor);
             final Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
             final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, rateReporterHandler))
        {
            executor.execute(reporter);
            executor.execute(() -> AeronThroughput.subscriberLoop(FRAGMENT_COUNT_LIMIT, running).accept(subscription));

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            //System.out.format(
              //      "\nStreaming %,d messages of size %d bytes to %s on stream Id %d\n",
                //    NUMBER_OF_MESSAGES, MESSAGE_LENGTH, CHANNEL, STREAM_ID);

            long backPressureCount = 0;

            final long start = System.currentTimeMillis();
            for (long i = 0; i < NUMBER_OF_MESSAGES; i++)
            {
                ATOMIC_BUFFER.putLong(0, i);

                while (publication.offer(ATOMIC_BUFFER, 0, ATOMIC_BUFFER.capacity()) < 0)
                {
                    backPressureCount++;
                    OFFER_IDLE_STRATEGY.idle(0);
                }
            }
            final long stop = System.currentTimeMillis();
            System.out.println("Average throughput for " +
                    MESSAGE_LENGTH + " byte messages was: " +
                    humanReadableCount(NUMBER_OF_MESSAGES / ((stop - start) / 1000), false) + " msgs/sec");
            //System.out.println("Done streaming. backPressureRatio=" + ((double)backPressureCount / NUMBER_OF_MESSAGES));

            if (0 < LINGER_TIMEOUT_MS)
            {
                //System.out.println("Lingering for " + LINGER_TIMEOUT_MS + " milliseconds...");
                Thread.sleep(LINGER_TIMEOUT_MS);
            }


            running.set(false);
            reporter.halt();
            executor.shutdown();
        }
    }
}
