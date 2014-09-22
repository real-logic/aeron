package uk.co.real_logic.aeron.benchmarks;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogReader;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static java.nio.ByteOrder.nativeOrder;
import static uk.co.real_logic.aeron.common.CommonContext.*;

public class BenchmarkUtil
{
    public static final String CHANNEL = "udp://localhost:40123";
    public static final int STREAM_ID = 10;
    public static final int MESSAGES_PER_RUN = 10;

    public static void useSharedMemoryOnLinux()
    {
        // use shared memory to avoid Disk I/O bottleneck
        if ("Linux".equalsIgnoreCase(System.getProperty("os.name")))
        {
            System.setProperty(ADMIN_DIR_PROP_NAME, "/dev/shm/aeron/conductor");
            System.setProperty(COUNTERS_DIR_PROP_NAME, "/dev/shm/aeron/counters");
            System.setProperty(DATA_DIR_PROP_NAME, "/dev/shm/aeron/data");
        }
    }

    public static void exchangeMessagesBetweenClients(
        final Aeron publishingClient, final Aeron consumingClient, final AtomicBuffer publishingBuffer)
    {
        exchangeMessagesBetweenClients(publishingClient, consumingClient, publishingBuffer, MESSAGES_PER_RUN);
    }

    public static void exchangeMessagesBetweenClients(
        final Aeron publishingClient, final Aeron consumingClient, final AtomicBuffer publishingBuffer, final int messagesPerRun)
    {
        clear(publishingBuffer);

        try (final Publication publication = publishingClient.addPublication(CHANNEL, STREAM_ID, 0))
        {
            final AtomicInteger receivedCount = new AtomicInteger(0);

            final LogReader.DataHandler handler =
                (buffer, offset, length, header) ->
                {
                    final int expectedValue = receivedCount.get();
                    final int value = buffer.getInt(offset, nativeOrder());
                    if (value != expectedValue)
                    {
                        System.err.println("Unexpected value: " + value);
                    }
                    receivedCount.incrementAndGet();
                };

            try (final Subscription subscription = consumingClient.addSubscription(CHANNEL, STREAM_ID, handler))
            {
                for (int i = 0; i < messagesPerRun; i++)
                {
                    publishingBuffer.putInt(0, i, nativeOrder());
                    if (!publication.offer(publishingBuffer))
                    {
                        System.err.println("Unable to send " + i);
                    }
                }

                int read = 0;
                do
                {
                    read += subscription.poll(messagesPerRun);
                }
                while (read < messagesPerRun);

                if (read > messagesPerRun)
                {
                    System.err.println("We've read too many messages: " + read);
                }

                if (receivedCount.get() != messagesPerRun)
                {
                    System.err.println("Data Handler has " + read + " messages");
                }
            }
        }
    }

    public static void clear(final AtomicBuffer buffer)
    {
        buffer.setMemory(0, buffer.capacity(), (byte)0);
    }

    public static Consumer<Subscription> subscriberLoop(final int limit)
    {
        return
            (subscription) ->
            {
                final BackoffIdleStrategy idleStrategy = new BackoffIdleStrategy(
                    100,
                    100,
                    TimeUnit.MICROSECONDS.toNanos(1),
                    TimeUnit.MICROSECONDS.toNanos(100));

                try
                {
                    while (true)
                    {
                        final int fragmentsRead = subscription.poll(limit);
                        idleStrategy.idle(fragmentsRead);
                    }
                }
                catch (final Exception ex)
                {
                    ex.printStackTrace();
                }
            };
    }
}
