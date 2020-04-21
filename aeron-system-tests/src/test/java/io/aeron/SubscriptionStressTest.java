package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.test.MediaDriverTestWatcher;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SubscriptionStressTest
{
    private TestMediaDriver mediaDriverA;
    private TestMediaDriver mediaDriverB;
    private Aeron aeronA;
    private Aeron aeronB;

    @RegisterExtension
    public MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context contextA = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .aeronDirectoryName(CommonContext.getAeronDirectoryName() + "/A")
            .threadingMode(ThreadingMode.DEDICATED)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        mediaDriverA = TestMediaDriver.launch(contextA, testWatcher);

        final MediaDriver.Context contextB = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.DEDICATED)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true);

//        mediaDriverB = TestMediaDriver.launch(contextB);

        aeronA = Aeron.connect(new Aeron.Context().aeronDirectoryName(contextA.aeronDirectoryName()));
//        aeronB = Aeron.connect(new Aeron.Context().aeronDirectoryName(contextB.aeronDirectoryName()));
    }

    @Test
    void shouldAddRemoveSubscriptionUnicast() throws InterruptedException
    {
        final String channel = "aeron:udp?endpoint=localhost:24326";
        final int streamId = 1001;
        CountDownLatch startLatch = new CountDownLatch(2);

        try (Publication pubA = aeronA.addPublication(channel, streamId))
        {
            final Thread pubThread = new Thread(publishRunnable2(startLatch, pubA));
            pubThread.start();
            long retryCounter = 0;

            for (int i = 0; i < 10000; i++)
            {
                try (Subscription subB = aeronA.addSubscription(
                    channel, streamId, image -> System.out.printf("Image: %s%n", image.sourceIdentity()), image -> {}))
                {
                    if (i % 10 == 0)
                    {
                        try
                        {
                            Tests.await(subB::isConnected, TimeUnit.SECONDS.toNanos(20));
                            Tests.await(() -> 0 != subB.poll((buffer, offset, length, header) -> {}, 20),
                                    TimeUnit.SECONDS.toNanos(4));
                        }
                        catch (TimeoutException e)
                        {
                            final String msg = String.format(
                                "Subscription: %d%nPosition: %d, limit: %d, re-subs: %d%n",
                                subB.registrationId(), pubA.position(), pubA.positionLimit(), i + 1);
                            throw new RuntimeException(msg, e);
                        }
                    }

                    subB.isConnected();
                    // No-op
                }
                catch (AeronException e)
                {
                    if (retryCounter == 0)
                    {
                        e.printStackTrace();
                    }
                    retryCounter++;
                }

                if ((i + 1) % 100 == 0)
                {
                    System.out.printf("Position: %d, limit: %d, re-subs: %d, retries: %d%n", pubA.position(), pubA.positionLimit(), i + 1, retryCounter);
                }
            }

            pubThread.interrupt();

            pubThread.join();
        }
    }

    @Test
    void shouldRapidlyAddAndRemoveSubscriptions() throws InterruptedException
    {
        CountDownLatch startLatch = new CountDownLatch(2);

        final String channel = "aeron:udp?endpoint=224.20.30.39:24326|fc=max|reliable=false|tether=false";
        final int streamId = 1001;

        try (Publication pubA = aeronA.addPublication(channel, streamId);
            Subscription subA = aeronA.addSubscription(channel, streamId))
        {
            Tests.await(() -> subA.isConnected() && pubA.isConnected(), TimeUnit.SECONDS.toNanos(2));

            final Thread pubThread = new Thread(publishRunnable(startLatch, pubA));
            final Thread subThread = new Thread(pollRunnable(startLatch, subA));

            pubThread.setDaemon(true);
            pubThread.start();

            subThread.setDaemon(true);
            subThread.start();

            startLatch.await();

            for (int i = 0; i < 10000; i++)
            {
                try (Subscription subB = aeronB.addSubscription(
                    channel, streamId, image -> System.out.printf("Image: %s%n", image.sourceIdentity()), image -> {}))
                {
                    if (i % 10 == 0)
                    {
                        Tests.await(subB::isConnected, TimeUnit.SECONDS.toNanos(4));
                        Tests.await(() -> 0 != subB.poll((buffer, offset, length, header) -> {}, 10), TimeUnit.SECONDS.toNanos(4));
                    }

                    subB.isConnected();
                    // No-op
                }

                if ((i + 1) % 100 == 0)
                {
                    System.out.printf("Position: %d, limit: %d, re-subs: %d%n", pubA.position(), pubA.positionLimit(), i + 1);
                }
            }

            Tests.sleep(1000);

            pubThread.interrupt();
            subThread.interrupt();
            pubThread.join();
            subThread.join();

            System.out.printf("Position: " + pubA.position());
        }
    }

    private Runnable publishRunnable2(final CountDownLatch startLatch, final Publication pubA)
    {
        return () ->
        {
            final YieldingIdleStrategy yieldingIdleStrategy = new YieldingIdleStrategy();
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[32]);
            unsafeBuffer.putLong(0, 1);
            unsafeBuffer.putLong(8, 1);
            unsafeBuffer.putLong(16, 2);
            unsafeBuffer.putLong(24, 3);
            startLatch.countDown();

            long lastError = 0;

            while (!Thread.currentThread().isInterrupted())
            {
                for (int i = 0; i < 50; i++)
                {
                    long position = pubA.offer(unsafeBuffer);

                    if (position > pubA.positionLimit())
                    {
                        System.out.printf("%d > %d%n", position, pubA.positionLimit());
                    }

                    if (position < 0)
                    {
                        if (position != lastError)
                        {
                            lastError = position;
                            System.out.println(System.currentTimeMillis() + " - " + printError(lastError) + ": true" + " @" + pubA.position() + "/" + pubA.positionLimit());
                        }
                    }
                    else
                    {
                        if (lastError != 0)
                        {
                            System.out.println(printError(lastError) + ": false");
                            lastError = 0;
                        }
                    } // 1587436090026

                    yieldingIdleStrategy.idle(position <= 0 ? 0 : 1);
                }

                LockSupport.parkNanos(1000000);
            }
        };
    }

    private String printError(final long position)
    {
        switch ((int) position)
        {
            case (int) Publication.NOT_CONNECTED:
                return "NOT_CONNECTED";
            case (int) Publication.BACK_PRESSURED:
                return "BACK_PRESSURED";
            case (int) Publication.ADMIN_ACTION:
                return "ADMIN_ACTION";
            case (int) Publication.CLOSED:
                return "CLOSED";
            case (int) Publication.MAX_POSITION_EXCEEDED:
                return "MAX_POSITION_EXCEEDED";
        }

        return "UNKNOWN";
    }

    private Runnable publishRunnable(final CountDownLatch startLatch, final Publication pubA)
    {
        return () ->
                {
                    final YieldingIdleStrategy yieldingIdleStrategy = new YieldingIdleStrategy();
                    final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[32]);
                    unsafeBuffer.putLong(0, 1);
                    unsafeBuffer.putLong(8, 1);
                    unsafeBuffer.putLong(16, 2);
                    unsafeBuffer.putLong(24, 3);
                    startLatch.countDown();

                    while (!Thread.currentThread().isInterrupted())
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            long position = pubA.offer(unsafeBuffer);

                            if (position == Publication.CLOSED ||
                                position == Publication.NOT_CONNECTED ||
                                position == Publication.MAX_POSITION_EXCEEDED)
                            {
                                System.err.println("Failed: " + position);
                                return;
                            }

                            yieldingIdleStrategy.idle(position <= 0 ? 0 : 1);
                        }

                        LockSupport.parkNanos(1000000);
                    }
                };
    }

    private Runnable pollRunnable(final CountDownLatch startLatch, final Subscription pubA)
    {
        return () ->
        {
            startLatch.countDown();
            final YieldingIdleStrategy yieldingIdleStrategy = new YieldingIdleStrategy();

            while (!Thread.currentThread().isInterrupted())
            {
                yieldingIdleStrategy.idle(pubA.poll((buffer, offset, length, header) -> {}, 20));
            }
        };
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeronA, aeronB, mediaDriverA, mediaDriverB);
    }
}
