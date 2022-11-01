/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SlowTest
@ExtendWith(InterruptingTestCallback.class)
class ConcurrentPublicationTermRotationRaceTest
{
    private static final int NUM_PUBLISHERS = Math.max(Math.min(Runtime.getRuntime().availableProcessors() / 2, 8), 4);
    private static final int NUM_MESSAGES = NUM_PUBLISHERS * 50_000;
    private static final int ITERATIONS = 200;
    private MediaDriver mediaDriver;
    private Aeron aeron;

    @BeforeEach
    void setup()
    {
        final String aeronDir = CommonContext.AERON_DIR_PROP_DEFAULT + "-concurrent-publication";
        mediaDriver = MediaDriver.launch(
            new MediaDriver.Context().dirDeleteOnStart(true).aeronDirectoryName(aeronDir));
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, mediaDriver);
    }

    @Test
    @InterruptAfter(300)
    void handleTermIdMovingAheadBetweenPositionChecksAndTheTermOffsetIncrement() throws InterruptedException
    {
        for (int i = 0; i < ITERATIONS; i++)
        {
            runTest();
        }
    }

    private void runTest() throws InterruptedException
    {
        final String channel =
            "aeron:ipc?alias=concurrency|term-length=64K|init-term-id=11|term-id=16|term-offset=48896|mtu=8192";
        final int streamId = 555555;

        try (ConcurrentPublication publication = aeron.addPublication(channel, streamId);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(publication);
            Tests.awaitConnected(subscription);

            final CountDownLatch startLatch = new CountDownLatch(NUM_PUBLISHERS + 1);
            final AtomicReference<Throwable> errors = new AtomicReference<>();
            final LongHashSet threadIds = new LongHashSet();
            final ArrayList<MessagePublisher> publishers = new ArrayList<>();
            for (int i = 0; i < NUM_PUBLISHERS; i++)
            {
                final MessagePublisher publisher;
                if ((i & 1) == 0)
                {
                    publisher = new OfferMessagePublisher(
                        publication,
                        8160,
                        "offer-" + i,
                        startLatch,
                        errors);
                }
                else
                {
                    publisher = new TryClaimMessagePublisher(
                        publication,
                        7777,
                        "try-claim",
                        startLatch,
                        errors);
                }
                publishers.add(publisher);
                threadIds.add(publisher.getId());
                publisher.start();
            }

            startLatch.countDown();
            startLatch.await();

            final MutableInteger msgCount = new MutableInteger();
            final FragmentAssembler fragmentHandler = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    final long threadId = buffer.getLong(offset, LITTLE_ENDIAN);
                    assertTrue(threadIds.contains(threadId));
                    msgCount.increment();
                });
            final Supplier<String> errorMessageSupplier = () -> "missing messages: expected=" + NUM_MESSAGES +
                ", sent=" + publishers.stream().mapToLong(p -> p.sendCount).sum() +
                ", received=" + msgCount;
            while (msgCount.get() < NUM_MESSAGES)
            {
                if (0 == subscription.poll(fragmentHandler, Integer.MAX_VALUE))
                {
                    final Throwable err = errors.get();
                    if (null != err)
                    {
                        LangUtil.rethrowUnchecked(err);
                    }
                    Tests.yieldingIdle(errorMessageSupplier);
                }
            }

            final Throwable err = errors.get();
            if (null != err)
            {
                LangUtil.rethrowUnchecked(err);
            }
        }
    }

    private abstract static class MessagePublisher extends Thread
    {
        private final CountDownLatch startLatch;
        private final ConcurrentPublication publication;
        private final int messageSize;
        private final AtomicReference<Throwable> errors;
        long sendCount;

        MessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            this.publication = publication;
            this.messageSize = messageSize;
            this.startLatch = startLatch;
            this.errors = errors;
            setName(name);
            setDaemon(true);
        }

        public void run()
        {
            startLatch.countDown();
            try
            {
                startLatch.await();

                final long threadId = getId();
                final int numMessages = NUM_MESSAGES / NUM_PUBLISHERS;
                for (int i = 0; i < numMessages; i++)
                {
                    long result;
                    while ((result = publish(publication, threadId, messageSize)) < 0)
                    {
                        if (Publication.CLOSED == result ||
                            Publication.MAX_POSITION_EXCEEDED == result ||
                            Publication.NOT_CONNECTED == result)
                        {
                            fail("failed to publish: " + result);
                        }
                        Tests.yield();
                    }

                    sendCount++;
                }
            }
            catch (final Throwable e)
            {
                if (!errors.compareAndSet(null, e))
                {
                    errors.get().addSuppressed(e);
                }
            }
        }

        abstract long publish(ConcurrentPublication publication, long payload, int size);
    }

    private static class OfferMessagePublisher extends MessagePublisher
    {
        private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer(1024);

        OfferMessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            super(publication, messageSize, name, startLatch, errors);
        }

        long publish(final ConcurrentPublication publication, final long payload, final int size)
        {
            msgBuffer.checkLimit(size);
            msgBuffer.putLong(0, payload, LITTLE_ENDIAN);
            return publication.offer(msgBuffer, 0, size);
        }
    }

    private static class TryClaimMessagePublisher extends MessagePublisher
    {
        private final BufferClaim bufferClaim = new BufferClaim();

        TryClaimMessagePublisher(
            final ConcurrentPublication publication,
            final int messageSize,
            final String name,
            final CountDownLatch startLatch,
            final AtomicReference<Throwable> errors)
        {
            super(publication, messageSize, name, startLatch, errors);
        }

        long publish(final ConcurrentPublication publication, final long payload, final int size)
        {
            final long result = publication.tryClaim(size, bufferClaim);
            if (result > 0)
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
                buffer.putLong(offset, payload, LITTLE_ENDIAN);
                bufferClaim.commit();
            }

            return result;
        }
    }
}
