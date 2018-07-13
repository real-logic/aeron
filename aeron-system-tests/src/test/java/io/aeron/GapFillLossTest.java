/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import io.aeron.driver.*;
import io.aeron.driver.ext.*;
import io.aeron.driver.reports.LossReport;
import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.*;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GapFillLossTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:54325";
    private static final String UNRELIABLE_CHANNEL =
        CHANNEL + "|" + CommonContext.RELIABLE_STREAM_PARAM_NAME + "=false";

    private static final int STREAM_ID = 1;
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    private static final int MSG_LENGTH = 1024;
    private static final int TERM_BUFFER_LENGTH = 1024 * 64;
    private static final int NUM_MESSAGES = 10_000;

    private static final AtomicLong FINAL_POSITION = new AtomicLong(Long.MAX_VALUE);

    @Test(timeout = 10_000)
    public void shouldGapFillWhenLossOccurs() throws Exception
    {
        final UnsafeBuffer srcBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MSG_LENGTH));
        srcBuffer.setMemory(0, MSG_LENGTH, (byte)7);

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH);

        final LossReport lossReport = mock(LossReport.class);
        ctx.lossReport(lossReport);

        final LossGenerator dataLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0.20, 0xcafebabeL);
        final LossGenerator noLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        ctx.sendChannelEndpointSupplier((udpChannel, statusIndicator, context) -> new DebugSendChannelEndpoint(
            udpChannel, statusIndicator, context, noLossGenerator, noLossGenerator));

        ctx.receiveChannelEndpointSupplier(
            (udpChannel, dispatcher, statusIndicator, context) -> new DebugReceiveChannelEndpoint(
            udpChannel, dispatcher, statusIndicator, context, dataLossGenerator, noLossGenerator));

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            Subscription subscription = aeron.addSubscription(UNRELIABLE_CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            final Subscriber subscriber = new Subscriber(subscription);
            final Thread subscriberThread = new Thread(subscriber);
            subscriberThread.setDaemon(true);
            subscriberThread.start();

            long position = 0;
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
                srcBuffer.putLong(0, i);

                while ((position = publication.offer(srcBuffer)) < 0L)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }

            FINAL_POSITION.set(position);
            subscriberThread.join();

            assertThat(subscriber.messageCount, lessThan(NUM_MESSAGES));
            verify(lossReport).createEntry(anyLong(), anyLong(), anyInt(), eq(STREAM_ID), anyString(), anyString());
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }

    static class Subscriber implements Runnable, FragmentHandler
    {
        private final Subscription subscription;
        int messageCount = 0;

        Subscriber(final Subscription subscription)
        {
            this.subscription = subscription;
        }

        public void run()
        {
            while (!subscription.isConnected())
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final Image image = subscription.imageAtIndex(0);

            while (image.position() < FINAL_POSITION.get())
            {
                final int fragments = subscription.poll(this, FRAGMENT_COUNT_LIMIT);
                if (0 == fragments)
                {
                    SystemTest.checkInterruptedStatus();
                    if (subscription.isClosed())
                    {
                        return;
                    }
                }
                Thread.yield();
            }
        }

        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageCount++;
        }
    }
}
