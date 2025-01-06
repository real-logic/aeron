/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.AeronCounters.DRIVER_RECEIVER_HWM_TYPE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class MultiSubscriberTest
{
    private static final String CHANNEL_1 = "aeron:udp?endpoint=localhost:24325|fruit=banana";
    private static final String CHANNEL_2 = "aeron:udp?endpoint=localhost:24325|fruit=apple";
    private static final String CHANNEL_3 = "aeron:udp?endpoint=localhost:24326|fruit=apple";
    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .threadingMode(ThreadingMode.SHARED);
    private final TestMediaDriver driver = TestMediaDriver.launch(context, watcher);

    private final Aeron aeron = Aeron.connect();

    @BeforeEach
    void setUp()
    {
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldReceiveMessageOnSeparateSubscriptions()
    {
        final FragmentHandler mockFragmentHandlerOne = mock(FragmentHandler.class);
        final FragmentHandler mockFragmentHandlerTwo = mock(FragmentHandler.class);

        final FragmentAssembler adapterOne = new FragmentAssembler(mockFragmentHandlerOne);
        final FragmentAssembler adapterTwo = new FragmentAssembler(mockFragmentHandlerTwo);

        try (Subscription subscriptionOne = aeron.addSubscription(CHANNEL_1, STREAM_ID);
            Subscription subscriptionTwo = aeron.addSubscription(CHANNEL_2, STREAM_ID);
            Publication publication = aeron.addPublication(CHANNEL_1, STREAM_ID))
        {
            final byte[] expectedBytes = "Hello, World! here is a small message".getBytes();
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(expectedBytes);

            assertEquals(0, subscriptionOne.poll(adapterOne, FRAGMENT_COUNT_LIMIT));
            assertEquals(0, subscriptionTwo.poll(adapterTwo, FRAGMENT_COUNT_LIMIT));

            while (!subscriptionOne.isConnected() || !subscriptionTwo.isConnected())
            {
                Tests.yield();
            }

            while (publication.offer(srcBuffer) < 0L)
            {
                Tests.yield();
            }

            while (subscriptionOne.poll(adapterOne, FRAGMENT_COUNT_LIMIT) == 0)
            {
                Tests.yield();
            }

            while (subscriptionTwo.poll(adapterTwo, FRAGMENT_COUNT_LIMIT) == 0)
            {
                Tests.yield();
            }

            verifyData(srcBuffer, mockFragmentHandlerOne);
            verifyData(srcBuffer, mockFragmentHandlerTwo);
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldReportDifferentUriForEachSubscription()
    {
        final AtomicLong subAImageCorrelationId = new AtomicLong(-1);
        final AtomicLong subBImageCorrelationId = new AtomicLong(-1);
        final String channel = "aeron:udp?endpoint=localhost:24325";
        final String channelA = channel + "|reliable=false";
        final String channelB = channel + "|reliable=true";

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication publication1 = aeron.addPublication(channel, STREAM_ID);
            Publication publication2 = aeron.addPublication(channel, STREAM_ID + 1);
            Subscription subA = aeron.addSubscription(
                channelA,
                STREAM_ID,
                image -> subAImageCorrelationId.set(image.correlationId()),
                image -> {});
            Subscription subB = aeron.addSubscription(
                channelB,
                STREAM_ID + 1,
                image -> subBImageCorrelationId.set(image.correlationId()),
                image -> {}))
        {
            Tests.awaitConnected(publication1);
            Tests.awaitConnected(publication2);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            while (-1 == subAImageCorrelationId.get() && -1 == subBImageCorrelationId.get())
            {
                Tests.yield();
            }

            int counterIdA;
            while (-1 == (counterIdA = aeron.countersReader()
                .findByTypeIdAndRegistrationId(DRIVER_RECEIVER_HWM_TYPE_ID, subAImageCorrelationId.get())))
            {
                Tests.yield();
            }
            assertThat(aeron.countersReader().getCounterLabel(counterIdA), containsString(channelA));

            int counterIdB;
            while (-1 == (counterIdB = aeron.countersReader()
                .findByTypeIdAndRegistrationId(DRIVER_RECEIVER_HWM_TYPE_ID, subBImageCorrelationId.get())))
            {
                Tests.yield();
            }
            assertThat(aeron.countersReader().getCounterLabel(counterIdB), containsString(channelB));
        }
    }

    private void verifyData(final UnsafeBuffer srcBuffer, final FragmentHandler mockFragmentHandler)
    {
        final ArgumentCaptor<DirectBuffer> bufferArg = ArgumentCaptor.forClass(DirectBuffer.class);
        final ArgumentCaptor<Integer> offsetArg = ArgumentCaptor.forClass(Integer.class);

        verify(mockFragmentHandler, times(1)).onFragment(
            bufferArg.capture(), offsetArg.capture(), eq(srcBuffer.capacity()), any(Header.class));

        final DirectBuffer capturedBuffer = bufferArg.getValue();
        final int offset = offsetArg.getValue();
        for (int i = 0; i < srcBuffer.capacity(); i++)
        {
            final int index = offset + i;
            assertEquals(srcBuffer.getByte(i), capturedBuffer.getByte(index), "same at " + index);
        }
    }
}
