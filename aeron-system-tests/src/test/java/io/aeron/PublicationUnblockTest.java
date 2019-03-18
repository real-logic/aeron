/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(Theories.class)
public class PublicationUnblockTest
{
    @DataPoint
    public static final String NETWORK_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String IPC_CHANNEL = "aeron:ipc";

    private static final int STREAM_ID = 1;
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .threadingMode(ThreadingMode.SHARED)
        .errorHandler(Throwable::printStackTrace)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .clientLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(400))
        .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
        .publicationUnblockTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500)));

    private final Aeron aeron = Aeron.connect(new Aeron.Context()
        .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(100)));

    @After
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(driver);
        driver.context().deleteAeronDirectory();
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldUnblockNonCommittedMessage(final String channel)
    {
        final MutableInteger fragmentCount = new MutableInteger();
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> fragmentCount.value++;

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
            Publication publicationOne = aeron.addPublication(channel, STREAM_ID);
            Publication publicationTwo = aeron.addPublication(channel, STREAM_ID))
        {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[driver.context().mtuLength()]);
            final int length = 128;
            srcBuffer.setMemory(0, length, (byte)66);
            final BufferClaim bufferClaim = new BufferClaim();

            while (publicationOne.tryClaim(length, bufferClaim) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            bufferClaim.buffer().setMemory(bufferClaim.offset(), length, (byte)65);
            bufferClaim.commit();

            while (publicationTwo.offer(srcBuffer, 0, length) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            while (publicationOne.tryClaim(length, bufferClaim) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            while (publicationTwo.offer(srcBuffer, 0, length) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final int expectedFragments = 3;
            int numFragments = 0;
            do
            {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (fragments == 0)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }

                numFragments += fragments;
            }
            while (numFragments < expectedFragments);

            assertThat(numFragments, is(expectedFragments));
            assertThat(fragmentCount.value, is(expectedFragments));
        }
    }
}
