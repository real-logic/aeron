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
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class PublicationUnblockTest
{
    private static List<String> channels()
    {
        return asList(
            "aeron:udp?endpoint=localhost:24325",
            "aeron:ipc");
    }

    private static final int STREAM_ID = 1001;
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .clientLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(400))
                .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))
                .publicationUnblockTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500)),
            testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context()
            .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(100)));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @MethodSource("channels")
    @InterruptAfter(10)
    void shouldUnblockNonCommittedMessage(final String channel)
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
                Tests.yield();
            }

            bufferClaim.buffer().setMemory(bufferClaim.offset(), length, (byte)65);
            bufferClaim.commit();

            while (publicationTwo.offer(srcBuffer, 0, length) < 0L)
            {
                Tests.yield();
            }

            while (publicationOne.tryClaim(length, bufferClaim) < 0L)
            {
                Tests.yield();
            }

            while (publicationTwo.offer(srcBuffer, 0, length) < 0L)
            {
                Tests.yield();
            }

            final int expectedFragments = 3;
            int numFragments = 0;
            do
            {
                final int fragments = subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
                if (fragments == 0)
                {
                    Tests.yield();
                }

                numFragments += fragments;
            }
            while (numFragments < expectedFragments);

            assertEquals(expectedFragments, numFragments);
            assertEquals(expectedFragments, fragmentCount.value);
        }
    }
}
