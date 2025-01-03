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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MultiGapLossAndRecoverySystemTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        TestMediaDriver.enableMultiGapLoss(context, 0, 4096, 100, 100);
    }

    private void launch(final MediaDriver.Context context)
    {
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    void shouldSendStreamOfDataAndHandleMultipleGaps()
    {
        launch(context);

        sendAndReceive(
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0",
            10 * 1024 * 1024
        );

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final long retransmitCount = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.RETRANSMITS_SENT.id());
            final long nakCount = aeron.countersReader()
                .getCounterValue(SystemCounterDescriptor.NAK_MESSAGES_SENT.id());
            // Prior to the advent of the UnicastRetransmitHandler, we'd end up dropping NAKs when we blew past
            // the max retransmit action limit.  In that case, we'd have to send more than the 100 NAKs required.
            // Now, however, the UnicastRetransmitHandler treats new NAKs as a tacit admission that the previous
            // NAK did its job and the prior gap was filled, so we can immediately handle the new NAK.

            final long expectedCountWithBuffer = 150L;
            assertThat(retransmitCount, lessThanOrEqualTo(expectedCountWithBuffer));
            assertThat(nakCount, lessThanOrEqualTo(expectedCountWithBuffer));
        }
    }

    private void sendAndReceive(final String channel, final int publicationLength)
    {
        final int streamId = 10000;
        final byte[] input = new byte[publicationLength];
        final byte[] output = new byte[publicationLength];
        final Random r = new Random(1);
        r.nextBytes(input);
        final UnsafeBuffer sendBuffer = new UnsafeBuffer();

        int inputPosition = 0;
        final MutableInteger outputPosition = new MutableInteger(0);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ExclusivePublication pub = aeron.addExclusivePublication(channel, streamId);
            Subscription sub = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            final FragmentAssembler handler = new FragmentAssembler(
                (buffer, offset, length, header) ->
                {
                    buffer.getBytes(offset, output, outputPosition.get(), length);
                    outputPosition.addAndGet(length);
                });

            while (inputPosition < input.length || outputPosition.get() < output.length)
            {
                if (inputPosition < input.length)
                {
                    final int length = Math.min(input.length - inputPosition, pub.maxMessageLength());
                    sendBuffer.wrap(input, inputPosition, length);
                    if (0 < pub.offer(sendBuffer))
                    {
                        inputPosition += length;
                    }
                }

                if (outputPosition.get() < output.length)
                {
                    sub.poll(handler, 10);
                }
            }
        }

        assertArrayEquals(input, output);
    }
}
