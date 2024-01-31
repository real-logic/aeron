/*
 * Copyright 2014-2024 Real Logic Limited.
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
import io.aeron.driver.StaticDelayGenerator;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.reports.LossReportReader;
import io.aeron.driver.reports.LossReportUtil;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataLossAndRecoverySystemTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .unicastFeedbackDelayGenerator(new StaticDelayGenerator(TimeUnit.MICROSECONDS.toNanos(100), false))
            .threadingMode(ThreadingMode.SHARED);
        TestMediaDriver.enableFixedLoss(context, 5, 102, 100_000);

        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    @Disabled
    void shouldSendStreamOfDataAndHandleLargeGap() throws IOException
    {
        final String channel =
            "aeron:udp?endpoint=localhost:10000|term-length=1m|init-term-id=0|term-id=0|term-offset=0";
        final int streamId = 10000;
        final byte[] input = new byte[10 * 1024 * 1024];
        final byte[] output = new byte[10 * 1024 * 1024];
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

        assertEquals(output.length, outputPosition.get());
        assertArrayEquals(input, output);

        printLoss(driver.aeronDirectoryName(), streamId);
    }

    static void printLoss(final String aeronDirectoryName, final int streamId) throws IOException
    {
        final File lossReportFile = LossReportUtil.file(aeronDirectoryName);
        assertTrue(lossReportFile.exists());

        MappedByteBuffer mappedByteBuffer = null;

        try (RandomAccessFile file = new RandomAccessFile(lossReportFile, "r");
            FileChannel channel = file.getChannel())
        {
            mappedByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final LossReportReader.EntryConsumer lossEntryConsumer = new LossReportReader.EntryConsumer()
            {
                public void accept(
                    final long observationCount,
                    final long totalBytesLost,
                    final long firstObservationTimestamp,
                    final long lastObservationTimestamp,
                    final int sessionId,
                    final int streamId,
                    final String channel,
                    final String source)
                {
                    System.out.println(
                        "observationCount = " + observationCount + ", totalBytesLost = " + totalBytesLost +
                        ", firstObservationTimestamp = " + firstObservationTimestamp +
                        ", lastObservationTimestamp = " + lastObservationTimestamp +
                        ", sessionId = " + sessionId + ", streamId = " + streamId + ", channel = " + channel +
                        ", source = " + source);
                }
            };

            LossReportReader.read(buffer, lossEntryConsumer);
        }
        finally
        {
            BufferUtil.free(mappedByteBuffer);
        }
    }
}
