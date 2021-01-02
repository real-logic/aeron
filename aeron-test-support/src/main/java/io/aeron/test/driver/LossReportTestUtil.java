/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test.driver;

import io.aeron.driver.reports.LossReportReader;
import io.aeron.driver.reports.LossReportUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LossReportTestUtil
{
    public static void verifyLossOccurredForStream(final String aeronDirectoryName, final int streamId)
        throws IOException
    {
        final File lossReportFile = LossReportUtil.file(aeronDirectoryName);
        assertTrue(lossReportFile.exists());

        MappedByteBuffer mappedByteBuffer = null;

        try (RandomAccessFile file = new RandomAccessFile(lossReportFile, "r");
            FileChannel channel = file.getChannel())
        {
            mappedByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final LossReportReader.EntryConsumer lossEntryConsumer = mock(LossReportReader.EntryConsumer.class);
            LossReportReader.read(buffer, lossEntryConsumer);

            verify(lossEntryConsumer).accept(
                longThat((l) -> l > 0),
                longThat((l) -> l > 0),
                anyLong(),
                anyLong(),
                anyInt(),
                eq(streamId),
                any(),
                any());
        }
        finally
        {
            IoUtil.unmap(mappedByteBuffer);
        }
    }
}
