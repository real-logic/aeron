package io.aeron.test;

import io.aeron.driver.reports.LossReportReader;
import io.aeron.driver.reports.LossReportUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LossReportTestUtil
{
    public static void verifyLossOccurredForStream(
        final String aeronDirectoryName,
        final int streamId) throws IOException
    {
        final File lossReportFile = LossReportUtil.file(aeronDirectoryName);
        assertTrue(lossReportFile.exists());

        try (
            RandomAccessFile file = new RandomAccessFile(lossReportFile, "r");
            FileChannel channel = file.getChannel())
        {
            final MappedByteBuffer mappedByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final LossReportReader.EntryConsumer lossEntryConsumer = mock(LossReportReader.EntryConsumer.class);
            LossReportReader.read(buffer, lossEntryConsumer);

            verify(lossEntryConsumer).accept(
                longThat(l -> l > 0), longThat(l -> l > 0), anyLong(), anyLong(), anyInt(), eq(streamId), any(), any());
        }
    }
}
