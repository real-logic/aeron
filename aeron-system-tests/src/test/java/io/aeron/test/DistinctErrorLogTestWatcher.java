package io.aeron.test;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.reports.ErrorReportUtil;
import org.agrona.IoUtil;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistinctErrorLogTestWatcher implements TestWatcher
{
    private final LongArrayList observationValues = new LongArrayList();
    private final List<String> errors = new ArrayList<>();

    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        if (observationValues.size() != errors.size() * 3)
        {
            throw new IllegalStateException();
        }

        System.out.println("Errors from distinct error log:");
        for (int i = 0, n = errors.size(); i < n; i++)
        {
            final int observationValuesIndex = i * 3;

            System.out.print("Observation count: ");
            System.out.print(observationValues.getLong(observationValuesIndex));
            System.out.print(", first timestamp: ");
            System.out.print(observationValues.getLong(observationValuesIndex + 1));
            System.out.print(", last timestamp: ");
            System.out.print(observationValues.getLong(observationValuesIndex + 2));
            System.out.println();
            System.out.println(errors.get(i));
            System.out.println();
        }
    }

    private void onObservation(
        int observationCount, long firstObservationTimestamp, long lastObservationTimestamp, String encodedException)
    {
        observationValues.addLong(observationCount);
        observationValues.addLong(firstObservationTimestamp);
        observationValues.addLong(lastObservationTimestamp);
        errors.add(encodedException);
    }

    public void captureErrors(String aeronDirectoryName)
    {
        final File cncFile = CommonContext.newCncFile(aeronDirectoryName);
        assertTrue(cncFile.exists());

        MappedByteBuffer cncByteBuffer = null;

        try (
            RandomAccessFile file = new RandomAccessFile(cncFile, "r");
            FileChannel channel = file.getChannel())
        {
            cncByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer errorLogBuffer = ErrorReportUtil.mapErrorLogBuffer(cncByteBuffer);

            ErrorLogReader.read(errorLogBuffer, this::onObservation);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            IoUtil.unmap(cncByteBuffer);
        }
    }
}
