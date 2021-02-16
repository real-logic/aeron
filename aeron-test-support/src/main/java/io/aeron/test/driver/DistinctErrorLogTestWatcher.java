/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test.driver;

import io.aeron.CommonContext;
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
        for (int i = 0, size = errors.size(); i < size; i++)
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
        final int observationCount,
        final long firstObservationTimestamp,
        final long lastObservationTimestamp,
        final String encodedException)
    {
        observationValues.addLong(observationCount);
        observationValues.addLong(firstObservationTimestamp);
        observationValues.addLong(lastObservationTimestamp);
        errors.add(encodedException);
    }

    public void captureErrors(final String aeronDirectoryName)
    {
        final File cncFile = CommonContext.newCncFile(aeronDirectoryName);
        assertTrue(cncFile.exists());

        MappedByteBuffer cncByteBuffer = null;

        try (RandomAccessFile file = new RandomAccessFile(cncFile, "r");
            FileChannel channel = file.getChannel())
        {
            cncByteBuffer = channel.map(READ_ONLY, 0, channel.size());
            final AtomicBuffer errorLogBuffer = CommonContext.errorLogBuffer(cncByteBuffer);

            ErrorLogReader.read(errorLogBuffer, this::onObservation);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            IoUtil.unmap(cncByteBuffer);
        }
    }
}
