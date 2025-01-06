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
package io.aeron.samples;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

/**
 * Application to print out errors recorded in the command-and-control (CnC) file is maintained by media driver in
 * shared memory. This application reads the CnC file and prints the distinct errors. Layout of the CnC file is
 * described in {@link CncFileDescriptor}.
 */
public class ErrorStat
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    private static final String ERROR_FILE_NAME_PROP = "aeron.samples.error.file.name";
    private static final String ERROR_FILE_NAME;

    private static final String ERROR_FILE_OFFSET_PROP = "aeron.samples.error.file.offset";
    private static final int ERROR_FILE_OFFSET;

    static
    {
        ERROR_FILE_NAME = System.getProperty(ERROR_FILE_NAME_PROP);
        ERROR_FILE_OFFSET = Integer.parseInt(System.getProperty(ERROR_FILE_OFFSET_PROP, "0"));
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final MappedByteBuffer errorMmap;
        final Function<MappedByteBuffer, AtomicBuffer> mmapToErrorBuffer;

        if (null != ERROR_FILE_NAME)
        {
            final File errorFile = new File(ERROR_FILE_NAME);
            System.out.println("Error file " + errorFile);
            errorMmap = SamplesUtil.mapExistingFileReadOnly(errorFile);
            mmapToErrorBuffer = (mmap) -> new UnsafeBuffer(
                mmap, ERROR_FILE_OFFSET, errorMmap.capacity() - ERROR_FILE_OFFSET);
        }
        else
        {
            final File cncFile = CommonContext.newDefaultCncFile();
            System.out.println("Command `n Control file " + cncFile);
            errorMmap = SamplesUtil.mapExistingFileReadOnly(cncFile);
            mmapToErrorBuffer = CommonContext::errorLogBuffer;
        }

        try
        {
            final AtomicBuffer buffer = mmapToErrorBuffer.apply(errorMmap);
            final int distinctErrorCount = ErrorLogReader.read(buffer, ErrorStat::accept);
            System.out.println();
            System.out.println(distinctErrorCount + " distinct errors observed.");
        }
        finally
        {
            IoUtil.unmap(errorMmap);
        }
    }

    private static void accept(
        final int observationCount,
        final long firstObservationTimestamp,
        final long lastObservationTimestamp,
        final String encodedException)
    {
        final String fromDate = DATE_FORMAT.format(new Date(firstObservationTimestamp));
        final String toDate = DATE_FORMAT.format(new Date(lastObservationTimestamp));

        System.out.println();
        System.out.println(observationCount + " observations from " + fromDate + " to " + toDate + " for:");
        System.out.println(encodedException);
    }
}
