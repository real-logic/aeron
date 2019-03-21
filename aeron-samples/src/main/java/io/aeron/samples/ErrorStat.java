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
package io.aeron.samples;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

import static io.aeron.CncFileDescriptor.CNC_VERSION;

/**
 * Application to print out errors recorded in the command-and-control (cnc) file is maintained by media driver in
 * shared memory. This application reads the the cnc file and prints the distinct errors. Layout of the cnc file is
 * described in {@link CncFileDescriptor}.
 */
public class ErrorStat
{
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    public static void main(final String[] args)
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = SamplesUtil.mapExistingFileReadOnly(cncFile);
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        if (CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException(
                "Aeron CnC version does not match: version=" + cncVersion + " required=" + CNC_VERSION);
        }

        final AtomicBuffer buffer = CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
        final int distinctErrorCount = ErrorLogReader.read(buffer, ErrorStat::accept);

        System.out.format("%n%d distinct errors observed.%n", distinctErrorCount);
    }

    private static void accept(
        final int observationCount,
        final long firstObservationTimestamp,
        final long lastObservationTimestamp,
        final String encodedException)
    {
        System.out.format(
            "***%n%d observations from %s to %s for:%n %s%n",
            observationCount,
            DATE_FORMAT.format(new Date(firstObservationTimestamp)),
            DATE_FORMAT.format(new Date(lastObservationTimestamp)),
            encodedException);
    }
}
