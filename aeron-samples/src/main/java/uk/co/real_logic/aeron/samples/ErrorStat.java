/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.samples;

import uk.co.real_logic.aeron.CncFileDescriptor;
import uk.co.real_logic.aeron.CommonContext;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.errors.ErrorLogReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Application to print out errors recorded in the command-and-control (cnc) file is maintained by media driver in shared
 * memory. This application reads the the cnc file and prints the distinct errors. Layout of the cnc file is described in
 * {@link CncFileDescriptor}.
 */
public class ErrorStat
{
    public static void main(final String[] args) throws Exception
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException("CNC version not supported: file version=" + cncVersion);
        }

        final AtomicBuffer buffer = CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, metaDataBuffer);
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

        final int distinctErrorCount = ErrorLogReader.read(
            buffer,
            (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                System.out.format(
                    "%d observations from %s to %s for:\n %s\n",
                    observationCount,
                    dateFormat.format(new Date(firstObservationTimestamp)),
                    dateFormat.format(new Date(lastObservationTimestamp)),
                    encodedException
                ));

        System.out.format("\n%d distinct errors observed.\n", distinctErrorCount);
    }
}
