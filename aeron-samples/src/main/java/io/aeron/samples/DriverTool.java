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
import org.agrona.IoUtil;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.Date;

import static io.aeron.CncFileDescriptor.*;

/**
 * Tool for printing out Aeron Media Driver Information. A command-and-control (CnC) file is maintained by media driver
 * in shared memory. This application reads the CnC file and prints its status. Layout of the Cnc file is
 * described in {@link CncFileDescriptor}.
 */
public class DriverTool
{
    public static void main(final String[] args)
    {
        boolean printPidOnly = false;

        if (0 != args.length)
        {
            checkForHelp(args);

            if (args[0].equals("pid"))
            {
                printPidOnly = true;
            }
        }

        final File cncFile = CommonContext.newDefaultCncFile();
        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException(
                "Aeron CnC version does not match: version=" + cncVersion + " required=" + CNC_VERSION);
        }

        final ManyToOneRingBuffer toDriver = new ManyToOneRingBuffer(createToDriverBuffer(cncByteBuffer, cncMetaData));

        if (printPidOnly)
        {
            System.out.println(pid(cncMetaData));
        }
        else
        {
            System.out.println("Command `n Control file: " + cncFile);
            System.out.format("Version: %d, PID: %d%n", cncVersion, pid(cncMetaData));
            printDateActivityAndStartTimestamps(startTimestamp(cncMetaData), toDriver.consumerHeartbeatTime());
        }
    }

    private static void printDateActivityAndStartTimestamps(final long startTimestamp, final long activityTimestamp)
    {
        System.out.format(
            "%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(startTimestamp),
            new Date(activityTimestamp));
    }

    private static void checkForHelp(final String[] args)
    {
        for (final String arg : args)
        {
            if ("-?".equals(arg) || "-h".equals(arg) || "-help".equals(arg))
            {
                System.out.println("\"Usage: [-Daeron.dir=<directory containing CnC file>] DriverTool <pid>");
                System.out.println("  pid: prints PID of driver only.");
                System.exit(0);
            }
        }
    }
}
