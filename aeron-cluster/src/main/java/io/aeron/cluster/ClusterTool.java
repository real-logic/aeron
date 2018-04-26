/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.CncFileDescriptor;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.RecordingLog;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ClusterTool
{
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    public static void main(final String[] args)
    {
        if (args.length != 2)
        {
            printHelp(System.out);
            System.exit(-1);
        }

        final File clusterDir = new File(args[0]);
        if (!clusterDir.exists())
        {
            System.err.println("ERR: cluster folder not found: " + clusterDir.getAbsolutePath());
            printHelp(System.out);
            System.exit(-1);
        }

        switch (args[1])
        {
            case "describe":
                describe(System.out, clusterDir);
                break;

            case "pid":
                pid(System.out, clusterDir);
                break;

            case "recovery-plan":
                recoveryPlan(System.out, clusterDir);
                break;

            case "recording-log":
                recordingLog(System.out, clusterDir);
                break;

            case "errors":
                errors(System.out, clusterDir);
                break;
        }
    }

    public static void describe(final PrintStream stream, final File clusterDir)
    {
        try (ClusterMarkFile markFile = openMarkFile(clusterDir, stream::println))
        {
            printTypeAndActivityTimestamp(stream, markFile);
            stream.println(markFile.decoder());
        }
    }

    public static void pid(final PrintStream stream, final File clusterDir)
    {
        try (ClusterMarkFile markFile = openMarkFile(clusterDir, null))
        {
            stream.println(markFile.decoder().pid());
        }
    }

    public static void recoveryPlan(final PrintStream stream, final File clusterDir)
    {
        try (AeronArchive archive = AeronArchive.connect())
        {
            final RecordingLog recordingLog = new RecordingLog(clusterDir);
            stream.println(recordingLog.createRecoveryPlan(archive));
        }
    }

    public static void recordingLog(final PrintStream stream, final File clusterDir)
    {
        final RecordingLog recordingLog = new RecordingLog(clusterDir);
        stream.println(recordingLog.toString());
    }

    public static void errors(final PrintStream stream, final File clusterDir)
    {
        try (ClusterMarkFile markFile = openMarkFile(clusterDir, System.out::println))
        {
            printTypeAndActivityTimestamp(stream, markFile);
            printErrors(stream, markFile);
        }
    }

    private static ClusterMarkFile openMarkFile(final File clusterDir, final Consumer<String> logger)
    {
        return new ClusterMarkFile(clusterDir, ClusterMarkFile.FILENAME, System::currentTimeMillis, TIMEOUT_MS, logger);
    }

    private static void printTypeAndActivityTimestamp(final PrintStream stream, final ClusterMarkFile markFile)
    {
        stream.print("Type: " + markFile.decoder().componentType() + " ");
        stream.format(
            "%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
    }

    private static void printErrors(final PrintStream stream, final ClusterMarkFile markFile)
    {
        stream.println("Cluster component error log:");
        ClusterMarkFile.saveErrorLog(stream, markFile.errorBuffer());

        final String aeronDirectory = markFile.decoder().aeronDirectory();
        stream.println("Aeron driver error log (directory: " + aeronDirectory + "):");
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException(
                "Aeron CnC version does not match: version=" + cncVersion +
                    " required=" + CncFileDescriptor.CNC_VERSION);
        }

        final AtomicBuffer buffer = CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
        ClusterMarkFile.saveErrorLog(stream, buffer);
    }

    private static void printHelp(final PrintStream stream)
    {
        stream.println("Usage: <cluster-dir> <command>");
        stream.println("  describe: prints out all descriptors in the file.");
        stream.println("  pid: prints PID of cluster component.");
        stream.println("  recovery-plan: prints recovery plan of cluster component.");
        stream.println("  recording-log: prints recording log of cluster component.");
        stream.println("  errors: prints Aeron and cluster component error logs.");
    }
}
