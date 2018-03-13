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

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.RecordingLog;

import java.io.File;
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
            printHelp();
            System.exit(-1);
        }

        final File clusterDir = new File(args[0]);
        if (!clusterDir.exists())
        {
            System.err.println("ERR: cluster folder not found: " + clusterDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        switch (args[1])
        {
            case "describe":
                try (ClusterMarkFile markFile = openMarkFile(clusterDir, System.out::println))
                {
                    printTypeAndActivityTimestamp(markFile);
                    System.out.println(markFile.decoder());
                }
                break;

            case "pid":
                try (ClusterMarkFile markFile = openMarkFile(clusterDir, null))
                {
                    System.out.println(markFile.decoder().pid());
                }
                break;

            case "recovery-plan":
                try (AeronArchive archive = AeronArchive.connect())
                {
                    final RecordingLog recordingLog = new RecordingLog(clusterDir);
                    System.out.println(recordingLog.createRecoveryPlan(archive));
                }
                break;

            case "recording-log":
                final RecordingLog recordingLog = new RecordingLog(clusterDir);
                System.out.println(recordingLog.toString());
                break;
        }
    }

    private static ClusterMarkFile openMarkFile(final File clusterDir, final Consumer<String> logger)
    {
        return new ClusterMarkFile(clusterDir, ClusterMarkFile.FILENAME, System::currentTimeMillis, TIMEOUT_MS, logger);
    }

    private static void printTypeAndActivityTimestamp(final ClusterMarkFile markFile)
    {
        System.out.print("Type: " + markFile.decoder().componentType());
        System.out.format(
            "%1$tH:%1$tM:%1$tS (start: %2tF %2$tH:%2$tM:%2$tS, activity: %3tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(markFile.decoder().startTimestamp()),
            new Date(markFile.activityTimestampVolatile()));
    }

    private static void printHelp()
    {
        System.out.println("Usage: <cluster-dir> <command>");
        System.out.println("  describe: prints out all descriptors in the file.");
        System.out.println("  pid: prints PID of cluster component.");
        System.out.println("  recovery-plan: prints recovery plan of cluster component.");
        System.out.println("  recording-log: prints recording log of cluster component.");
    }
}
