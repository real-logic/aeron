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

import io.aeron.cluster.codecs.cnc.ClusterComponentType;
import io.aeron.cluster.codecs.cnc.CncHeaderDecoder;

import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ClusterTool
{
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private static File clusterDir;

    public static void main(final String[] args)
    {
        if (args.length == 0 || args.length > 3)
        {
            printHelp();
            System.exit(-1);
        }

        clusterDir = new File(args[0]);
        if (!clusterDir.exists())
        {
            System.err.println("ERR: cluster folder not found: " + clusterDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        if (args.length == 2 && args[1].equals("describe"))
        {
            try (ClusterCncFile cncFile = openCncFile(System.out::println))
            {
                final CncHeaderDecoder decoder = cncFile.decoder();

                printTypeAndActivityTimestamp(decoder.fileType(), decoder.activityTimestamp());
                System.out.println(decoder);
            }
        }
        else if (args.length == 2 && args[1].equals("pid"))
        {
            try (ClusterCncFile cncFile = openCncFile(null))
            {
                System.out.println(cncFile.decoder().pid());
            }
        }
    }

    private static ClusterCncFile openCncFile(final Consumer<String> logger)
    {
        return new ClusterCncFile(clusterDir, ClusterCncFile.FILENAME, System::currentTimeMillis, TIMEOUT_MS, logger);
    }

    private static void printTypeAndActivityTimestamp(final ClusterComponentType type, final long activityTimestamp)
    {
        System.out.print("Type: " + type);
        System.out.format(
            " %1$tH:%1$tM:%1$tS (activity: %2$tH:%2$tM:%2$tS)%n", new Date(), new Date(activityTimestamp));
    }

    private static void printHelp()
    {
        System.out.println("Usage: <cluster-dir> <command> <optional args>");
        System.out.println("  describe: prints out all descriptors in the file. Optionally specify a recording id" +
            " to describe a single recording.");
        System.out.println("  pid: prints PID of cluster component.");
    }
}
