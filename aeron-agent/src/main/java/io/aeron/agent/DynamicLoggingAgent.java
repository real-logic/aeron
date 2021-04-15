/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.agent;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.PropertyAction;
import org.agrona.Strings;
import org.agrona.SystemUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumMap;

import static io.aeron.agent.ConfigOption.*;

/**
 * Attach/detach logging agent dynamically to a running process.
 */
public class DynamicLoggingAgent
{
    /**
     * Attach logging agent jar to a running process.
     *
     * @param args program arguments.
     */
    public static void main(final String[] args)
    {
        if (args.length < 3)
        {
            printHelp();
            System.exit(-1);
        }

        final File agentJar = Paths.get(args[0]).toAbsolutePath().toFile();
        if (!agentJar.exists())
        {
            throw new IllegalArgumentException(agentJar + " does not exist!");
        }

        final String processId = args[1];
        if (Strings.isEmpty(processId))
        {
            throw new IllegalArgumentException("no processId provided!");
        }

        final String command = args[2];
        for (int i = 3; i < args.length; i++)
        {
            SystemUtil.loadPropertiesFile(PropertyAction.PRESERVE, args[i]);
        }

        switch (command)
        {
            case START_COMMAND:
            {
                final EnumMap<ConfigOption, String> configOptions = fromSystemProperties();
                String logFile = configOptions.get(LOG_FILENAME);
                if (Strings.isEmpty(logFile))
                {
                    final String baseDir;
                    if (SystemUtil.isLinux() && Files.exists(Paths.get("/dev/shm")))
                    {
                        baseDir = "/dev/shm/";
                    }
                    else
                    {
                        baseDir = SystemUtil.tmpDirName();
                    }

                    logFile = baseDir + "agent-log-pid-" + processId + ".log";
                    configOptions.put(LOG_FILENAME, logFile);
                }

                if (Strings.isEmpty(configOptions.get(READER_CLASSNAME)))
                {
                    configOptions.put(READER_CLASSNAME, READER_CLASSNAME_DEFAULT);
                }

                final String agentArgs = buildAgentArgs(configOptions);
                ByteBuddyAgent.attach(agentJar, processId, agentArgs);
                break;
            }

            case STOP_COMMAND:
            {
                ByteBuddyAgent.attach(agentJar, processId, command);
                break;
            }

            default:
                throw new IllegalArgumentException("invalid command: " + command);
        }
    }

    private static void printHelp()
    {
        System.out.println("Usage: <agent-jar> <java-process-id> <command> [property files...]");
        System.out.println("  <agent-jar> - fully qualified path to the agent jar");
        System.out.println("  <java-process-id> - PID of the Java process to attach an agent to");
        System.out.println("  <command> - either '" + START_COMMAND + "' or '" + STOP_COMMAND + "'");
        System.out
            .println("  [property files...] - an optional list of property files to configure logging options");
    }

}
