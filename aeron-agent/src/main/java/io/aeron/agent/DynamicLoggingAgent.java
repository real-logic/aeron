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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumMap;

import static io.aeron.agent.ConfigOption.*;
import static java.lang.System.out;

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
                attachAgent(agentJar, processId, agentArgs);
                out.println("Logging started.");
                out.println("Reader agent: " + configOptions.get(READER_CLASSNAME));
                out.println("File: " + configOptions.get(LOG_FILENAME));
                break;
            }

            case STOP_COMMAND:
            {
                attachAgent(agentJar, processId, command);
                break;
            }

            default:
                throw new IllegalArgumentException("invalid command: " + command);
        }
    }

    private static void printHelp()
    {
        out.println("Usage: <agent-jar> <java-process-id> <command> [property files...]");
        out.println("  <agent-jar> - fully qualified path to the agent jar");
        out.println("  <java-process-id> - PID of the Java process to attach an agent to");
        out.println("  <command> - either '" + START_COMMAND + "' or '" + STOP_COMMAND + "'");
        out.println("  [property files...] - an optional list of property files to configure logging options");
    }

    private static void attachAgent(final File agentJar, final String processId, final String agentArgs)
    {
        try
        {
            ByteBuddyAgent.attach(agentJar, processId, agentArgs);
        }
        catch (final Throwable t)
        {
            translateError(processId, t);
            System.exit(-1);
        }
    }

    private static void translateError(final String processId, final Throwable t)
    {
        Throwable cause = t;
        while (null != cause.getCause())
        {
            cause = cause.getCause();
        }

        if (cause instanceof IOException)
        {
            out.println("Process with PID " + processId + " is not running or not reachable.");
        }
        else if ("AgentInitializationException".equals(cause.getClass().getSimpleName()))
        {
            out.printf(
                "Cannot start logging as it is already running.%n" +
                "To stop logging use a '%s' command.%n%n",
                STOP_COMMAND);
        }
        else
        {
            t.printStackTrace(out);
        }
    }

}
