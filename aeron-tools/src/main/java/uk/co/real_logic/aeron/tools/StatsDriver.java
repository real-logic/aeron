/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.*;
import uk.co.real_logic.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;

public class StatsDriver
{
    private Options options;
    private StatsOutput output = null;
    private Stats stats = null;
    private String file = null;
    private AtomicBoolean running = null;

    public StatsDriver(final String[] args)
    {
        try
        {
            parseArgs(args);
            running = new AtomicBoolean(true);
            stats = new Stats(output);

            final Runnable task = new Runnable()
            {
                public void run()
                {
                    try
                    {
                        while (running.get())
                        {
                            stats.collectStats();
                            Thread.sleep(1000);
                        }
                        stats.close();
                    }
                    catch (final Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            };
            final Thread worker = new Thread(task);
            worker.start();

            SigInt.register(() -> running.set(false));
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    public void parseArgs(final String[] args) throws ParseException
    {
        options = new Options();
        options.addOption(null, "vmstat", false, "Format transport stats in vmstat format.");
        options.addOption(null, "console", false, "Dump raw stats to the console.");
        options.addOption(null, "netstat", false, "Format channel info in netstat format.");
        options.addOption(null, "csv", false, "Format transport stats as comma separated values.");
        options.addOption(null, "file", true, "Output file for csv format stats.");
        options.addOption("h", "help", false, "Display help message.");


        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);

        final String opt;

        if (command.hasOption("help"))
        {
            System.out.println(options.toString());
            System.exit(0);
        }

        /** Default is console output **/
        output = new StatsConsoleOutput();

        if (command.hasOption("vmstat"))
        {
            output = new StatsVmStatOutput();
        }
        else if (command.hasOption("console"))
        {
            output = new StatsConsoleOutput();
        }
        else if (command.hasOption("netstat"))
        {
            output = new StatsNetstatOutput();
        }
        else if (command.hasOption("csv"))
        {
            if (command.hasOption("file"))
            {
                file = command.getOptionValue("file", null);
            }
            output = new StatsCsvOutput(file);
        }
    }

    public static void main(final String[] args)
    {
        new StatsDriver(args);
    }
}
