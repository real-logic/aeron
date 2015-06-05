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

import org.apache.commons.cli.ParseException;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.SigIntBarrier;

import java.util.logging.Logger;

public class MediaDriverTool
{
    private static final Logger LOG = Logger.getLogger(MediaDriverTool.class.getName());

    public static void main(final String[] args)
    {
        final MediaDriverOptions opts = new MediaDriverOptions();
        int printHelp = 1;
        int exitValue = 0;
        try
        {
            printHelp = opts.parseArgs(args);
        }
        catch (final ParseException ex)
        {
            ex.printStackTrace();
            exitValue = -1;
        }

        if (printHelp != 0)
        {
            opts.printHelp("MediaDriverTool");
            System.out.println(USAGE_GUIDE);
            System.exit(exitValue);
        }
        final MediaDriverTool driver = new MediaDriverTool();
        driver.run(opts);
    }

    public void run(final MediaDriverOptions opts)
    {
        if (opts.properties() != null)
        {
            // Set system properties (aeron configuration) from the loaded configuration file.
            System.getProperties().putAll(opts.properties());
        }
        final MediaDriver.Context context = new MediaDriver.Context()
            .conductorIdleStrategy(opts.conductorIdleStrategy())
            .senderIdleStrategy(opts.senderIdleStrategy())
            .receiverIdleStrategy(opts.receiverIdleStrategy())
            .sharedNetworkIdleStrategy(opts.sharedNetworkIdleStrategy())
            .sharedIdleStrategy(opts.sharedIdleStrategy())
            .dataLossGenerator(opts.dataLossGenerator())
            .controlLossGenerator(opts.controlLossGenerator());

        // Everything else can be changed by Aeron settings.
        try (final MediaDriver driver = MediaDriver.launch(context))
        {
            LOG.info("Media Driver Started.");
            new SigIntBarrier().await();
            LOG.info("Media Driver Stopped.");
        }
    }

    private static final String NL = System.lineSeparator();
    private static final String USAGE_GUIDE = "" +
        //                                                                 80 chars -> |
        NL +
        "Use this tool to run an Aeron Media driver with configuration in a properties" + NL +
        "file. Additionally, the idle strategies for all possible threads can be set" + NL +
        "independently via the command line or properties." + NL +
        NL +
        "The Agrona project provides 3 implementations of IdleStrategy:" + NL +
        uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy.class.getName() + NL +
        uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy.class.getName() + NL +
        uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy.class.getName() + NL +
        "You may also implement your own IdleStrategy and pass its class name." + NL +
        NL +
        "It is possible to provide input parameters to the BackoffIdleStrategy" + NL +
        "through the command line. After specifying the BackoffIdleStrategy class," + NL +
        "add (<long>,<long>,<long>,<long>). For Example:" + NL +
        uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy.class.getName() + "(1,1,1,1)" + NL +
        NL +
        "Set the default idle strategies for each thread using these properties:" + NL +
        "aeron.tools.mediadriver.sender" + NL +
        "aeron.tools.mediadriver.receiver" + NL +
        "aeron.tools.mediadriver.conductor" + NL +
        "aeron.tools.mediadriver.network" + NL +
        "areon.tools.mediadriver.shared" + NL +
        NL +
        "Set the loss generator classes for data and control using these properties:" + NL +
        "aeron.tools.mediadriver.data.loss" + NL +
        "aeron.tools.mediadriver.control.loss" + NL;
}
