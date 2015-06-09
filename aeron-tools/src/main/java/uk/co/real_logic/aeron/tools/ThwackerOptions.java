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

public class ThwackerOptions
{
    /** line separator */
    private static final String NL = System.lineSeparator();

    private static final String DEFAULT_VERIFIABLE_MESSAGE_STREAM = "no";
    private static final String DEFAULT_USE_SAME_SID = "no";
    private static final String DEFAULT_USE_CHANNEL_PER_PUB = "no";
    private static final String DEFAULT_USE_EMBEDDED_DRIVER = "yes";
    private static final String DEFAULT_CHANNEL = "udp://localhost";
    private static final String DEFAULT_PORT = "51234";
    private static final String DEFAULT_DURATION = "30000";
    private static final String DEFAULT_ITERATIONS = "1";
    private static final String DEFAULT_SENDERS = "1";
    private static final String DEFAULT_RECEIVERS = "1";
    private static final String DEFAULT_ADDERS = "1";
    private static final String DEFAULT_REMOVERS = "1";
    private static final String DEFAULT_ELEMENTS = "10";
    private static final String DEFAULT_MAX_MSG_SIZE = "35";
    private static final String DEFAULT_MIN_MSG_SIZE = "35";
    private static final String USAGE = "" +
        // stay within column 89 (80 when printed). That's here ---------------------> |
        "Examples:" + NL +
        "-v no -d 10000 -e 10 -s 1 --max-size 100 --min-size 64" + NL +
        "    Run for 10s with unverifiable messages, 10 pubs and 10 subs, 1 sending" + NL +
        "    thread, and random message sizes from 64 to 100 bytes";

    private Options options = null;

    private boolean useVerifiableMessageStream;
    private boolean useSameSID;
    private boolean useChannelPerPub;
    private boolean useEmbeddedDriver;
    private String channel;
    private int port;
    private int duration;
    private int iterations;
    private int senders;
    private int receivers;
    private int adders;
    private int removers;
    private int elements;
    private int maxSize;
    private int minSize;

    public ThwackerOptions()
    {
        options = new Options();
        options.addOption("v", "verify", true, "Enable verifiable message streams [yes/no]");
        options.addOption(null, "same-sid", true, "Enable all Pubs/Subs to use same Stream ID [yes/no]");
        options.addOption(null, "channel-per-pub", true, "Enable all Pubs to use different channels [yes/no]");
        options.addOption(null, "embedded", true, "Enable use of Embedded Driver [yes/no]");
        options.addOption("c", "channel", true, "Set the channel");
        options.addOption("p", "port", true, "Set the port");
        options.addOption("d", "duration", true, "Time duration for \"thwacking\" in milliseconds");
        options.addOption("i", "iterations", true, "Number of iterations of \"thwacking\"");
        options.addOption("s", "senders", true, "Number of sending threads");
        options.addOption("r", "receivers", true, "Number of receiving threads");
        options.addOption(null, "adders", true, "Number of creating threads");
        options.addOption(null, "removers", true, "Number of deleting threads");
        options.addOption("e", "elements", true, "Number of Publications and Subscriptions");
        options.addOption(null, "min-size", true, "Minimum size message a Publication will send");
        options.addOption(null, "max-size", true, "Maximum size message a Publication will send");
        options.addOption("h", "help", false, " Print help text.");


        // Init variables, will be overwritten in parseArgs
        useVerifiableMessageStream = false;
        useSameSID = false;
        useChannelPerPub = false;
        useEmbeddedDriver = true;
        channel = null;
        port = 0;
        duration = 0;
        iterations = 0;
        senders = 0;
        receivers = 0;
        adders = 0;
        removers = 0;
        elements = 0;
        maxSize = 0;
        minSize = 0;
    }

    public int parseArgs(final String[] args)throws ParseException
    {
        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);
        String opt;

        if (command.hasOption("help"))
        {
            // Don't do anything, just signal the caller that they should call printHelp
            return 1;
        }

        opt = command.getOptionValue("verify", DEFAULT_VERIFIABLE_MESSAGE_STREAM);
        useVerifiableMessageStream = parseYesNo(opt);
        opt = command.getOptionValue("same-sid", DEFAULT_USE_SAME_SID);
        useSameSID = parseYesNo(opt);
        opt = command.getOptionValue("channel-per-pub", DEFAULT_USE_CHANNEL_PER_PUB);
        useChannelPerPub = parseYesNo(opt);
        opt = command.getOptionValue("channel", DEFAULT_CHANNEL);
        channel = parseString(opt);
        opt = command.getOptionValue("embedded", DEFAULT_USE_EMBEDDED_DRIVER);
        useEmbeddedDriver = parseYesNo(opt);
        opt = command.getOptionValue("port", DEFAULT_PORT);
        port = parsePositiveInt(opt);
        opt = command.getOptionValue("duration", DEFAULT_DURATION);
        duration = parsePositiveInt(opt);
        opt = command.getOptionValue("iterations", DEFAULT_ITERATIONS);
        iterations = parsePositiveInt(opt);
        opt = command.getOptionValue("senders", DEFAULT_SENDERS);
        senders = parsePositiveInt(opt);
        opt = command.getOptionValue("receivers", DEFAULT_RECEIVERS);
        receivers = parsePositiveInt(opt);
        opt = command.getOptionValue("adders", DEFAULT_ADDERS);
        adders = parsePositiveInt(opt);
        opt = command.getOptionValue("removers", DEFAULT_REMOVERS);
        removers = parsePositiveInt(opt);
        opt = command.getOptionValue("elements", DEFAULT_ELEMENTS);
        elements = parsePositiveInt(opt);
        opt = command.getOptionValue("max-size", DEFAULT_MAX_MSG_SIZE);
        maxSize = parsePositiveInt(opt);
        opt = command.getOptionValue("min-size", DEFAULT_MIN_MSG_SIZE);
        minSize = parsePositiveInt(opt);

        return 0;
    }
    /**
     * Print the help message for the available options.
     * @param program Name of the program calling print help.
     */
    public void printHelp(final String program)
    {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(program + " [options]", options);
        System.out.println(NL + USAGE + NL);
    }

    private boolean parseYesNo(final String opt) throws ParseException
    {
        boolean rc;
        if ("yes".equalsIgnoreCase(opt))
        {
            rc = true;
        }
        else if ("no".equalsIgnoreCase(opt))
        {
            rc = false;
        }
        else
        {
            throw new ParseException("An option specified '" + opt + "' can only be 'yes' or 'no'");
        }

        return rc;
    }

    private String parseString(final String opt)
    {
        //TODO: Add proper channel parsing
        return opt;
    }

    private int parsePositiveInt(final String opt) throws ParseException
    {
        int rc = 0;
        try
        {
            rc = Integer.parseInt(opt);
        }
        catch (final NumberFormatException e)
        {
            throw new ParseException("An integer could not be determined from the specified option: " + opt);
        }
        if (rc < 0)
        {
            throw new ParseException("An option specified '" + opt + "' can only be a positive integer");
        }

        return rc;
    }

    public boolean verifiable()
    {
        return this.useVerifiableMessageStream;
    }
    public boolean sameSID()
    {
        return this.useSameSID;
    }
    public boolean channelPerPub()
    {
        return this.useChannelPerPub;
    }
    public boolean embeddedDriver()
    {
        return this.useEmbeddedDriver;
    }
    public String channel()
    {
        return this.channel;
    }
    public int port()
    {
        return this.port;
    }
    public int duration()
    {
        return this.duration;
    }
    public int iterations()
    {
        return this.iterations;
    }
    public int senders()
    {
        return this.senders;
    }
    public int receivers()
    {
        return this.receivers;
    }
    public int adders()
    {
        return this.adders;
    }
    public int removers()
    {
        return this.removers;
    }
    public int elements()
    {
        return this.elements;
    }
    public int maxMsgSize()
    {
        return maxSize;
    }
    public int minMsgSize()
    {
        return minSize;
    }
}
