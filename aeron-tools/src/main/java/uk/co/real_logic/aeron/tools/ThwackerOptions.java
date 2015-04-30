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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Created by mike on 4/16/2015.
 */
public class ThwackerOptions
{
    static final String DEFAULT_VERIFIABLE_MESSAGE_STREAM = "no";
    static final String DEFAULT_USE_SAME_SID = "no";
    static final String DEFAULT_USE_CHANNEL_PER_PUB = "no";
    static final String DEFAULT_USE_EMBEDDED_DRIVER = "yes";
    static final String DEFAULT_CHANNEL = "udp://localhost:";
    static final String DEFAULT_PORT = "51234";
    static final String DEFAULT_DURATION = "30000";
    static final String DEFAULT_ITERATIONS = "1";
    static final String DEFAULT_SENDERS = "1";
    static final String DEFAULT_RECEIVERS = "1";
    static final String DEFAULT_ADDERS = "1";
    static final String DEFAULT_REMOVERS = "1";
    static final String DEFAULT_ELEMENTS = "10";
    static final String DEFAULT_MAX_MSG_SIZE = "35";
    static final String DEFAULT_MIN_MSG_SIZE = "35";



    Options options = null;

    boolean useVerifiableMessageStream;
    boolean useSameSID;
    boolean useChannelPerPub;
    boolean useEmbeddedDriver;
    String channel;
    int port;
    int duration;
    int iterations;
    int senders;
    int receivers;
    int adders;
    int removers;
    int elements;
    int maxSize;
    int minSize;



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

    public boolean parseYesNo(final String opt) throws ParseException
    {
        boolean rc;
        if (opt.equalsIgnoreCase("yes"))
        {
            rc = true;
        }
        else if (opt.equalsIgnoreCase("no"))
        {
            rc = false;
        }
        else
        {
            throw new ParseException("An option specified '" + opt + "' can only be 'yes' or 'no'");
        }
        return rc;
    }

    public String parseString(final String opt)
    {
        //TODO: Add proper channel parsing
        return opt;
    }

    public int parsePositiveInt(final String opt) throws ParseException
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

    public boolean getVerifiable()
    {
        return this.useVerifiableMessageStream;
    }

    public boolean getSameSID()
    {
        return this.useSameSID;
    }

    public boolean getChannelPerPub()
    {
        return this.useChannelPerPub;
    }
    public boolean getEmbeddedDriver()
    {
        return this.useEmbeddedDriver;
    }
    public String getChannel()
    {
        return this.channel;
    }
    public int getPort()
    {
        return this.port;
    }
    public int getDuration()
    {
        return this.duration;
    }
    public int getIterations()
    {
        return this.iterations;
    }
    public int getSenders()
    {
        return this.senders;
    }
    public int getReceivers()
    {
        return this.receivers;
    }
    public int getAdders()
    {
        return this.adders;
    }
    public int getRemovers()
    {
        return this.removers;
    }
    public int getElements()
    {
        return this.elements;
    }
    public int getMaxMsgSize()
    {
        return maxSize;
    }
    public int getMinMsgSize()
    {
        return minSize;
    }
}
