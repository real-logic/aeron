package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.*;

/**
 * Created by mike on 4/16/2015.
 */
public class ThwackerOptions
{
    static final String defaultVerifiableMessageStream = "no";
    static final String defaultUseSameSID = "no";
    static final String defaultUseChannelPerPub = "no";
    static final String defaultUseEmbeddedDriver = "yes";
    static final String defaultChannel = "udp://localhost:";
    static final String defaultPort = "51234";
    static final String defaultDuration = "30000";
    static final String defaultIterations = "1";
    static final String defaultSenders = "1";
    static final String defaultReceivers = "1";
    static final String defaultAdders = "1";
    static final String defaultRemovers = "1";
    static final String defaultElements = "5";



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

    }

    public int parseArgs(String[] args)throws ParseException
    {
        CommandLineParser parser = new GnuParser();
        CommandLine command = parser.parse(options, args);
        String opt;

        opt = command.getOptionValue("verify", defaultVerifiableMessageStream);
        useVerifiableMessageStream = parseYesNo(opt);
        opt = command.getOptionValue("same-sid", defaultUseSameSID);
        useSameSID = parseYesNo(opt);
        opt = command.getOptionValue("channel-per-pub", defaultUseChannelPerPub);
        useChannelPerPub = parseYesNo(opt);
        opt = command.getOptionValue("channel", defaultChannel);
        channel = parseString(opt);
        opt = command.getOptionValue("embedded", defaultUseEmbeddedDriver);
        useEmbeddedDriver = parseYesNo(opt);
        opt = command.getOptionValue("port", defaultPort);
        port = parsePositiveInt(opt);
        opt = command.getOptionValue("duration", defaultDuration);
        duration = parsePositiveInt(opt);
        opt = command.getOptionValue("iterations", defaultIterations);
        iterations = parsePositiveInt(opt);
        opt = command.getOptionValue("senders", defaultSenders);
        senders = parsePositiveInt(opt);
        opt = command.getOptionValue("receivers", defaultReceivers);
        receivers = parsePositiveInt(opt);
        opt = command.getOptionValue("adders", defaultAdders);
        adders = parsePositiveInt(opt);
        opt = command.getOptionValue("removers", defaultRemovers);
        removers = parsePositiveInt(opt);
        opt = command.getOptionValue("elements", defaultElements);
        elements = parsePositiveInt(opt);

        return 0;

    }

    public boolean parseYesNo(String opt) throws ParseException
    {
        boolean rc;
        if(opt.equalsIgnoreCase("yes"))
        {
            rc = true;
        }
        else if(opt.equalsIgnoreCase("no"))
        {
            rc = false;
        }
        else
        {
            throw new ParseException("An option specified '" + opt + "' can only be 'yes' or 'no'");
        }
        return rc;
    }

    public String parseString(String opt)
    {
        //TODO: Add proper channel parsing
        return opt;
    }

    public int parsePositiveInt(String opt) throws ParseException
    {
        int rc = 0;
        try
        {
            rc = Integer.parseInt(opt);
        }
        catch(NumberFormatException e)
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
}
