package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;

public class PublisherTool
{
    public static final String APP_USAGE = "PublisherTool";

    private final PubSubOptions options;

    public PublisherTool(PubSubOptions options)
    {
        this.options = options;
    }

    public static void main(String[] args)
    {
        PubSubOptions opts = new PubSubOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                opts.printHelp(PublisherTool.APP_USAGE);
                System.exit(0);
            }
        }

        catch (ParseException ex)
        {
            ex.printStackTrace();
            opts.printHelp(PublisherTool.APP_USAGE);
            System.exit(-1);
        }
        final PublisherTool app = new PublisherTool(opts);
    }
}