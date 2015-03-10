package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;

public class PublisherTool
{
    PublisherTool(String[] args)
    {
        PubSubOptions opts = new PubSubOptions();
        try
        {
            if (opts.parseArgs(args) == 1)
            {
                opts.printHelp("PublisherTool");
                System.exit(0);
            }
        }
        catch (ParseException ex)
        {
            System.out.println(ex.getMessage());
            System.exit(-1);
        }
    }

    public static void main(String[] args)
    {
        final PublisherTool app = new PublisherTool(args);
    }
}