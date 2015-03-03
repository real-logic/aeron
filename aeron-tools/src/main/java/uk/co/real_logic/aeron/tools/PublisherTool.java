package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;

public class PublisherTool
{
    public static void main(String[] args) {
        PubSubOptions opts = new PubSubOptions();
        opts.printHelp("PublisherTool");
        try {
            opts.parseArgs(args);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            System.exit(-1);
        }
    }
}