package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.*;

import java.util.List;

/**
 * Created by bhorst on 3/3/15.
 */
public class PubSubOptions {
    final Options options;

    long randomSeed;
    long messages;
    long threads;
    long iterations;

    public PubSubOptions() {
        // TODO: Add more detail to the descriptions
        options = new Options();
        options.addOption("r", "rate", true, "Send rate pattern.");
        options.addOption("m", "messages", true, "Send n messages before exiting.");
        options.addOption("i", "iterations", true, "Run the rate sequence n times.");
        options.addOption("s", "size", true, "Message payload size sequence, in bytes.");
        options.addOption(null, "seed", true, "Random number generator seed.");
        options.addOption("d", "data", true, "Send data file or verifiable stream.");
        options.addOption("c", "channels", true, "Create the given channels.");
        options.addOption(null, "driver", true, "'external' or 'embedded' driver.");
        options.addOption("t", "threads", true, "Number of threads.");

        // these will all be overridden in parseArgs
        randomSeed = 0;
        threads = 0;
        messages = 0;
        iterations = 0;
    }

    public void parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new GnuParser();
        CommandLine command = parser.parse(options, args);

        String opt;

        // threads, default = 1
        opt = command.getOptionValue("t", "1");
        try {
            setThreads(Long.parseLong(opt));
        } catch (NumberFormatException threads_ex) {
            throw new ParseException("Couldn't parse threads value '" + opt + "' as type long.");
        }

        // seed, default = 0
        opt = command.getOptionValue("seed", "0");
        try {
            randomSeed = Long.parseLong(opt);
        } catch (NumberFormatException seed_ex) {
            throw new ParseException("Couldn't parse randomSeed value '" + opt + "' as type long.");
        }

        // messages, default = unlimited (max long value)
        opt = command.getOptionValue("messages", "unlimited");
        try {
            if (opt.equalsIgnoreCase("unlimited")) {
                messages = Long.MAX_VALUE;
            } else {
                messages = Long.parseLong(opt);
            }
        } catch (NumberFormatException messages_ex) {
            throw new ParseException("Couldn't parse messages value '" + opt + "' as type long.");
        }

        // iterations, default = 1
        opt = command.getOptionValue("iterations", "1");
        try {
            iterations = Long.parseLong(opt);
        } catch (NumberFormatException iterations_ex) {
            throw new ParseException("Couldn't parse iterations value '" + opt + "' as type long.");
        }
    }

    public List<String> getChannels() {
        return null;
    }

    public void printHelp(String program) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(program, options);
    }

    public long getThreads() {
        return threads;
    }
    public void setThreads(long t) {
        threads = t;
    }
}
