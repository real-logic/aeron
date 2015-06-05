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

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a class to hold information about what an Aeron publisher or subscriber
 * application should do. It's main purpose is to parse command line options. It may
 * open files during parsing, so all programs should call #close() to clean up properly.
 */
public class PubSubOptions
{
    /** line separator */
    private static final String NL = System.lineSeparator();

    /** Apache Commons CLI options */
    private final Options options;

    /** Application should print advanced usage guide with help */
    private boolean showUsage;
    /** Application should create an embedded Aeron media driver */
    private boolean useEmbeddedDriver;
    /** Application provided a session Id for all strings */
    private boolean useSessionId;
    /** Messages should include verifiable stream headers */
    private boolean useVerifiableStream;
    /** The seed for the random number generator */
    private long randomSeed;
    /** The number of messages an Application should send before exiting */
    private long messages;
    /** The number of times to repeat the sending rate pattern */
    private long iterations;
    /** Use session ID for all streams instead of default random */
    private int sessionId;
    /** The number of threads to use when sending or receiving in an application */
    private int threads;
    /** The Aeron channels to open */
    private List<ChannelDescriptor> channels;
    /** The total number of channel + stream Id pairs */
    private int totalStreams;
    /** The message rate sending pattern */
    private List<RateControllerInterval> rateIntervals;
    /** The stream used to generate data for a Publisher to send */
    private InputStream input;
    /** The stream used by a Subscriber to write the data received */
    private OutputStream output;
    /** The {@link MessageSizePattern} used to determine next message size */
    private MessageSizePattern sizePattern;

    private boolean outputNeedsClose;
    private boolean inputNeedsClose;

    /* Default values for options */
    private static final String DEFAULT_CHANNEL = "udp://localhost:31111#1";
    private static final String DEFAULT_DRIVER = "external";
    private static final String DEFAULT_INPUT = "null";
    private static final String DEFAULT_ITERATIONS = "1";
    private static final String DEFAULT_MESSAGES = "unlimited";
    private static final String DEFAULT_OUTPUT = "null";
    private static final String DEFAULT_RATE = "max";
    private static final String DEFAULT_SEED = "0";
    private static final String DEFAULT_SESSION = "default";
    private static final String DEFAULT_SIZE = "32";
    private static final String DEFAULT_THREADS = "1";
    private static final String DEFAULT_VERIFY = "yes";

    /** class that holds the default string values of the options */
    private static final OptionValuesStruct DEFAULT_VALUES = new PubSubOptions.OptionValuesStruct(
        DEFAULT_CHANNEL,
        DEFAULT_DRIVER,
        DEFAULT_INPUT,
        DEFAULT_ITERATIONS,
        DEFAULT_MESSAGES,
        DEFAULT_OUTPUT,
        DEFAULT_RATE,
        DEFAULT_SEED,
        DEFAULT_SESSION,
        DEFAULT_SIZE,
        DEFAULT_THREADS,
        DEFAULT_VERIFY);

    public PubSubOptions()
    {
        options = new Options();
        options.addOption("c",  "channels", true,
            "Create the given Aeron channels [default: " + DEFAULT_VALUES.channels + "].");
        options.addOption(null, "defaults", true,
            "File overriding default values for the command line options.");
        options.addOption(null, "driver", true,
            "Use 'external' or 'embedded' Aeron driver [default: " + DEFAULT_VALUES.driver + "].");
        options.addOption("h",  "help", false,
            "Display simple usage message.");
        options.addOption("i",  "input", true,
            "Publisher will send random bytes ('null'), " +
            "bytes read from stdin ('stdin'), or a file " +
            "(path to file) as data [default: " + DEFAULT_VALUES.input + "].");
        options.addOption(null, "iterations", true,
            "Run the rate sequence n times [default: " + DEFAULT_VALUES.iterations + "].");
        options.addOption("m",  "messages", true,
            "Send or receive n messages before exiting [default: " + DEFAULT_VALUES.messages + "].");
        options.addOption("o",  "output", true,
            "Subscriber will write the stream to the output file.");
        options.addOption("r",  "rate", true,
            "Send/receive rate pattern CSV list [default: " + DEFAULT_VALUES.rate + "].");
        options.addOption(null, "seed", true,
            "Random number generator seed.");
        options.addOption(null, "session", true,
            "Use session id for all publishers [default: " + DEFAULT_VALUES.session + "].");
        options.addOption("s",  "size", true,
            "Message payload size sequence, in bytes [default: " + DEFAULT_VALUES.size + "].");
        options.addOption("t",  "threads", true,
            "Round-Robin channels acress a number of threads [default: " + DEFAULT_VALUES.threads + "].");
        options.addOption(null, "usage", false,
            "Display advanced usage guide.");
        options.addOption(null, "verify", true,
            "Messages and streams are verifiable (yes|no) [default: " + DEFAULT_VALUES.verify + "].");

        // these will all be overridden in parseArgs
        randomSeed = 0;
        threads = 0;
        messages = 0;
        iterations = 0;
        sessionId = 0;
        totalStreams = 0;
        inputNeedsClose = false;
        outputNeedsClose = false;
        useEmbeddedDriver = false;
        useSessionId = false;
        sizePattern = null;
        input = null;
        output = null;
        channels = new ArrayList<ChannelDescriptor>();
        rateIntervals = new ArrayList<RateControllerInterval>();
    }

    /**
     * This is a struct for storing multiple ports on the same channel and to provide
     * a helper function to insert a specific port into the string for a valid aeron channel.
     * This can handle both styles of channel strings (aeron:, and udp:).
     */
    private class ChannelStruct
    {
        /** entire string that appears before the port value */
        String prefix;
        /** entire string that appears after the port value */
        String suffix;
        int portLow;
        int portHigh;

        public ChannelStruct()
        {
            clear();
        }

        public void clear()
        {
            prefix = "";
            suffix = "";
            portLow = 0;
            portHigh = 0;
        }

        /**
         * Helper function to add a single port to a channel string.
         * @param port
         * @return
         */
        String getChannelWithPort(final int port)
        {
            return prefix + ":" + port + suffix;
        }
    }

    /**
     * Internal structure to hold the default string values for each option
     */
    private static final class OptionValuesStruct
    {
        final String channels;
        final String driver;
        final String input;
        final String iterations;
        final String messages;
        final String output;
        final String rate;
        final String seed;
        final String session;
        final String size;
        final String threads;
        final String verify;

        OptionValuesStruct(
            final String channels,
            final String driver,
            final String input,
            final String iterations,
            final String messages,
            final String output,
            final String rate,
            final String seed,
            final String session,
            final String size,
            final String threads,
            final String verify)
        {
            this.channels = channels;
            this.driver = driver;
            this.input = input;
            this.iterations = iterations;
            this.messages = messages;
            this.output = output;
            this.rate = rate;
            this.seed = seed;
            this.session = session;
            this.size = size;
            this.threads = threads;
            this.verify = verify;
        }

        /** copy constructor for string values */
        OptionValuesStruct(final OptionValuesStruct other)
        {
            this.channels = other.channels;
            this.driver = other.driver;
            this.input = other.input;
            this.iterations = other.iterations;
            this.messages = other.messages;
            this.output = other.output;
            this.rate = other.rate;
            this.seed = other.seed;
            this.session = other.session;
            this.size = other.size;
            this.threads = other.threads;
            this.verify = other.verify;
        }

        /**
         * Copy constructor using a parsed command line and a default value if one
         * was not specified.
         * */
        OptionValuesStruct(final CommandLine cmd, final OptionValuesStruct other)
        {
            this.channels = cmd.getOptionValue("channels", other.channels);
            this.driver = cmd.getOptionValue("driver", other.driver);
            this.input = cmd.getOptionValue("input", other.input);
            this.iterations = cmd.getOptionValue("iterations", other.iterations);
            this.messages = cmd.getOptionValue("messages", other.messages);
            this.output = cmd.getOptionValue("output", other.output);
            this.rate = cmd.getOptionValue("rate", other.rate);
            this.seed = cmd.getOptionValue("seed", other.seed);
            this.session = cmd.getOptionValue("session", other.session);
            this.size = cmd.getOptionValue("size", other.size);
            this.threads = cmd.getOptionValue("threads", other.threads);
            this.verify = cmd.getOptionValue("verify", other.verify);
        }
    }

    /**
     * Parse command line arguments into usable objects. This must be called to set up the default values.
     * It's possible that this method will open a file for input or output so all users of this method should
     * also call #close().
     * @param args Command line arguments
     * @return 0 when options parsed, 1 if program should call {@link #printHelp(String)}.
     * @throws ParseException
     */
    public int parseArgs(final String[] args) throws ParseException
    {
        OptionValuesStruct defaults;
        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);
        String opt;

        if (command.hasOption("usage"))
        {
            showUsage = true;
            // Signal the application it should call printHelp
            return 1;
        }
        if (command.hasOption("help"))
        {
            // Don't do anything, just signal the caller that they should call printHelp
            return 1;
        }

        defaults = DEFAULT_VALUES;
        if (command.hasOption("defaults"))
        {
            // Load the defaults before parsing any of the values.
            defaults = getDefaultsFromOptionsFile(command.getOptionValue("defaults"));
        }

        opt = command.getOptionValue("threads", defaults.threads);
        threads(parseIntCheckPositive(opt));

        opt = command.getOptionValue("seed", defaults.seed);
        randomSeed(parseLongCheckPositive(opt));

        opt = command.getOptionValue("messages", defaults.messages);
        messages(parseNumberOfMessages(opt));

        opt = command.getOptionValue("iterations", defaults.iterations);
        iterations(parseIterations(opt));

        opt = command.getOptionValue("session", defaults.session);
        sessionId(parseSessionId(opt));

        opt = command.getOptionValue("driver", defaults.driver);
        useEmbeddedDriver(parseDriver(opt));

        opt = command.getOptionValue("input", defaults.input);
        parseInputStream(opt);

        opt = command.getOptionValue("output", defaults.output);
        parseOutputStream(opt);

        opt = command.getOptionValue("channels", defaults.channels);
        parseChannels(opt);

        opt = command.getOptionValue("rate", defaults.rate);
        parseRates(opt);

        opt = command.getOptionValue("size", defaults.size);
        parseMessageSizes(opt);

        opt = command.getOptionValue("verify", defaults.verify);
        parseVerify(opt);
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
        System.out.println(NL + USAGE_EXAMPLES + NL);
        if (showUsage)
        {
            System.out.println(ADVANCED_GUIDE);
        }
        else
        {
            System.out.println("Use --usage for expanded help message.");
        }
    }

    /**
     * Get the list of channels on which to publish or subscribe.
     * @return
     */
    public List<ChannelDescriptor> channels()
    {
        return channels;
    }

    /**
     * Set the list of channels on which to publish or subscribe
     * @param channels
     */
    public void channels(final List<ChannelDescriptor> channels)
    {
        this.channels = channels;
    }

    /**
     * Get the output stream where a subscriber will write received data.
     * @return
     */
    public OutputStream output()
    {
        return output;
    }

    /**
     * Set the output stream where a subscriber will write received data.
     * @param output
     */
    public void output(final OutputStream output)
    {
        this.output = output;
    }

    /**
     * Get the input stream that a Publisher will read for data to send.
     * @return
     */
    public InputStream input()
    {
        return input;
    }

    /**
     * Get if messages use additional space to store checksums for the message and stream.
     * @return
     */
    public boolean verify()
    {
        return useVerifiableStream;
    }

    /**
     * Set if messages use additional space to store checksums for the message and stream.
     * @param verify
     */
    public void verify(final boolean verify)
    {
        useVerifiableStream = verify;
    }

    /**
     * Set the input stream that a Publisher will read for data to send.
     * @param input
     */
    public void input(final InputStream input)
    {
        this.input = input;
    }

    public void rateIntervals(final List<RateControllerInterval> rates)
    {
        this.rateIntervals = rates;
    }

    public List<RateControllerInterval> rateIntervals()
    {
        return this.rateIntervals;
    }

    /**
     * Get the number of threads for the application to use.
     * @return
     */
    public int threads()
    {
        return threads;
    }

    /**
     * Set the number of threads for the application to use.
     * @param t Number of threads.
     */
    public void threads(final int t)
    {
        threads = t;
    }

    /**
     * Get the total number of messages an application will send or receive before exiting.
     * @return Total number of messages
     */
    public long messages()
    {
        return this.messages;
    }

    /**
     * Set the total number of messages an application will send or receive before exiting.
     * @param messages
     */
    public void messages(final long messages)
    {
        this.messages = messages;
    }

    /**
     * The number of times to run the rate sequence.
     * @return
     */
    public long iterations()
    {
        return iterations;
    }

    /**
     * The seed for a random number generator.
     * @return
     */
    public long randomSeed()
    {
        return randomSeed;
    }

    /**
     * Set the seed for a random number generator.
     * @param value
     */
    public void randomSeed(final long value)
    {
        randomSeed = value;
    }

    /**
     * Set the number of times to run the rate sequence.
     * @param value
     */
    public void iterations(final long value)
    {
        iterations = value;
    }

    /**
     * True when application should use an embedded Aeron media driver.
     * @return
     */
    public boolean useEmbeddedDriver()
    {
        return useEmbeddedDriver;
    }

    /**
     * Set the use of an embedded Aeron media driver.
     * @param embedded
     */
    public void useEmbeddedDriver(final boolean embedded)
    {
        useEmbeddedDriver = embedded;
    }

    /**
     * Enable or disable the use of a specific session ID.
     * @see #sessionId(int)
     * @param enabled
     */
    public void useSessionId(final boolean enabled)
    {
        this.useSessionId = enabled;
    }

    /**
     * When an Aeron stream should be created with a session ID this will return true. Otherwise
     * no session ID should be given to the Aeron transport.
     * @return True when a session ID should be used.
     * @see #sessionId()
     */
    public boolean useSessionId()
    {
        return this.useSessionId;
    }

    /**
     * Set the session ID to be used when #useSessionId() returns true.
     * @see #useSessionId #sessionId(boolean)
     * @param id
     */
    public void sessionId(final int id)
    {
        this.sessionId = id;
    }

    /**
     * Get the session ID to use for an Aeron transport. Only valid if #useSessionId() returns true.
     * @return The session ID for the Aeron transport.
     */
    public int sessionId()
    {
        return this.sessionId;
    }

    /**
     * Get the message size pattern used to determine what each messages size should be.
     * @return
     */
    public MessageSizePattern messageSizePattern()
    {
        return this.sizePattern;
    }

    /**
     * Set the message size pattern used to determine what each message size should be.
     * @param pattern
     */
    public void messageSizePattern(final MessageSizePattern pattern)
    {
        this.sizePattern = pattern;
    }

    /**
     * Return the total number of channel + streamId pairs specified by the channels option.
     * @return total number of channel + streamId paris.
     */
    public int numberOfStreams()
    {
        return totalStreams;
    }

    /**
     * Set the number of channel + streamId pairs.
     * @param value Representing the number of streams
     */
    public void numberOfStreams(final int value)
    {
        totalStreams = value;
    }
    /**
     * If the parsed arguments created file input or output streams, those need to be closed.
     * This is a convenience method that will handle all the closable cases for you. Call this
     * before shutting down an application. Output streams will also be flushed.
     */
    public void close() throws IOException
    {
        if (inputNeedsClose)
        {
            input.close();
            inputNeedsClose = false;
        }

        if (output != null)
        {
            output.flush();
            if (outputNeedsClose)
            {
                output.close();
                outputNeedsClose = false;
            }
        }
    }

    private long parseNumberOfMessages(final String m) throws ParseException
    {
        long value = Long.MAX_VALUE;
        if (!m.equalsIgnoreCase("unlimited"))
        {
            value = parseLongCheckPositive(m);
        }
        return value;
    }

    private void parseVerify(final String verifyStr) throws ParseException
    {
        if (verifyStr.equalsIgnoreCase("no"))
        {
            useVerifiableStream = false;
        }
        else if (verifyStr.equalsIgnoreCase("yes"))
        {
            useVerifiableStream = true;
        }
        else
        {
            throw new ParseException("The verify option '" + verifyStr + "' can only be 'yes' or 'no'");
        }
    }
    /**
     * Parse an integer for the session id. If the input is "default" the flag for useSessionId will be false.
     * If the string parses into a valid integer, useSessionId will be true.
     * @param sid Integer string or "default"
     * @return sessionId
     * @throws ParseException When input string is not "default" or an integer.
     */
    private int parseSessionId(final String sid) throws ParseException
    {
        int value = 0;
        useSessionId = false;
        if (!sid.equalsIgnoreCase("default"))
        {
            try
            {
                value = Integer.parseInt(sid);
            }
            catch (final NumberFormatException ex)
            {
                throw new ParseException("Could not parse session ID '" + sid + "' as an integer.");
            }
            useSessionId = true;
        }

        return value;
    }

    private long parseIterations(final String iterationsStr) throws ParseException
    {
        long value = Long.MAX_VALUE;
        if (!iterationsStr.equalsIgnoreCase("unlimited"))
        {
            value = parseLongCheckPositive(iterationsStr);
        }

        return value;
    }

    private boolean parseDriver(final String useEmbeddedStr) throws ParseException
    {
        boolean embedded;
        if (useEmbeddedStr.equalsIgnoreCase("external"))
        {
            embedded = false;
        }
        else if (useEmbeddedStr.equalsIgnoreCase("embedded"))
        {
            embedded = true;
        }
        else
        {
            throw new ParseException("Invalid driver option '" + useEmbeddedStr + "'. Must be 'embedded' or 'external'");
        }

        return embedded;
    }

    /**
     * Parses a comma separated list of channels. The channels can use ranges for ports and
     * stream-id on a per address basis. Channel Example: udp://192.168.0.100:21000-21004#1-10
     * will give 5 channels with 10 streams each.
     * @param csv
     * @throws ParseException
     */
    private void parseChannels(final String csv) throws ParseException
    {
        final ChannelStruct chan = new ChannelStruct();
        int streamIdLow = 1;
        int streamIdHigh = 1;
        final String[] channelDescriptions = csv.split(",");
        for (int i = 0; i < channelDescriptions.length; i++)
        {
            // channelComponents should have 1 or 2 pieces
            // 1 when only an address and ports are supplied, 2 when stream-ids are also supplied.
            final String[] channelComponents = channelDescriptions[i].split("#");
            if (channelComponents.length > 2)
            {
                throw new ParseException("Channel '" + channelDescriptions[i] + "' has too many '#' characters");
            }

            // get Channel structure
            final String address = channelComponents[0];
            if (address.startsWith("aeron:"))
            {
                parseAeronChannelToStruct(address, chan);
            }
            else
            {
                parseRawChannelToStruct(address, chan);
            }

            // get stream Ids
            if (channelComponents.length > 1)
            {
                final String ids = channelComponents[1];
                final int[] streamIdRange = findMinAndMaxStreamIds(ids);
                streamIdLow = streamIdRange[0];
                streamIdHigh = streamIdRange[1];
            }
            else
            {
                // no stream id specified, just use 1 for low and high
                streamIdLow = 1;
                streamIdHigh = 1;
            }

            // Sanity Check ports and streams
            if (chan.portLow < 0 || chan.portLow > 65535)
            {
                throw new ParseException("Low port of '" + channelDescriptions[i] + "' is not a valid port.");
            }
            if (chan.portHigh < 0 || chan.portHigh > 65535)
            {
                throw new ParseException("High port of '" + channelDescriptions[i] + "' is not a valid port.");
            }
            if (chan.portLow > chan.portHigh)
            {
                throw new ParseException("Low port of '" + channelDescriptions[i] + "' is greater than high port.");
            }
            if (streamIdLow > streamIdHigh)
            {
                throw new ParseException("Low stream-id of '" + channelDescriptions[i] + "' is greater than high stream-id.");
            }

            // OK, now create the channels.
            addChannelRanges(chan, streamIdLow, streamIdHigh);
        }
    }

    /**
     * Parse a channel that starts with "aeron:" which is a different way of defining
     * aeron channel that allows for more verbose settings.
     * for UDP unicast:   aeron:udp?remote=<ip>:<port(s)>|local=<interface>
     * for UDP multicast: aeron:udp?group=<multicast_ip>:<port(s)>|interface=<interface>
     * @param chanString Aeron URI
     * @param chanStruct Object is filled with values parsed from the chanString
     */
    private void parseAeronChannelToStruct(final String chanString, final ChannelStruct chanStruct) throws ParseException
    {
        // need to split out the values and find the ports
        final int ipv6PortIdx = chanString.indexOf("]:");
        String ports;

        if (ipv6PortIdx != -1)
        {
            // IPv6, ports immediately follow the "]:" sequence, and finish at the end of the
            // string or a | character.
            final int startIdx = ipv6PortIdx + 2;
            final int endIdx = findPortsEndIdx(chanString, startIdx);
            ports = chanString.substring(startIdx, endIdx);
            // base is everything up to and including the ]
            chanStruct.prefix = chanString.substring(0, ipv6PortIdx + 1);
            // anything after the ports is the suffix
            chanStruct.suffix = chanString.substring(endIdx, chanString.length());
        }
        else
        {
            // IPv4, The ports are located after the 2nd ":" character in the string, and finish
            // at the end of the string or a | character.
            final String[] addressComponents = chanString.split(":");
            if (addressComponents.length != 3)
            {
                throw new ParseException("Channel address '" + chanString + "' wrong number of ':' characters for IPv4.");
            }
            final int endIdx = findPortsEndIdx(addressComponents[2], 0);
            ports = addressComponents[2].substring(0, endIdx);
            chanStruct.prefix = addressComponents[0] + ":" + addressComponents[1];
            chanStruct.suffix = addressComponents[2].substring(endIdx, addressComponents[2].length());
        }

        final int[] portsArray = findMinAndMaxPort(ports);
        chanStruct.portLow = portsArray[0];
        chanStruct.portHigh = portsArray[1];
    }

    /**
     * Parse a raw aeron channel in the form: <media><channel>:<port(s)>
     * @param chanString URI
     * @param chanStruct This object is filled with the values parsed from chanString
     */
    private void parseRawChannelToStruct(final String chanString, final ChannelStruct chanStruct) throws ParseException
    {
        chanStruct.clear();
        String ports;
        final int ipv6PortIdx = chanString.indexOf("]:");
        if (ipv6PortIdx != -1)
        {
            // IPv6, ports are in the remaining characters of the string
            ports = chanString.substring(ipv6PortIdx + 2);
            // the base address is everything up to the closing bracket, but not the : and ports
            chanStruct.prefix = chanString.substring(0, ipv6PortIdx + 1);
        }
        else
        {
            // IPv4
            final String[] addressComponents = chanString.split(":");
            if (addressComponents.length != 3)
            {
                throw new ParseException("Channel address '" + chanString + "' wrong number of ':' characters for IPv4.");
            }
            ports = addressComponents[2];
            chanStruct.prefix = addressComponents[0] + ":" + addressComponents[1];
        }
        // get the port, or port range
        final int[] portsArray = findMinAndMaxPort(ports);
        chanStruct.portLow = portsArray[0];
        chanStruct.portHigh = portsArray[1];
    }

    /**
     * Walk the string to find the end of the ports, which can be a number or range.
     * Ports will end at the end of the string, or at a # or | character.
     * @param input
     * @param startIdx
     * @return The index of the first character not part of the ports string.
     */
    private int findPortsEndIdx(final String input, final int startIdx)
    {
        int endIdx;
        for (endIdx = startIdx; endIdx < input.length(); endIdx++)
        {
            if (input.charAt(endIdx) == '|')
            {
                break;
            }
        }

        return endIdx;
    }

    /**
     * Helper function to find low and high port from the port string in an address. This is mostly here
     * so that the parseChannels method isn't huge.
     * @param ports The port string which is either a number or range containing a hyphen.
     * @return An array of length 2 containing the low and high.
     */
    private int[] findMinAndMaxPort(final String ports) throws ParseException
    {
        int portLow = 0;
        int portHigh = 0;
        if (ports.contains("-"))
        {
            // It's a range in the form portLow-portHigh
            final String[] portRangeStrings = ports.split("-");
            if (portRangeStrings.length != 2)
            {
                throw new ParseException("Address port range '" + ports + "' contains too many '-' characters.");
            }

            try
            {
                portLow = Integer.parseInt(portRangeStrings[0]);
                portHigh = Integer.parseInt(portRangeStrings[1]);
            }
            catch (final NumberFormatException portRangeEx)
            {
                throw new ParseException("Address port range '" + ports + "' did not parse into two integers.");
            }
        }
        else
        {
            // It's a single port
            try
            {
                portLow = Integer.parseInt(ports);
                portHigh = portLow;
            }
            catch (final NumberFormatException portEx)
            {
                throw new ParseException("Address port '" + ports + "' didn't parse into an integer");
            }
        }
        if (portLow > portHigh)
        {
            throw new ParseException("Address port range '" + ports + "' has low port greater than high port.");
        }

        return new int[] { portLow, portHigh };
    }

    /**
     * Helper function to find the minimum and maximum values in the stream ID section of a channel.
     * This is mostly here so the parse channels function isn't too large.
     * @param inputString String containing the ids, either single integer or 2 integer range with hyphen.
     * @return An array that is always length 2 which contains minimum and maximum stream IDs.
     */
    private int[] findMinAndMaxStreamIds(final String inputString) throws ParseException
    {
        int streamIdLow = 1;
        int streamIdHigh = 1;

        String ids = inputString;
        // If the ids are part of an aeron: URI it might have a | in it with non channel information after
        final int indexOfPipe = ids.indexOf("|");
        if (indexOfPipe != -1)
        {
            ids = inputString.substring(0, indexOfPipe);
        }

        if (ids.contains("-"))
        {
            // identifier strings contain a low and a high
            final String[] idRange = ids.split("-");
            if (idRange.length != 2)
            {
                throw new ParseException("Stream ID range '" + ids + "' has too many '-' characters.");
            }
            try
            {
                streamIdLow = Integer.parseInt(idRange[0]);
                streamIdHigh = Integer.parseInt(idRange[1]);
            }
            catch (final NumberFormatException idRangEx)
            {
                throw new ParseException("Stream ID range '" + ids + "' did not parse into two integers.");
            }
        }
        else
        {
            // single Id specified
            try
            {
                streamIdLow = Integer.parseInt(ids);
                streamIdHigh = streamIdLow;
            }
            catch (final NumberFormatException streamIdEx)
            {
                throw new ParseException("Stream ID '" + ids + "' did not parse into an integer.");
            }
        }

        return new int[] { streamIdLow, streamIdHigh };
    }

    /**
     * Function to add ChannelDescriptor objects to the channels list.
     * @param chan Channel address including port low and high
     * @param streamIdLow
     * @param streamIdHigh
     */
    private void addChannelRanges(final ChannelStruct chan, final int streamIdLow, final int streamIdHigh)
    {
        int currentPort = chan.portLow;
        while (currentPort <= chan.portHigh)
        {
            final ChannelDescriptor cd = new ChannelDescriptor();
            cd.channel(chan.getChannelWithPort(currentPort));

            final int[] idArray = new int[streamIdHigh - streamIdLow + 1];
            int streamId = streamIdLow;
            for (int i = 0; i < idArray.length; i++)
            {
                // set all the session Ids in the array
                idArray[i] = streamId++;
            }
            cd.streamIdentifiers(idArray);
            channels.add(cd);
            totalStreams += idArray.length;
            currentPort++;
        }
    }

    /**
     *
     * @param ratesCsv
     */
    private void parseRates(final String ratesCsv) throws ParseException
    {
        final String[] rates = ratesCsv.split(",");
        for (final String currentRate : rates)
        {
            // the currentRate will contain a duration and rate
            // [(message|seconds)@](bits per second|messages per second)
            // i.e. 100s@1Mbps,1000m@10mps
            final String[] rateComponents = currentRate.split("@");
            if (rateComponents.length > 2)
            {
                throw new ParseException("Message rate '" + currentRate + "' contains too many '@' characters.");
            }

            // Duration is either in seconds or messages based on timeDuration flag.
            double duration = Long.MAX_VALUE;
            boolean timeDuration = true;
            if (rateComponents.length == 2)
            {
                // duration is seconds if it ends with 's'
                final String lowerCaseRate = rateComponents[0].toLowerCase();
                if (lowerCaseRate.endsWith("m"))
                {
                    // value is messages, not seconds
                    timeDuration = false;
                }
                else if (!lowerCaseRate.endsWith("s"))
                {
                    throw new ParseException("Rate " + rateComponents[0] + " does not contain 'm' or 's' to specify " +
                            "a duration in messages or seconds.");
                }
                final String durationStr = lowerCaseRate.substring(0, rateComponents[0].length() - 1);
                duration = parseDoubleBetweenZeroAndMaxLong(durationStr);
            }

            // rate string is always the last entry of the components
            final String rateComponent = rateComponents[rateComponents.length - 1];
            double rate = Long.MAX_VALUE;
            boolean bitsPerSecondRate = true;
            if (!rateComponent.equalsIgnoreCase("max"))
            {
                // rate string is not special value "max", determine value and type.
                // Find the first non-numeric character
                final Matcher matcher = Pattern.compile("[a-zA-Z]").matcher(rateComponent);
                if (!matcher.find())
                {
                    throw new ParseException("Rate " + rateComponent + " did not contain any units (Mbps, mps, etc...).");
                }
                final int idx = matcher.start();
                final String prefix = rateComponent.substring(0, idx);
                final String suffix = rateComponent.substring(idx, rateComponent.length());
                rate = parseDoubleBetweenZeroAndMaxLong(prefix);
                if (suffix.equalsIgnoreCase("mps"))
                {
                    bitsPerSecondRate = false;
                }
                else
                {
                    // rate is in bits per second, get the correct value based on suffix
                    rate *= parseBitRateMultiplier(suffix);
                }
            }
            addSendRate(duration, timeDuration, rate, bitsPerSecondRate);
        }
    }

    private void addSendRate(
        final double duration,
        final boolean isTimeDuration,
        final double rate,
        final boolean isBitsPerSecondRate)
    {
        // There are 4 combinations of potential rates, each with it's own implementation of RateControllerInterval.
        if (isTimeDuration)
        {
            if (isBitsPerSecondRate)
            {
                // number of seconds at bits per second
                rateIntervals.add(new SecondsAtBitsPerSecondInterval(duration, (long)rate));
            }
            else
            {
                // number of seconds at number of messages per second
                rateIntervals.add(new SecondsAtMessagesPerSecondInterval(duration, rate));
            }
        }
        else
        {
            if (isBitsPerSecondRate)
            {
                // number of messages at bits per second
                rateIntervals.add(new MessagesAtBitsPerSecondInterval((long)duration, (long)rate));
            }
            else
            {
                // number of messages at number of messages per second
                rateIntervals.add(new MessagesAtMessagesPerSecondInterval((long)duration, rate));
            }
        }
    }

    private void parseMessageSizes(final String cvs) throws ParseException
    {
        long numMessages = 0;
        int messageSizeMin = 0;
        int messageSizeMax = 0;

        final String[] sizeEntries = cvs.split(",");
        for (int i = 0; i < sizeEntries.length; i++)
        {
            // The message size may be separated with a '@' to send a number of messages at a given size or range.
            final String entryStr = sizeEntries[i];
            final String[] entryComponents = entryStr.split("@");
            if (entryComponents.length > 2)
            {
                throw new ParseException("Message size '" + entryStr + "' contains too many '@' characters.");
            }

            String sizeStr;
            // Get number of messages and find the size string to be parsed later
            if (entryComponents.length == 2)
            {
                // contains a number of messages followed by size or size range.
                // Example: 100@8K-1MB (100 messages between 8 kilobytes and 1 megabyte in length)
                try
                {
                    numMessages = Long.parseLong(entryComponents[0]);
                }
                catch (final NumberFormatException numMessagesEx)
                {
                    throw new ParseException("Number of messages in '" + entryStr + "' could not parse as long value");
                }
                sizeStr = entryComponents[1];
            }
            else
            {
                numMessages = Long.MAX_VALUE;
                sizeStr = entryComponents[0];
            }

            // parse the size string
            final String[] sizeRange = sizeStr.split("-");
            if (sizeRange.length > 2)
            {
                throw new ParseException("Message size range in '" + entryStr + "' has too many '-' characters.");
            }

            messageSizeMin = parseSize(sizeRange[0]);
            messageSizeMax = messageSizeMin;
            if (sizeRange.length == 2)
            {
                // A range was specified, find the max value
                messageSizeMax = parseSize(sizeRange[1]);
            }
            addSizeRange(numMessages, messageSizeMin, messageSizeMax);
        } // end for loop
    }

    /**
     * Parse a size into bytes. The size is a number with or without a suffix. The total bytes must be less
     * than Integer.MAX_VALUE.
     * Possible suffixes: B,b for bytes
     *                    KB,kb,K,k for kilobyte (1024 bytes)
     *                    MB,mb,M,m for megabytes (1024*1024 bytes)
     * @param sizeStr String containing formatted size
     * @return Number of bytes
     * @throws ParseException When input is invalid or number of bytes too large.
     */
    private int parseSize(final String sizeStr) throws ParseException
    {
        final int kb = 1024;
        final int mb = 1024 * 1024;
        int multiplier = 1;
        long size = 0;
        final String numberStr;

        if (sizeStr.endsWith("KB") || sizeStr.contains("kb"))
        {
            multiplier = kb;
            numberStr = sizeStr.substring(0, sizeStr.length() - 2);
        }
        else if (sizeStr.endsWith("K") || sizeStr.endsWith("k"))
        {
            multiplier = kb;
            numberStr = sizeStr.substring(0, sizeStr.length() - 1);
        }
        else if (sizeStr.endsWith("MB") || sizeStr.contains("mb"))
        {
            multiplier = mb;
            numberStr = sizeStr.substring(0, sizeStr.length() - 2);
        }
        else if (sizeStr.endsWith("M") || sizeStr.endsWith("m"))
        {
            multiplier = mb;
            numberStr = sizeStr.substring(0, sizeStr.length() - 1);
        }
        else if (sizeStr.endsWith("B") || sizeStr.endsWith("b"))
        {
            multiplier = 1;
            numberStr = sizeStr.substring(0, sizeStr.length() - 1);
        }
        else
        {
            // No suffix, assume bytes.
            multiplier = 1;
            numberStr = sizeStr;
        }

        try
        {
            size = Long.parseLong(numberStr);
        }
        catch (final Exception ex)
        {
            throw new ParseException("Could not parse '" + numberStr + "' into a long value.");
        }
        size *= multiplier;

        if (size > Integer.MAX_VALUE || size < 0)
        {
            // can't be larger than max signed int (2 gb) or less than 0.
            throw new ParseException("Payload size '" + sizeStr + "' too large or negative.");
        }
        return (int)size;
    }

    private void addSizeRange(final long messages, final int minSize, final int maxSize) throws ParseException
    {
        try
        {
            if (sizePattern == null)
            {
                sizePattern = new MessageSizePattern(messages, minSize, maxSize);
            }
            else
            {
                sizePattern.addPatternEntry(messages, minSize, maxSize);
            }
        }
        catch (final Exception ex)
        {
            throw new ParseException(ex.getMessage());
        }
    }

    private void parseInputStream(final String inputStr) throws ParseException
    {
        if (inputStr.equalsIgnoreCase("null"))
        {
            input(null);
        }
        else if (inputStr.equalsIgnoreCase("stdin"))
        {
            input(System.in);
        }
        else
        {
            try
            {
                input(new FileInputStream(inputStr));
            }
            catch (final FileNotFoundException ex)
            {
                throw new ParseException("Input file '" + inputStr + "' not found.");
            }
            // keep track of the fact we need to close this file input stream.
            inputNeedsClose = true;
        }
    }

    private void parseOutputStream(final String outputStr) throws ParseException
    {
        if (outputStr.equalsIgnoreCase("null"))
        {
            output(null);
        }
        else if (outputStr.equalsIgnoreCase("stdout"))
        {
            output(System.out);
        }
        else if (outputStr.equalsIgnoreCase("stderr"))
        {
            output(System.err);
        }
        else
        {
            try
            {
                output(new FileOutputStream(outputStr));
            }
            catch (final FileNotFoundException ex)
            {
                throw new ParseException("Could not open file '" + outputStr + "' for writing");
            }
            // Keep track of the fact we need to close this file stream
            outputNeedsClose = true;
        }
    }

    /**
     *
     * @param filename
     * @return
     * @throws ParseException
     */
    private PubSubOptions.OptionValuesStruct getDefaultsFromOptionsFile(final String filename) throws ParseException
    {
        BufferedReader br;
        final ArrayList<String> args = new ArrayList<String>();
        try
        {
            br = newBufferedFileReader(filename);
        }
        catch (final FileNotFoundException ex)
        {
            throw new ParseException("Option defaults file '" + filename + "' not found.");
        }

        String line;
        try
        {
            // build up the args list and we will use it to create a new CommandLine
            // object to parse the values for our options.
            int lineCount = 0;
            while ((line = br.readLine()) != null)
            {
                lineCount++;
                line = line.trim();
                // # is a commented line, and line length 0 is empty
                if (line.length() > 0 && !line.startsWith("#"))
                {
                    // Split values by any number of consecutive whitespaces.
                    final String[] arguments = line.split("\\s+");
                    Collections.addAll(args, arguments);
                }
            }
            br.close();
        }
        catch (final IOException ex)
        {
            throw new ParseException(ex.getMessage());
        }

        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args.toArray(new String[args.size()]));
        return new OptionValuesStruct(command, DEFAULT_VALUES);
    }

    BufferedReader newBufferedFileReader(final String filename) throws FileNotFoundException
    {
        return new BufferedReader(new FileReader(filename));
    }

    /**
     * Parses a bit rate multiplier based on a string that may contain Gbps, Mbps, Kbps, bps
     * @param s
     * @return
     * @throws Exception
     */
    private int parseBitRateMultiplier(final String s) throws ParseException
    {
        final String rateLowercase = s.toLowerCase();

        if (rateLowercase.equals("gbps"))
        {
            return 1000000000;
        }
        if (rateLowercase.equals("mbps"))
        {
            return 1000000;
        }
        if (rateLowercase.equals("kbps"))
        {
            return 1000;
        }
        if (rateLowercase.equals("bps"))
        {
            return 1;
        }
        throw new ParseException("bit rate " + s + " was not 'Gbps','Mbps','Kbps', or 'bps'.");
    }

    /**
     * Parses a long string and returns the value. Value must be positive.
     * @param longStr
     * @return
     * @throws ParseException
     */
    private long parseLongCheckPositive(final String longStr) throws ParseException
    {
        long value;

        try
        {
            value = Long.parseLong(longStr);
        }
        catch (final NumberFormatException ex)
        {
            throw new ParseException("Could not parse '" + longStr + "' as a long value.");
        }
        if (value < 0)
        {
            throw new ParseException("Long value '" + longStr + "' must be positive.");
        }
        return value;
    }

    /**
     * Parses an integer and returns the value if positive.
     * @param intStr
     * @return
     * @throws ParseException
     */
    private int parseIntCheckPositive(final String intStr) throws  ParseException
    {
        int value;

        try
        {
            value = Integer.parseInt(intStr);
        }
        catch (final NumberFormatException ex)
        {
            throw new ParseException("Could not parse '" + intStr + "' as an int value");
        }
        if (value < 0)
        {
            throw new ParseException("Integer value '" + "' must be positive");
        }
        return value;
    }

    private double parseDoubleBetweenZeroAndMaxLong(final String doubleStr) throws ParseException
    {
        double value = 0;

        try
        {
            value = Double.parseDouble(doubleStr);
        }
        catch (final NumberFormatException ex)
        {
            throw new ParseException("Could not parse '" + doubleStr + " as a double value.");
        }
        if (value < 0D || value > Long.MAX_VALUE)
        {
            throw new ParseException("Double value '" + value + "' must be positive and <= long max value.");
        }
        return value;
    }

    private static final String USAGE_EXAMPLES = "" +
        // stay within column 88 (80 when printed). That's here ---------------------> |
        "Examples:" + NL +
        "-c udp://localhost:31111 -r 60m@1mps" + NL +
        "    Send 60 messages at a rate of 1 message per second" + NL +
        NL +
        "-c udp://224.10.10.12:30000#1-10 -r 1Mbps -s 100-200 -m 1000000 -t 2" + NL +
        "    Create 10 multicast channels on port 30000 using stream ID 1 through 10." + NL +
        "    These channels will be split Round-Robin across 2 threads that will each" + NL +
        "    send messages sized between 100 and 200 bytes at a rate of 1Mbps. After a" + NL +
        "    total of 1 million messages have been sent, the program will exit.";

    /** Advanced guide to the function and format of command line parameters */
    private static final String ADVANCED_GUIDE = "" +
        // stay within column 88 (80 when printed). That's here ---------------------> |
        "Options Usage Guide" + NL +
        NL +
        "-c,--channels '(csv list)'" + NL +
        "    This is a list of one or more Aeron channels. The value may represent a" + NL +
        "    single channel or contain ranges for both ports and stream IDs. Many" + NL +
        "    channels may be defined by using a comma separated list. There are 3 parts" + NL +
        "    to each channel; Address, port, and stream ID. The port and stream ID can" + NL +
        "    be either a single value, or a low to high range separated by a '-'. The" + NL +
        "    port and stream ID values are combined together to create a cartesian" + NL +
        "    product of channels for the given address." + NL +
        "    *NOTE: Enclose entire value in single quotes when on a command prompt." + NL +
        NL +
        "    Entry Input Format:" + NL +
        "    'udp://<IP>:port[-portHigh][#streamId[-streamIdHigh]][,...]'" + NL +
        "    [OR]" + NL +
        "    'aeron:udp?(group|remote)<IP>:port[-portHigh][|(local|address)<IP>]" + NL +
        "            [#streamId[-streamIdHigh]][,...]'" + NL +
        "        For multicast use group and address, for unicast use local and remote." + NL +
        NL +
        "    IP addresses can be v4 or v6. IPv6 addresses must be in brackets [ ]." + NL +
        NL +
        "    Examples:" + NL +
        "    udp://localhost:21000" + NL +
        "        Use one channel on port 21000 with stream ID 1" + NL +
        "    udp://224.10.10.21:9100-9109#5" + NL +
        "        Use 10 channels on port 9100 through 9109 all with stream ID 5." + NL +
        "    udp://localhost:21000#5,udp://224.10.10.20:9100-9109#5" + NL +
        "        Comma separated list of the previous two examples, 11 total channels." + NL +
        "    udp://192.168.0.101:9100-9109#5-6" + NL +
        "        On each port between 9100 and 9109 create a channel with stream ID 5" + NL +
        "        and another with stream ID 6 for 20 total channels." + NL +
        "    aeron:udp?group=224.10.10.21:9100|address=192.168.0.101" + NL +
        "        Send to multicast group 224.10.10.21 port 9100 using an interface." + NL +
        "    aeron:udp?remote=192.168.0.100:21000|local=192.168.0.121" + NL +
        "        Send unicast to 192.168.0.100 on port 21000 with stream ID 1." + NL +
        NL +
        "--defaults (filename)" + NL +                                              // |
        "    This allows a file to change the default option values for the program." + NL +
        "    The file is loaded before applying any other command line parameters, so" + NL +
        "    any duplicate options on the command line will override the value in the" + NL +
        "    options file. The syntax for the file is the same as the command line," + NL +
        "    with the exceptions that a '#' used to start a line is considered a" + NL +
        "    comment, and a new line can be used in place of a space." + NL +
        NL +
        "--driver (embedded|external)" + NL +                                       // |
        "    Controls whether the application will start an embedded Aeron messaging" + NL +
        "    driver or communicate with an external one." +
        "" + NL +
        "-h,--help" + NL +                                                          // |
        "    Show the shorthand usage guide." + NL +
        NL +
        "-i,--input (null|stdin|<file>)" + NL +                                     // |
        "    Input data for a Publisher to send. When set to 'null' and by default," + NL +
        "    the publisher will generate random data. If 'stdin' is used, standard" + NL +
        "    input will be sent. Any other value is assumed to be a filename. When the" + NL +
        "    publisher reaches the end of the stream, it will exit." + NL +
        NL +
        "--iterations (number)" + NL +                                              // |
        "    Repeat the send rate pattern the given number of times, then exit. See" + NL +
        "    the --rate option." + NL +
        NL +
        "-m,--messages (number)" + NL +                                             // |
        "    Exit after the application sends or receives a given number of messages." + NL +
        NL +
        "-o,--output (null|stdout|stderr|<file>)" + NL +
        "    A subscriber will write data received to the given output stream. By" + NL +
        "    default, the subscriber will not write to any stream. This is the " + NL +
        "    behavior of the 'null' value." + NL +
        NL +
        "-r,--rate (csv list)" + NL +                                               // |
        "    This is a list of one or more send rates for a publisher. Each rate entry" + NL +
        "    contains two parts, duration and speed. The duration is the number of" + NL +
        "    seconds or number of messages, and the speed is the bits per second or" + NL +
        "    messages per second. With these options there are four valid combinations" + NL +
        "    of entries; Messages at messages per second, messages at bits per second," + NL +
        "    seconds at messages per second, and seconds at bits per second. The suffix" + NL +
        "    that appears after the numbers determines the type. The 'G', 'M', and 'K'" + NL +
        "    prefix can be used with bps. A sending application will run through the" + NL +
        "    rate pattern once, or --iterations times before exiting. If the duration" + NL +
        "    is not supplied, then it is assumed to mean forever." + NL +
        NL +
        "    Entry Input Format:" + NL +
        "    [<duration>(m|s)@]<speed>(mps|bps)[,...]" + NL +
        NL +                                                                        // |
        "    Examples:" + NL +
        "    10Mbps" + NL +
        "        Send forever at 10 Megabits per second." + NL +
        "    1000m@10mps" + NL +
        "        Send 1000 messages at 10 messages per second." + NL +
        "    10s@1.5Kbps,1s@1Gbps,0.5mps" + NL +
        "        Send for 10 seconds at 1.5 Kilobit per second, spike to 1" + NL +
        "        Gigabit per second for 1 second, then send one message every 2 seconds" + NL +
        "        forever." + NL +
        NL +
        "--seed (number)" + NL +                                                    // |
        "    Set the seed for the random number generator. If multiple threads are" + NL +
        "    being used, each one will use an incrementing seed value." + NL +
        NL +
        "--session (number|default)" + NL +                                         // |
        "    All publishers will be created using the given number as their session ID." + NL +
        "    The special value \"default\" can be used to allow Aeron to select an ID" + NL +
        "    at random." + NL +
        "" + NL +
        "-s,--size (csv list)" + NL +                                               // |
        "    This is a list of one or more message payload sizes. Each entry in the" + NL +
        "    list contains up to two parts, the number of messages and the size or" + NL +
        "    range of possible sizes. The size is specified as a number and optional" + NL +
        "    suffix. A range of sizes is specified by two sizes separated by a hyphen." + NL +
        "    Possible suffixes are 'GB' or 'G', 'MB' or 'M', 'KB' or 'K', and 'B'. " + NL +
        "    The values are binary units, so 'KB' is actually 1024 bytes. If the number" + NL +
        "    of messages not specified then the given size or range will be used" + NL +
        "    indefinitely. The pattern of message sizes will repeat until the sender" + NL +
        "    exits." + NL +
        NL +
        "    Entry Input Format:" + NL +
        "    [<messages>@]<size>[B][-<maximum>[B]][,...]" + NL +
        NL +
        "    Examples:" + NL +
        "    100" + NL +
        "        All messages will be 100 bytes in size." + NL +
        "    32-1KB" + NL +
        "        All messages will have a random size between 32 and 1024 bytes." + NL +
        "    99@8K,1@1MB-2MB" + NL +
        "        The first 99 messages will be 8 Kilobytes in size, then one message" + NL +
        "        will be between 1 Megabyte and 2 Megabytes. This pattern will repeat" + NL +
        "        as long as messages are being sent." + NL +
        NL +
        "-t,--threads (number)" + NL +                                              // |
        "    Use the given number of threads to process channels. Channels are split" + NL +
        "    Round-Robin across the threads." + NL +
        NL +
        "--verify (yes|no)" + NL +                                                  // |
        "    Each message will reserve space for checksum data that can be used to" + NL +
        "    verify both the individual message and the stream up to that point." + NL +
        "    The default behavior is 'yes', and will use the first 16 bytes of the" + NL +
        "    message payload to store verification data. To send messages with less" + NL +
        "    than 16 bytes of payload this option must be set to 'no'. Subscribers" + NL +
        "    can detect that a message is verifiable. The checksums are not written" + NL +
        "    to the output stream.";
}
