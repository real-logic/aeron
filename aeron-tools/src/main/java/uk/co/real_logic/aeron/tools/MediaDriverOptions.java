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
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.aeron.driver.LossGenerator;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MediaDriverOptions
{
    // Command line options for media driver
    private final Options options;
    // Properties containing Aeron settings.
    private Properties properties;

    private IdleStrategy conductorIdleStrategy;
    private IdleStrategy senderIdleStrategy;
    private IdleStrategy receiverIdleStrategy;
    private IdleStrategy sharedNetworkIdleStrategy;
    private IdleStrategy sharedIdleStrategy;
    private LossGenerator dataLossGenerator;
    private LossGenerator controlLossGenerator;

    private static final String NULL_VALUE = "null";

    public MediaDriverOptions()
    {
        options = new Options()
            .addOption("p", "properties", true, "A properties file containing Aeron options.")
            .addOption("h", "help", false, "Display help message.")
            .addOption("c", "conductor", true, "IdleStrategy class for the conductor thread.")
            .addOption("s", "sender", true, "IdleStrategy class for the sender thread.")
            .addOption("r", "receiver", true, "IdleStrategy class for the receiver thread.")
            .addOption("n", "network", true, "IdleStrategy class for the shared network thread.")
            .addOption("a", "shared", true, "IdleStrategy class for the shared thread.")
            .addOption(null, "data-loss", true, "LossGenerator class for dropping incoming data.")
            .addOption(null, "control-loss", true, "LossGenerator class for dropping incoming control data.");
    }

    /**
     * Parse command line arguments for MediaDriverTool and call the setter functions.
     *
     * @param args Command line arguments.
     * @return 0 on success, 1 if application should call {@link #printHelp(String)}
     * @throws org.apache.commons.cli.ParseException On string parsing error.
     */
    public int parseArgs(final String[] args) throws ParseException
    {
        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);

        if (command.hasOption("help"))
        {
            // Inform application the user wants help message.
            return 1;
        }

        String senderValue = NULL_VALUE;
        String receiverValue = NULL_VALUE;
        String sharedNetworkValue = NULL_VALUE;
        String sharedValue = NULL_VALUE;
        String conductorValue = NULL_VALUE;
        String dataLossValue = NULL_VALUE;
        String controlLossValue = NULL_VALUE;

        if (command.hasOption("properties"))
        {
            this.properties = parseProperties(command.getOptionValue("properties"));
            senderValue = properties.getProperty("aeron.tools.mediadriver.sender");
            receiverValue = properties.getProperty("aeron.tools.mediadriver.receiver");
            sharedNetworkValue = properties.getProperty("aeron.tools.mediadriver.network");
            sharedValue = properties.getProperty("aeron.tools.mediadriver.shared");
            conductorValue = properties.getProperty("aeron.tools.mediadriver.conductor");
            dataLossValue = properties.getProperty("aeron.tools.mediadriver.data.loss");
            controlLossValue = properties.getProperty("aeron.tools.mediadriver.control.loss");
        }

        senderIdleStrategy = parseIdleStrategy(command.getOptionValue("sender", senderValue));
        receiverIdleStrategy = parseIdleStrategy(command.getOptionValue("receiver", receiverValue));
        sharedNetworkIdleStrategy = parseIdleStrategy(command.getOptionValue("network", sharedNetworkValue));
        sharedIdleStrategy = parseIdleStrategy(command.getOptionValue("shared", sharedValue));
        conductorIdleStrategy = parseIdleStrategy(command.getOptionValue("conductor", conductorValue));
        dataLossGenerator = parseLossGenerator(command.getOptionValue("data-loss", dataLossValue));
        controlLossGenerator = parseLossGenerator(command.getOptionValue("control-loss", controlLossValue));

        return 0;
    }

    public void printHelp(final String applicationName)
    {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(applicationName + " [options]", options);
    }

    public IdleStrategy conductorIdleStrategy()
    {
        return conductorIdleStrategy;
    }

    public void conductorIdleStrategy(final IdleStrategy conductorIdleStrategy)
    {
        this.conductorIdleStrategy = conductorIdleStrategy;
    }

    public IdleStrategy senderIdleStrategy()
    {
        return senderIdleStrategy;
    }

    public void senderIdleStrategy(final IdleStrategy senderIdleStrategy)
    {
        this.senderIdleStrategy = senderIdleStrategy;
    }

    public IdleStrategy receiverIdleStrategy()
    {
        return receiverIdleStrategy;
    }

    public void receiverIdleStrategy(final IdleStrategy receiverIdleStrategy)
    {
        this.receiverIdleStrategy = receiverIdleStrategy;
    }

    public IdleStrategy sharedNetworkIdleStrategy()
    {
        return sharedNetworkIdleStrategy;
    }

    public void sharedNetworkIdleStrategy(final IdleStrategy sharedNetworkIdleStrategy)
    {
        this.sharedNetworkIdleStrategy = sharedNetworkIdleStrategy;
    }

    public IdleStrategy sharedIdleStrategy()
    {
        return sharedIdleStrategy;
    }

    public void sharedIdleStrategy(final IdleStrategy sharedIdleStrategy)
    {
        this.sharedIdleStrategy = sharedIdleStrategy;
    }

    public LossGenerator dataLossGenerator()
    {
        return dataLossGenerator;
    }

    public void dataLossGenerator(LossGenerator lossGenerator)
    {
        this.dataLossGenerator = lossGenerator;
    }

    public LossGenerator controlLossGenerator()
    {
        return controlLossGenerator;
    }

    public void controlLossGenerator(LossGenerator lossGenerator)
    {
        this.controlLossGenerator = lossGenerator;
    }

    public Properties properties()
    {
        return properties;
    }

    public void properties(final Properties properties)
    {
        this.properties = properties;
    }

    /*
    Argument parsing code
     */

    /**
     * Helper function to be used as a spy for properties file input stream.
     *
     * @param filename The properties file
     * @return InputStream for reading the file.
     * @throws FileNotFoundException
     */
    InputStream newFileInputStream(final String filename) throws FileNotFoundException
    {
        return new FileInputStream(filename);
    }

    private Properties parseProperties(final String arg) throws ParseException
    {
        final Properties p = new Properties();

        // InputStream is AutoClosable
        try (final InputStream inputStream = newFileInputStream(arg))
        {
            p.load(inputStream);
        }
        catch (final IOException ex)
        {
            throw new ParseException(ex.getMessage());
        }

        return p;
    }

    private IdleStrategy parseIdleStrategy(final String arg) throws ParseException
    {
        if (arg == null || arg.equalsIgnoreCase(NULL_VALUE))
        {
            return null;
        }
        IdleStrategy strategy;
        if (arg.startsWith(BackoffIdleStrategy.class.getName()))
        {
            // this is a special case for BackoffIdleStrategy that allows us to pass in
            // the constructor parameters. At some point, we may want to add a generic way to
            // do this, but for now BackoffIdleStrategy gets special treatment.
            strategy = parseBackoffIdleStrategy(arg);
        }
        else
        {
            // Use reflection to create the new IdleStrategy with no parameters.
            try
            {
                final Class clazz = Class.forName(arg);
                strategy = (IdleStrategy)clazz.newInstance();
            }
            catch (final ClassNotFoundException | IllegalAccessException | InstantiationException | ClassCastException ex)
            {
                throw new ParseException("Error creating new instance of '" + arg + "': " + ex.getMessage());
            }
        }

        return strategy;
    }

    // Generates a new BackoffIdleStrategy with default parameters, or parsed parameters when present.
    private IdleStrategy parseBackoffIdleStrategy(final String arg) throws ParseException
    {
        final int openParenIndex = arg.indexOf("(");
        long maxSpins = Configuration.AGENT_IDLE_MAX_SPINS;
        long maxYields = Configuration.AGENT_IDLE_MAX_YIELDS;
        long minParkPeriodNs = Configuration.AGENT_IDLE_MIN_PARK_NS;
        long maxParkPeriodNs = Configuration.AGENT_IDLE_MAX_PARK_NS;

        if (openParenIndex != -1)
        {
            if (!arg.endsWith(")"))
            {
                throw new ParseException("A BackoffIdleStrategy with parameters has no matching closing parenthesis.");
            }
            // The parameters to the backoff strategy are added just like they would be hardcoded in java.
            // i.e. "uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy(1, 1, 1, 1)"
            final String[] params = arg.substring(openParenIndex + 1, arg.length() - 1).split(",");
            if (params.length != 4)
            {
                throw new ParseException("A BackoffIdleStrategy must have all 4 parameters separated by commas.");
            }
            maxSpins = parseLong(params[0].trim());
            maxYields = parseLong(params[1].trim());
            minParkPeriodNs = parseLong(params[2].trim());
            maxParkPeriodNs = parseLong(params[3].trim());
        }

        return newBackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    }

    private LossGenerator parseLossGenerator(final String arg) throws ParseException
    {
        if (arg == null || arg.equalsIgnoreCase(NULL_VALUE))
        {
            return null;
        }
        LossGenerator lossGenerator;
        try
        {
            final Class clazz = Class.forName(arg);
            lossGenerator = (LossGenerator)clazz.newInstance();
        }
        catch (final ClassNotFoundException | IllegalAccessException | InstantiationException | ClassCastException ex)
        {
            throw new ParseException("Error creating new instance of '" + arg + "': " + ex.getMessage());
        }

        return lossGenerator;
    }

    // Broken out into simple method for testing.
    IdleStrategy newBackoffIdleStrategy(
        final long maxSpins,
        final long maxYields,
        final long minParkPeriodNs,
        final long maxParkPeriodNs)
    {
        return new BackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    }

    private long parseLong(final String longValue) throws ParseException
    {
        long value;
        try
        {
            value = Long.parseLong(longValue);
        }
        catch (final NumberFormatException ex)
        {
            throw new ParseException("Could not parse '" + longValue + "' as a long value.");
        }

        return value;
    }
}
