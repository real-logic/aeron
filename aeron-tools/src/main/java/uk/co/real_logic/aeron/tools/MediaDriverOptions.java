package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.*;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by bhorst on 4/10/15.
 */
public class MediaDriverOptions
{
    // Command line options for media driver
    private Options options;

    // Properties containing Aeron settings.
    private Properties properties;

    // The 5 idle strategies available for the Media Driver
    private IdleStrategy conductorIdleStrategy;
    private IdleStrategy senderIdleStrategy;
    private IdleStrategy receiverIdleStrategy;
    private IdleStrategy sharedNetworkIdleStrategy;
    private IdleStrategy sharedIdleStrategy;

    public MediaDriverOptions()
    {
        options = new Options()
                .addOption("p", "properties", true, "A properties file containing Aeron options.")
                .addOption("h", "help", false, "Display help message.")
                .addOption("c", "conductor", true, "IdleStrategy class for the conductor thread.")
                .addOption("s", "sender", true, "IdleStrategy class for the sender thread.")
                .addOption("r", "receiver", true, "IdleStrategy class for the receiver thread.")
                .addOption("n", "network", true, "IdleStrategy class for the shared network thread.")
                .addOption("a", "shared", true, "IdleStrategy class for the shared thread.");

        // Idle strategies are null by default (Aeron should use its default if we pass null)
        conductorIdleStrategy = null;
        senderIdleStrategy = null;
        receiverIdleStrategy = null;
        sharedNetworkIdleStrategy = null;
        sharedIdleStrategy = null;
        properties = null;
    }

    /**
     * Parse command line arguments for MediaDriverTool and call the setter functions.
     * @param args Command line arguments.
     * @return 0 on success, 1 if application should call {@link #printHelp(String)}
     * @throws org.apache.commons.cli.ParseException On string parsing error.
     */
    public int parseArgs(String[] args) throws ParseException
    {
        CommandLineParser parser = new GnuParser();
        CommandLine command = parser.parse(options, args);

        if (command.hasOption("help"))
        {
            // Inform application the user wants help message.
            return 1;
        }

        if (command.hasOption("properties"))
        {
            this.properties = parseProperties(command.getOptionValue("properties"));
        }
        String defaultValue;
        defaultValue = properties == null ? "null" : properties.getProperty("aeron.tools.mediadriver.sender");
        senderIdleStrategy = parseIdleStrategy(command.getOptionValue("sender", defaultValue));

        defaultValue = properties == null ? "null" : properties.getProperty("aeron.tools.mediadriver.receiver");
        receiverIdleStrategy = parseIdleStrategy(command.getOptionValue("receiver", defaultValue));

        defaultValue = properties == null ? "null" : properties.getProperty("aeron.tools.mediadriver.network");
        sharedNetworkIdleStrategy = parseIdleStrategy(command.getOptionValue("network", defaultValue));

        defaultValue = properties == null ? "null" : properties.getProperty("aeron.tools.mediadriver.shared");
        sharedIdleStrategy = parseIdleStrategy(command.getOptionValue("shared", defaultValue));

        defaultValue = properties == null ? "null" : properties.getProperty("aeron.tools.mediadriver.conductor");
        conductorIdleStrategy = parseIdleStrategy(command.getOptionValue("conductor", defaultValue));
        return 0;
    }

    public void printHelp(String applicationName)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(applicationName + " [options]", options);
    }

    public IdleStrategy getConductorIdleStrategy()
    {
        return conductorIdleStrategy;
    }

    public void setConductorIdleStrategy(IdleStrategy conductorIdleStrategy)
    {
        this.conductorIdleStrategy = conductorIdleStrategy;
    }

    public IdleStrategy getSenderIdleStrategy()
    {
        return senderIdleStrategy;
    }

    public void setSenderIdleStrategy(IdleStrategy senderIdleStrategy)
    {
        this.senderIdleStrategy = senderIdleStrategy;
    }

    public IdleStrategy getReceiverIdleStrategy()
    {
        return receiverIdleStrategy;
    }

    public void setReceiverIdleStrategy(IdleStrategy receiverIdleStrategy)
    {
        this.receiverIdleStrategy = receiverIdleStrategy;
    }

    public IdleStrategy getSharedNetworkIdleStrategy()
    {
        return sharedNetworkIdleStrategy;
    }

    public void setSharedNetworkIdleStrategy(IdleStrategy sharedNetworkIdleStrategy)
    {
        this.sharedNetworkIdleStrategy = sharedNetworkIdleStrategy;
    }

    public IdleStrategy getSharedIdleStrategy()
    {
        return sharedIdleStrategy;
    }

    public void setSharedIdleStrategy(IdleStrategy sharedIdleStrategy)
    {
        this.sharedIdleStrategy = sharedIdleStrategy;
    }

    public Properties getProperties()
    {
        return properties;
    }

    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    /*
    Argument parsing code
     */

    /**
     * Helper function to be used as a spy for properties file input stream.
     * @param filename The properties file
     * @return InputStream for reading the file.
     * @throws FileNotFoundException
     */
    InputStream newFileInputStream(String filename) throws FileNotFoundException
    {
        return new FileInputStream(filename);
    }

    private Properties parseProperties(String arg) throws ParseException
    {
        Properties p = new Properties();
        InputStream inputStream;
        try
        {
            inputStream = newFileInputStream(arg);
        }
        catch (FileNotFoundException ex)
        {
            throw new ParseException("Could not find properties file " + arg);
        }
        try
        {
            p.load(inputStream);
        }
        catch (IOException ex)
        {
            throw new ParseException(ex.getMessage());
        }
        return p;
    }

    private IdleStrategy parseIdleStrategy(String arg) throws ParseException
    {
        if (arg == null || arg.equalsIgnoreCase("NULL"))
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
                Class clazz = Class.forName(arg);
                strategy = (IdleStrategy)clazz.newInstance();
            }
            catch (ClassNotFoundException ex)
            {
                throw new ParseException("Class not found: " + ex.getMessage());
            }
            catch (IllegalAccessException ex)
            {
                throw new ParseException("Illegal access of class '" + arg + "': " + ex.getMessage());
            }
            catch (InstantiationException ex)
            {
                throw new ParseException("Could not instantiate class '" + arg + "': " + ex.getMessage());
            }
        }
        return strategy;
    }

    /* Generates a new BackoffIdleStrategy with default parameters, or parsed parameters when present. */
    private IdleStrategy parseBackoffIdleStrategy(String arg) throws ParseException
    {
        int openParenIndex = arg.indexOf("(");
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
            String[] params = arg.substring(openParenIndex + 1, arg.length() - 1).split(",");
            if (params.length != 4)
            {
                throw new ParseException("A BackoffIdleStrategy must have all 4 parameters separated by commas.");
            }
            maxSpins = parseLong(params[0].trim());
            maxYields = parseLong(params[1].trim());
            minParkPeriodNs = parseLong(params[2].trim());
            maxParkPeriodNs = parseLong(params[3].trim());

        }
        return makeBackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    }

    // Broken out into simple method for testing.
    IdleStrategy makeBackoffIdleStrategy(long maxSpins, long maxYields, long minParkPeriodNs, long maxParkPeriodNs)
    {
        return new BackoffIdleStrategy(maxSpins, maxYields, minParkPeriodNs, maxParkPeriodNs);
    }

    private long parseLong(String longValue) throws ParseException
    {
        long value;
        try
        {
            value = Long.parseLong(longValue);
        }
        catch (NumberFormatException ex)
        {
            throw new ParseException("Could not parse '" + longValue + "' as a long value.");
        }
        return value;
    }
}
