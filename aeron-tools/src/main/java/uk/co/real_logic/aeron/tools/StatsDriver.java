package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.*;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatsDriver
{
  private Options options;
  private StatsOutput output = null;
  private Stats stats = null;
  private String file = null;
  private AtomicBoolean running = null;

  public StatsDriver(String[] args)
  {
    try
    {
      parseArgs(args);
      running = new AtomicBoolean(true);
      stats = new Stats(output, null);

      Runnable task = new Runnable()
      {
        public void run()
        {
          try
          {
            while (running.get())
            {
              stats.collectStats();
              Thread.sleep(1000);
            }
            stats.close();
          }
          catch (Exception e)
          {
            e.printStackTrace();
          }
        }
      };
      Thread worker = new Thread(task);
      worker.start();

      SigInt.register(() -> running.set(false));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public void parseArgs(String[] args) throws ParseException
  {
    options = new Options();
    options.addOption(null, "vmstat", false, "Format transport stats in vmstat format.");
    options.addOption(null, "console", false, "Dump raw stats to the console.");
    options.addOption(null, "netstat", false, "Format channel info in netstat format.");
    options.addOption(null, "csv", false, "Format transport stats as comma separated values.");
    options.addOption(null, "file", true, "Output file for csv format stats.");
    options.addOption("h", "help", false, "Display help message.");

    CommandLineParser parser = new GnuParser();
    CommandLine command = parser.parse(options, args);

    String opt;

    if (command.hasOption("help"))
    {
      System.out.println("Help Message");
      System.exit(0);
    }

    /** Default is console output **/
    output = new StatsConsoleOutput();

    if (command.hasOption("vmstat"))
    {
      output = new StatsVMStatOutput();
    }
    else if (command.hasOption("console"))
    {
      output = new StatsConsoleOutput();
    }
    else if (command.hasOption("netstat"))
    {
      output = new StatsNetstatOutput();
    }
    else if (command.hasOption("csv"))
    {
      if (command.hasOption("file"))
      {
        file = command.getOptionValue("file", null);
      }
      output = new StatsCSVOutput(file);
    }
  }

  public static void main(String[] args)
  {
    new StatsDriver(args);
  }
}
