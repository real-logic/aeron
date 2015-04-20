package uk.co.real_logic.aeron.tools;

import java.io.File;
import java.io.FileWriter;

public class StatsCSVOutput implements StatsOutput
{
  public static final String DEFAULT_FILE = "stats.csv";

  private String file;
  private FileWriter out;
  private boolean firstTime = true;

  public StatsCSVOutput(final String file)
  {
    if (file != null)
    {
      this.file = file;
    }
    else
    {
      this.file = DEFAULT_FILE;
    }

    try
    {
      System.out.println("Output file: " + this.file);
      final File outFile = new File(this.file);
      outFile.createNewFile();
      out = new FileWriter(outFile);
    }
    catch (final Exception e)
    {
      e.printStackTrace();
    }
  }

  @Override
public void format(final String[] keys, final long[] vals) throws Exception
  {
    if (firstTime)
    {
      for (int i = 0; i < keys.length - 1; i++)
      {
        out.write(keys[i] + ",");
      }
      out.write(keys[keys.length - 1] + "\n");
      firstTime = false;
    }

    for (int i = 0; i < vals.length - 1; i++)
    {
      out.write(vals[i] + ",");
    }
    out.write(vals[vals.length - 1] + "\n");
    out.flush();
  }

  @Override
public void close() throws Exception
  {
    out.close();
  }
}
