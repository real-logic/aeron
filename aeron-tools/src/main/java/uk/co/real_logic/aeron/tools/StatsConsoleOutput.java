package uk.co.real_logic.aeron.tools;

import java.util.Date;

public class StatsConsoleOutput implements StatsOutput
{
  public StatsConsoleOutput()
  {

  }

  public void format(String[] keys, long[] vals) throws Exception
  {
    System.out.print("\033[H\033[2J");
    System.out.format("%1$tH:%1$tM:%1$tS - Aeron Stats\n", new Date());
    System.out.println("===============================");

    for (int i = 0; i < keys.length; i++)
    {
      System.out.println(keys[i] + ": " + vals[i]);
    }
  }

  public void close() throws Exception
  {

  }
}
