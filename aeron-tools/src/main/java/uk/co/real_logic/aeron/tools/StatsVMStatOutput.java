package uk.co.real_logic.aeron.tools;

public class StatsVMStatOutput implements StatsOutput
{
  private int iterations;
  private String titles[] =
  {
    "Bytes", "Failed Offers", "NAKs", "SMs", "Heartbeats", "RX", "Flow Control", "Invalid", "Driver", "Short Sends", "Keep"
  };
  private String subTitles[] =
  {
    "In/Out", "RP/SP/DCP", "In/Out", "In/Out", "", "", "Under/Over", "Packets", "Exceptions", "DF/SF/NF/SMF", "Alives"
  };

  public StatsVMStatOutput()
  {
    iterations = 0;
  }

  public void format(String[] keys, long[] vals) throws Exception
  {
    if (iterations % 20 == 0)
    {
      for (int i = 0; i < titles.length; i++)
      {
        System.out.format("%1$-15s", titles[i]);
      }
      System.out.println();
      for (int i = 0; i < titles.length; i++)
      {
        System.out.format("%1$-15s", subTitles[i]);
      }
      System.out.println();
    }
    System.out.format("%-15s", vals[1] + "/" + vals[0]);
    System.out.format("%-15s", vals[2] + "/" + vals[3] + "/" + vals[4]);
    System.out.format("%-15s", vals[6] + "/" + vals[5]);
    System.out.format("%-15s", vals[8] + "/" + vals[7]);
    System.out.format("%-15s", vals[9]);
    System.out.format("%-15s", vals[10]);
    System.out.format("%-15s", vals[11] + "/" + vals[12]);
    System.out.format("%-15s", vals[13]);
    System.out.format("%-15s", vals[14]);
    System.out.format("%-15s", vals[15] + "/" + vals[16] + "/" + vals[17] + "/" + vals[18]);
    System.out.format("%-15s", vals[19]);
    System.out.println();
    iterations++;
  }

  public void close() throws Exception
  {

  }
}
