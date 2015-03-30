package uk.co.real_logic.aeron.tools;

import java.util.Arrays;

import java.awt.Color;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.*;

public class PingRunner
{
  private int numMsgs = 10000000;
  private int numWarmupMsgs = 100000;
  private int msgLen = 64;
  private long[] rtts = null;
  private double[] sortedRTT = null;
  private double[] tmp = null;
  private int idx;
  private boolean warmedUp = false;
  private PingImpl impl = null;
  private String transport = "";

  public PingRunner(String[] args)
  {
    rtts = new long[numMsgs];
    idx = 0;

    if (args[0].equalsIgnoreCase("aeron"))
    {
      impl = new AeronPing();
    }
    else if (args[0].equalsIgnoreCase("aeron-claim"))
    {
      impl = new AeronClaimPing();
    }

    transport = args[0];
    impl.prepare();
    impl.connect();
    run();
    impl.shutdown();
    printStats();
  }

  private void run()
  {
    // Send warm-up messages
    for (int i = 0; i < numWarmupMsgs; i++)
    {
      impl.sendPingAndReceivePong(msgLen);
    }

    for (int i = 0; i < numMsgs; i++)
    {
      rtts[i] = impl.sendPingAndReceivePong(msgLen);
    }
  }

  private void printStats()
  {
    double sum = 0.0;
    double max = 0;
    double min = Long.MAX_VALUE;
    int maxIdx = 0;
    int minIdx = 0;
    double mean = 0.0;
    double stdDev = 0.0;

    tmp = new double[rtts.length];

    for (int i = 0; i < rtts.length; i++)
    {
      tmp[i] = rtts[i] / 1000.0;
    }

    sortedRTT = new double[rtts.length];
    System.arraycopy(tmp, 0, sortedRTT, 0, tmp.length);
    Arrays.sort(sortedRTT);

    for (int i = 0; i < numMsgs; i++)
    {
      if (tmp[i] > max)
      {
        max = tmp[i];
        maxIdx = i;
      }
      if (tmp[i] < min)
      {
        min = tmp[i];
        minIdx = i;
      }
      sum += (double)(tmp[i]);
    }

    mean = sum / numMsgs;
    sum = 0;
    for (int i = 0; i < numMsgs; i++)
    {
      sum += Math.pow(mean - tmp[i], 2);
    }
    stdDev = Math.sqrt(sum / numMsgs);

    /*generateScatterPlot(min, max, .9);
    generateScatterPlot(min, max, .95);
    generateScatterPlot(min, max, .98);*/
    generateScatterPlot(min, max, .99, mean, stdDev);
    generateScatterPlot(min, max, .999, mean, stdDev);
    generateScatterPlot(min, max, .9999, mean, stdDev);
    generateScatterPlot(min, max, .99999, mean, stdDev);
    generateScatterPlot(min, max, .999999, mean, stdDev);

    System.out.format("Mean: %.3fus\n", mean);
    System.out.format("Std Dev: %.3fus\n", stdDev);
    System.out.format("Min: %.3fus Index %d\n", min, minIdx);
    System.out.format("Max: %.3fus. Index %d\n", max, maxIdx);
    System.exit(0);
  }

  private void generateScatterPlot(double min, double max, double percentile, double mean, double stdDev)
  {
    BufferedImage image = new BufferedImage(500, 400, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g2 = image.createGraphics();
    FontMetrics fm = g2.getFontMetrics();
    String filename = transport + "_scatterplot_" + percentile + ".png";
    File imageFile = new File(filename);
    int width = 390;
    int height = 370;
    int num = (int)((numMsgs - 1) * percentile);
    double newMax = sortedRTT[num];
    double stepY = (double)(height / (double)(newMax));
    double stepX = (double)width / numMsgs;

    g2.setColor(Color.white);
    g2.fillRect(0, 0, 500, 400);
    g2.setColor(Color.black);

    g2.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
    g2.drawString("Latency ScatterPlot (microseconds)", 250 - fm.stringWidth("Latency ScatterPlot (microseconds)") / 2, 20);
    g2.drawString("" + newMax, 10, 20);
    g2.drawLine(100, 20, 100, 390);
    g2.drawLine(100, 390, 490, 390);

    g2.setColor(Color.red);
    for (int i = 0; i < numMsgs; i++)
    {
      if (tmp[i] < newMax)
      {
        int posX = 100 + (int)(stepX * i);
        int posY = 390 - (int)(stepY * (tmp[i]));
        g2.fillRect(posX, posY, 1, 1);
      }
    }

    g2.setColor(Color.green);
    g2.drawLine(100, 390 - (int)(stepY * mean), 490, 390 - (int)(stepY * mean));

    g2.setColor(Color.blue);
    g2.drawLine(100, 390 - (int)(stepY * (mean - stdDev)), 490, 390 - (int)(stepY * (mean - stdDev)));
    g2.drawLine(100, 390 - (int)(stepY * (mean + stdDev)), 490, 390 - (int)(stepY * (mean + stdDev)));

    try
    {
      ImageIO.write(image, "png", imageFile);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public static void main(String[] args)
  {
    new PingRunner(args);
  }
}
