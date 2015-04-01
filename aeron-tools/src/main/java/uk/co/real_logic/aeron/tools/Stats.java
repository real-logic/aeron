package uk.co.real_logic.aeron.tools;

import static java.nio.ByteOrder.nativeOrder;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.common.CncFileDescriptor;
import uk.co.real_logic.aeron.common.CommonContext;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;

/**
 * Layout of the counter stats:
 * Pos: Label
 * 0: Bytes sent
 * 1: Bytes received
 * 2: Failed offers to ReceiverProxy
 * 3: Failed offers to SenderProxy
 * 4: Failed offers to DriverConductorProxy
 * 5: NAKs sent
 * 6: NAKs received
 * 7: SMs sent
 * 8: SMs received
 * 9: Heartbeats sent
 * 10: Retransmits sent
 * 11: Flow control under runs
 * 12: FLow control over runs
 * 13: Invalid packets
 * 14: Driver Exceptions
 * 15: Data Frame short sends
 * 16: Setup Frame short sends
 * 17: NAK Frame short sends
 * 18: SM Frame short sends
 * 19: Client Keep Alives
 */

public class Stats
{
  private CommonContext context = null;
  private File cncFile = null;
  private MappedByteBuffer cncByteBuffer = null;
  private DirectBuffer metaDataBuffer = null;
  private int cncVersion;
  private AtomicBuffer labelsBuffer = null;
  private AtomicBuffer valuesBuffer = null;
  private CountersManager countersManager = null;
  private AtomicBoolean running = null;
  private StatsOutput output = null;

  private static final int LABEL_SIZE = 1024;
  private static final int NUM_BASE_STATS = 20;
  private static final int UNREGISTERED_LABEL_SIZE = -1;

  public Stats(StatsOutput output, String dirName) throws Exception
  {
    if (output == null)
    {
      this.output = new StatsConsoleOutput();
    }
    else
    {
      this.output = output;
    }

    if (dirName == null)
    {
      cncFile = CommonContext.newDefaultCncFile();
    }
    else
    {
      context = new CommonContext().dirName(dirName).conclude();
      cncFile = context.cncFile();
    }

    cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
    metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
    cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

    if (CncFileDescriptor.CNC_VERSION != cncVersion)
    {
      throw new IllegalStateException("CNC version not understood: version = " + cncVersion);
    }

    labelsBuffer = CncFileDescriptor.createCounterLabelsBuffer(cncByteBuffer, metaDataBuffer);
    valuesBuffer = CncFileDescriptor.createCounterValuesBuffer(cncByteBuffer, metaDataBuffer);

    countersManager = new CountersManager(labelsBuffer, valuesBuffer);
  }

  public void collectStats() throws Exception
  {
    String[] keys = null;
    long[] vals = null;
    int size;
    int idx;

    if (output instanceof StatsNetstatOutput)
    {
      idx = NUM_BASE_STATS;
      ArrayList<String> tmpKeys = new ArrayList<String>();
      ArrayList<Long> tmpVals = new ArrayList<Long>();

      while ((size = isValid(idx)) != 0)
      {
        if (size != UNREGISTERED_LABEL_SIZE)
        {
          tmpKeys.add(getLabel(idx));
          tmpVals.add(getValue(idx));
        }
        idx++;
      }

      keys = tmpKeys.toArray(new String[tmpKeys.size()]);
      vals = new long[tmpVals.size()];
      for (int i = 0; i < vals.length; i++)
      {
        vals[i] = tmpVals.get(i).longValue();
      }
    }
    else if (output instanceof StatsConsoleOutput)
    {
      ArrayList<String> tmpKeys = new ArrayList<String>();
      ArrayList<Long> tmpVals = new ArrayList<Long>();
      idx = 0;

      while ((size = isValid(idx)) != 0)
      {
        if (size != UNREGISTERED_LABEL_SIZE)
        {
          tmpKeys.add(getLabel(idx));
          tmpVals.add(getValue(idx));
        }
        idx++;
      }

      keys = tmpKeys.toArray(new String[tmpKeys.size()]);
      vals = new long[tmpVals.size()];
      for (int i = 0; i < vals.length; i++)
      {
        vals[i] = tmpVals.get(i).longValue();
      }
    }
    else if (output instanceof StatsVMStatOutput || output instanceof StatsCSVOutput)
    {
      keys = new String[NUM_BASE_STATS];
      vals = new long[NUM_BASE_STATS];

      for (idx = 0; idx < NUM_BASE_STATS; idx++)
      {
        if ((size = isValid(idx)) != 0)
        {
          if (size != UNREGISTERED_LABEL_SIZE)
          {
            keys[idx] = getLabel(idx);
            vals[idx] = getValue(idx);
          }
        }
      }
    }
    output.format(keys, vals);
  }

  public void close() throws Exception
  {
    output.close();
  }

  private int isValid(int idx)
  {
    return labelsBuffer.getInt(idx * LABEL_SIZE);
  }

  private String getLabel(int idx)
  {
    return labelsBuffer.getStringUtf8(idx * LABEL_SIZE, nativeOrder());
  }

  private long getValue(int idx)
  {
    int offset = CountersManager.counterOffset(idx);
    return valuesBuffer.getLongVolatile(offset);
  }
}
