package uk.co.real_logic.aeron.tools.log_analysis;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;

import uk.co.real_logic.aeron.common.CncFileDescriptor;
import uk.co.real_logic.aeron.common.CommonContext;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;

import static java.nio.ByteOrder.nativeOrder;

public class DriverStats
{
    ArrayList<String> labels = null;
    ArrayList<Long> values = null;

  private CommonContext context = null;
    private File cncFile = null;
    private MappedByteBuffer cncByteBuffer = null;
    private DirectBuffer metaDataBuffer = null;
    private int cncVersion;
    private AtomicBuffer labelsBuffer = null;
    private AtomicBuffer valuesBuffer = null;
    private CountersManager countersManager = null;
    private boolean populated = false;

    private static final int LABEL_SIZE = 1024;
    private static final int UNREGISTERED_LABEL_SIZE = -1;

    public DriverStats()
    {
        labels = new ArrayList<String>();
        values = new ArrayList<Long>();
    }

    public String getLabel(int idx)
    {
        return labels.get(idx);
    }

    public boolean populated()
    {
        return populated;
    }

    public long getValue(int idx)
    {
        return values.get(idx);
    }

    public int getNumLabels()
    {
        return labels.size();
    }

    public void populate()
    {
        cncFile = CommonContext.newDefaultCncFile();
        cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException("CNC version not understood");
        }

        labelsBuffer = CncFileDescriptor.createCounterLabelsBuffer(cncByteBuffer, metaDataBuffer);
        valuesBuffer = CncFileDescriptor.createCounterValuesBuffer(cncByteBuffer, metaDataBuffer);

        countersManager = new CountersManager(labelsBuffer, valuesBuffer);

        int size = 0;
        int idx = 0;

        if (populated)
        {
            labels.clear();
            values.clear();
        }

        while ((size = isValid(idx)) != 0)
        {
            if (size != UNREGISTERED_LABEL_SIZE)
            {
                labels.add(readLabel(idx));
                values.add(readValue(idx));
            }
            idx++;
        }
        populated = true;
    }

    public String toString()
    {
        StringBuffer out = new StringBuffer("");

        return out.toString();
    }

    private int isValid(int idx)
    {
        return labelsBuffer.getInt(idx * LABEL_SIZE);
    }

    private long readValue(int idx)
    {
        int offset = CountersManager.counterOffset(idx);
        return valuesBuffer.getLongVolatile(offset);
    }

    private String readLabel(int idx)
    {
        return labelsBuffer.getStringUtf8(idx * LABEL_SIZE, nativeOrder());
    }
}
