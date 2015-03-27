package uk.co.real_logic.aeron.tools.log_analysis;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.activeTermId;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.defaultFrameHeaders;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.initialTermId;

import java.io.File;
import java.util.Observable;

import uk.co.real_logic.aeron.LogBuffers;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class LogModel extends Observable
{
    private static final int LOG_TYPE_STATS = 1;
    private static final int LOG_TYPE_PUB = 2;
    private static final int LOG_TYPE_SUB = 3;

    private int logType = 0;
    private int initialTermId;
    private int activeTermId;
    private int termLength;
    private UnsafeBuffer[] defaultFrameHeaders = null;
    private int selectedFrameHeader = -1;
    private DriverStats stats = null;

    public LogModel()
    {

    }

    public void processStatsBuffer()
    {
        logType = LOG_TYPE_STATS;
        if (stats == null)
        {
            stats = new DriverStats();
        }
        stats.populate();
        setChanged();
        notifyObservers();
    }

    public void processLogBuffer(String path)
    {
        logType = LOG_TYPE_PUB;

        try
        {
            LogBuffers logBuffers = new LogBuffers(path);
            UnsafeBuffer[] atomicBuffers = logBuffers.atomicBuffers();
            DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
            UnsafeBuffer logMetaDataBuffer = atomicBuffers[PARTITION_COUNT * 2];
            initialTermId = initialTermId(logMetaDataBuffer);
            activeTermId = activeTermId(logMetaDataBuffer);
            termLength = atomicBuffers[0].capacity();

            defaultFrameHeaders = defaultFrameHeaders(logMetaDataBuffer);

              setChanged();
            notifyObservers();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        setChanged();
        notifyObservers();
    }

    public DriverStats getStats()
    {
        return stats;
    }

    public int getLogType()
    {
        return logType;
    }

    public int getInitialTermId()
    {
        return initialTermId;
    }

    public int getActiveTermId()
    {
        return activeTermId;
    }

    public int getTermLength()
    {
        return termLength;
    }

    public UnsafeBuffer[] getDefaultFrameHeaders()
    {
        return defaultFrameHeaders;
    }

    public UnsafeBuffer getDefaultFrameHeader(int idx)
    {
        return defaultFrameHeaders[idx];
    }

    public void setFile(File file)
    {

    }
}
