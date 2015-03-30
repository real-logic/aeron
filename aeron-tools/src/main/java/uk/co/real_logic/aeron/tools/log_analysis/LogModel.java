package uk.co.real_logic.aeron.tools.log_analysis;

import java.io.File;
//import java.util.Observable;

import uk.co.real_logic.aeron.LogBuffers;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.*;
import static uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class LogModel
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
    }

    public void processLogBuffer(String path)
    {
        logType = LOG_TYPE_PUB;

        try
        {
            int messageDumpLimit = 100;
            LogBuffers logBuffers = new LogBuffers(path);
            UnsafeBuffer[] atomicBuffers = logBuffers.atomicBuffers();
            DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
            UnsafeBuffer logMetaDataBuffer = atomicBuffers[PARTITION_COUNT * 2];
            termLength = atomicBuffers[0].capacity();
            System.out.format("Initial term id: %d\n", initialTermId(logMetaDataBuffer));
            System.out.format(" Active term id: %d\n", activeTermId(logMetaDataBuffer));
            System.out.format("   Active Index: %d\n", indexByTerm(initialTermId(logMetaDataBuffer),
                    activeTermId(logMetaDataBuffer)));
            System.out.format("    Term Length: %d\n", termLength);

            initialTermId = initialTermId(logMetaDataBuffer);
            activeTermId = activeTermId(logMetaDataBuffer);


            defaultFrameHeaders = defaultFrameHeaders(logMetaDataBuffer);
            for (int i = 0; i < defaultFrameHeaders.length; i++)
            {
                dataHeaderFlyweight.wrap(defaultFrameHeaders[i]);
                System.out.format("Index %d Default %s\n", i, dataHeaderFlyweight);
            }

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                UnsafeBuffer metaDataBuffer = atomicBuffers[i + PARTITION_COUNT];
                System.out.format("Index %d Term Meta Data status=%s tail=%d\n",
                        i, termStatus(metaDataBuffer),
                        metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET));
            }

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                System.out.format("Index %d Term Data\n\n", i);
                UnsafeBuffer termBuffer = logBuffers.atomicBuffers()[i];
                dataHeaderFlyweight.wrap(termBuffer);

                int offset = 0;
                do
                {
                    dataHeaderFlyweight.offset(offset);
                    System.out.println(dataHeaderFlyweight.toString());

                    int frameLength = dataHeaderFlyweight.frameLength();
                    if (frameLength == 0)
                    {
                        int limit = Math.min(termLength - (offset + HEADER_LENGTH), messageDumpLimit);
                        System.out.println(bytesToHex(termBuffer, offset + HEADER_LENGTH, limit));
                        break;
                    }

                    int limit = Math.min(frameLength - HEADER_LENGTH, messageDumpLimit);
                    System.out.println(bytesToHex(termBuffer, offset + HEADER_LENGTH, limit));

                    offset += BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                }
                while (offset < termLength);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private String termStatus(UnsafeBuffer metaDataBuffer)
    {
        int status = metaDataBuffer.getInt(TERM_STATUS_OFFSET);
        switch (status)
        {
            case CLEAN:
                return "CLEAN";

            case NEEDS_CLEANING:
                return "NEEDS_CLEANING";

            default:
                return status + " <UNKNOWN>";
        }
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private char[] bytesToHex(DirectBuffer buffer, final int offset, final int length)
    {
        final char[] hexChars = new char[length * 2];

        for (int i = 0; i < length; i++)
        {
            int b = buffer.getByte(offset + i) & 0xFF;
            hexChars[i * 2] = HEX_ARRAY[b >>> 4];
            hexChars[i * 2 + 1] = HEX_ARRAY[b & 0x0F];
        }

        return hexChars;
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
