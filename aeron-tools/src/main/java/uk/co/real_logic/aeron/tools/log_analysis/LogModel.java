package uk.co.real_logic.aeron.tools.log_analysis;

import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.CLEAN;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.NEEDS_CLEANING;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_STATUS_OFFSET;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.TERM_TAIL_COUNTER_OFFSET;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.activeTermId;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.defaultFrameHeaders;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.indexByTerm;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.initialTermId;
import static uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight.HEADER_LENGTH;

import java.io.File;
//import java.util.Observable;

import uk.co.real_logic.aeron.LogBuffers;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

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
    private final int selectedFrameHeader = -1;
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

    public void processLogBuffer(final String path)
    {
        logType = LOG_TYPE_PUB;

        try
        {
            final int messageDumpLimit = 100;
            final LogBuffers logBuffers = new LogBuffers(path);
            final UnsafeBuffer[] atomicBuffers = logBuffers.atomicBuffers();
            final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
            final UnsafeBuffer logMetaDataBuffer = atomicBuffers[PARTITION_COUNT * 2];
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
                final UnsafeBuffer metaDataBuffer = atomicBuffers[i + PARTITION_COUNT];
                System.out.format("Index %d Term Meta Data status=%s tail=%d\n",
                        i, termStatus(metaDataBuffer),
                        metaDataBuffer.getInt(TERM_TAIL_COUNTER_OFFSET));
            }

            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                System.out.format("Index %d Term Data\n\n", i);
                final UnsafeBuffer termBuffer = logBuffers.atomicBuffers()[i];
                dataHeaderFlyweight.wrap(termBuffer);

                int offset = 0;
                do
                {
                    dataHeaderFlyweight.offset(offset);
                    System.out.println(dataHeaderFlyweight.toString());

                    final int frameLength = dataHeaderFlyweight.frameLength();
                    if (frameLength == 0)
                    {
                        final int limit = Math.min(termLength - (offset + HEADER_LENGTH), messageDumpLimit);
                        System.out.println(bytesToHex(termBuffer, offset + HEADER_LENGTH, limit));
                        break;
                    }

                    final int limit = Math.min(frameLength - HEADER_LENGTH, messageDumpLimit);
                    System.out.println(bytesToHex(termBuffer, offset + HEADER_LENGTH, limit));

                    offset += BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
                }
                while (offset < termLength);
            }
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    private String termStatus(final UnsafeBuffer metaDataBuffer)
    {
        final int status = metaDataBuffer.getInt(TERM_STATUS_OFFSET);
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

    private char[] bytesToHex(final DirectBuffer buffer, final int offset, final int length)
    {
        final char[] hexChars = new char[length * 2];

        for (int i = 0; i < length; i++)
        {
            final int b = buffer.getByte(offset + i) & 0xFF;
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

    public UnsafeBuffer getDefaultFrameHeader(final int idx)
    {
        return defaultFrameHeaders[idx];
    }

    public void setFile(final File file)
    {

    }
}
