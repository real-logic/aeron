package io.aeron.driver.reports;

import io.aeron.CncFileDescriptor;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;

import java.nio.MappedByteBuffer;

public class ErrorReportUtil
{
    public static AtomicBuffer mapErrorLogBuffer(final MappedByteBuffer cncByteBuffer)
    {
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);

        return CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
    }
}
