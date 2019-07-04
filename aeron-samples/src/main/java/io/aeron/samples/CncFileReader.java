package io.aeron.samples;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;

import static io.aeron.CncFileDescriptor.*;
import static io.aeron.samples.SamplesUtil.mapExistingFileReadOnly;

/**
 * A utility class for interpreting the cnc file.
 */
public final class CncFileReader implements AutoCloseable
{
    private final MappedByteBuffer cncByteBuffer;
    private final int cncVersion;
    private final CountersReader countersReader;
    private final ManyToOneRingBuffer toDriverBuffer;
    private final String cncSemanticVersion;

    private CncFileReader(final MappedByteBuffer cncByteBuffer)
    {
        this.cncByteBuffer = cncByteBuffer;

        final DirectBuffer cncMetaDataBuffer = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(cncVersionOffset(0));

        try
        {
            checkVersion(cncVersion);
        }
        catch (final AeronException e)
        {
            IoUtil.unmap(cncByteBuffer);
            throw e;
        }

        this.cncVersion = cncVersion;
        this.cncSemanticVersion = SemanticVersion.toString(cncVersion);

        this.toDriverBuffer = new ManyToOneRingBuffer(
            CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

        this.countersReader = new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer),
            createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
    }

    /**
     * Map an existing CnC file.
     *
     * @throws AeronException if the cnc version major version is not compatible.
     * @return the {@link CncFileReader} wrapper for reading useful data from the cnc file.
     */
    public static CncFileReader map()
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        final MappedByteBuffer cncByteBuffer = mapExistingFileReadOnly(cncFile);
        return new CncFileReader(cncByteBuffer);
    }

    /**
     * Get the cnc version.
     *
     * @return the cnc version.
     */
    public int cncVersion()
    {
        return cncVersion;
    }

    /**
     * Get the cnc semantic version.
     *
     * @return the cnc semantic version.
     */
    public String semanticVersion()
    {
        return cncSemanticVersion;
    }

    /**
     * Get the counters reader for querying counter values.
     *
     * @return the counters reader.
     */
    public CountersReader countersReader()
    {
        return countersReader;
    }

    /**
     * Get the timestamp (ms) of the last driver heartbeat.
     *
     * @return the timestamp (ms) of the last driver heartbeat.
     */
    public long driverHeartbeat()
    {
        return toDriverBuffer.consumerHeartbeatTime();
    }

    /**
     * Get the number of milliseconds since the last driver heartbeat.
     *
     * @return the number of milliseconds since the last driver heartbeat.
     */
    public long driverHeartbeatAge()
    {
        return System.currentTimeMillis() - driverHeartbeat();
    }

    @Override
    public void close()
    {
        IoUtil.unmap(cncByteBuffer);
    }
}
