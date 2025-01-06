/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.exceptions.AeronException;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CncFileDescriptor.*;
import static io.aeron.samples.SamplesUtil.mapExistingFileReadOnly;

/**
 * Reader for Aeron CnC file represented by {@link CncFileDescriptor} which can be used for observability.
 */
public final class CncFileReader implements AutoCloseable
{
    private boolean isClosed = false;
    private final int driverPid;
    private final int cncVersion;
    private final String cncSemanticVersion;
    private final MappedByteBuffer cncByteBuffer;
    private final CountersReader countersReader;
    private final UnsafeBuffer toDriverBuffer;

    private CncFileReader(final MappedByteBuffer cncByteBuffer)
    {
        this.cncByteBuffer = cncByteBuffer;

        final DirectBuffer cncMetaDataBuffer = createMetaDataBuffer(cncByteBuffer);
        driverPid = cncMetaDataBuffer.getInt(pidOffset(0));
        final int cncVersion = cncMetaDataBuffer.getInt(cncVersionOffset(0));

        try
        {
            checkVersion(cncVersion);
        }
        catch (final AeronException e)
        {
            BufferUtil.free(cncByteBuffer);
            throw e;
        }

        this.cncVersion = cncVersion;
        this.cncSemanticVersion = SemanticVersion.toString(cncVersion);

        this.toDriverBuffer = CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer);

        this.countersReader = new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer),
            createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer),
            StandardCharsets.US_ASCII);
    }

    /**
     * Map an existing CnC file.
     *
     * @return the {@link CncFileReader} wrapper for reading useful data from the cnc file.
     * @throws AeronException if the cnc version major version is not compatible.
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
     * @return the counters' reader.
     */
    public CountersReader countersReader()
    {
        return countersReader;
    }

    /**
     * Get the epoch timestamp (ms) of the last driver heartbeat.
     *
     * @return the epoch timestamp (ms) of the last driver heartbeat.
     */
    public long driverHeartbeatMs()
    {
        final int timestampOffset = (toDriverBuffer.capacity() - RingBufferDescriptor.TRAILER_LENGTH) +
            RingBufferDescriptor.CONSUMER_HEARTBEAT_OFFSET;

        return toDriverBuffer.getLongVolatile(timestampOffset);
    }

    /**
     * Return media driver PID that is stored in the CnC file.
     *
     * @return media driver PID.
     */
    public int driverPid()
    {
        return driverPid;
    }

    /**
     * Get the number of milliseconds since the last driver heartbeat.
     *
     * @return the number of milliseconds since the last driver heartbeat.
     */
    public long driverHeartbeatAgeMs()
    {
        return System.currentTimeMillis() - driverHeartbeatMs();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            BufferUtil.free(cncByteBuffer);
        }
    }
}
