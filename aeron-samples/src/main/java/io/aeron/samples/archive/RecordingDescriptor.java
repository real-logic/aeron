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
package io.aeron.samples.archive;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;

/**
 * Class that collects the parameters of the <code>RecordingDescriptor Consumer</code> callback for use with the
 * <code>RecordingDescriptorCollector</code>.
 *
 * @see io.aeron.archive.client.RecordingDescriptorConsumer
 * @see RecordingDescriptorCollector
 */
public class RecordingDescriptor
{
    private long controlSessionId;
    private long correlationId;
    private long recordingId;
    private long startTimestamp;
    private long stopTimestamp;
    private long startPosition;
    private long stopPosition;
    private int initialTermId;
    private int segmentFileLength;
    private int termBufferLength;
    private int mtuLength;
    private int sessionId;
    private int streamId;
    private String strippedChannel;
    private String originalChannel;
    private String sourceIdentity;
    private boolean isRetained = false;

    RecordingDescriptor reset()
    {
        this.controlSessionId = Aeron.NULL_VALUE;
        this.correlationId = Aeron.NULL_VALUE;
        this.recordingId = Aeron.NULL_VALUE;
        this.startTimestamp = AeronArchive.NULL_TIMESTAMP;
        this.stopTimestamp = AeronArchive.NULL_TIMESTAMP;
        this.startPosition = AeronArchive.NULL_POSITION;
        this.stopPosition = AeronArchive.NULL_POSITION;
        this.initialTermId = 0;
        this.segmentFileLength = 0;
        this.termBufferLength = 0;
        this.mtuLength = 0;
        this.sessionId = 0;
        this.streamId = 0;
        this.strippedChannel = null;
        this.originalChannel = null;
        this.sourceIdentity = null;

        return this;
    }

    RecordingDescriptor set(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.initialTermId = initialTermId;
        this.segmentFileLength = segmentFileLength;
        this.termBufferLength = termBufferLength;
        this.mtuLength = mtuLength;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.strippedChannel = strippedChannel;
        this.originalChannel = originalChannel;
        this.sourceIdentity = sourceIdentity;

        return this;
    }

    /**
     * controlSessionId of the originating session requesting to list recordings.
     *
     * @return controlSessionId
     */
    public long controlSessionId()
    {
        return controlSessionId;
    }

    /**
     * correlationId of the associated request to list recordings.
     *
     * @return correlationId
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * recordingId of this recording descriptor.
     *
     * @return recordingId
     */
    public long recordingId()
    {
        return recordingId;
    }

    /**
     * startTimestamp of the recording.
     *
     * @return startTimestamp
     */
    public long startTimestamp()
    {
        return startTimestamp;
    }

    /**
     * stopTimestamp of the recording.
     *
     * @return stopTimestamp
     */
    public long stopTimestamp()
    {
        return stopTimestamp;
    }

    /**
     * startPosition of the recording against the recorded publication, the {@link Image#joinPosition()}.
     *
     * @return startPosition
     */
    public long startPosition()
    {
        return startPosition;
    }

    /**
     * stopPosition reached for the recording, final position for {@link Image#position()}.
     *
     * @return stopPosition
     */
    public long stopPosition()
    {
        return stopPosition;
    }

    /**
     * initialTermId of the recorded stream, {@link Image#initialTermId()}.
     *
     * @return initialTermId
     */
    public int initialTermId()
    {
        return initialTermId;
    }

    /**
     * segmentFileLength of the recording which is a multiple of termBufferLength.
     *
     * @return segmentFileLength
     */
    public int segmentFileLength()
    {
        return segmentFileLength;
    }

    /**
     * termBufferLength of the recorded stream, {@link Image#termBufferLength()}.
     *
     * @return termBufferLength
     */
    public int termBufferLength()
    {
        return termBufferLength;
    }

    /**
     * mtuLength of the recorded stream, {@link Image#mtuLength()}.
     *
     * @return mtuLength
     */
    public int mtuLength()
    {
        return mtuLength;
    }

    /**
     * sessionId of the recorded stream, this will be the most recent session id for extended recordings.
     *
     * @return sessionId
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * streamId of the recorded stream, {@link Subscription#streamId()}.
     *
     * @return streamId
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * strippedChannel of the recorded stream which is used for the recording subscription in the archive.
     *
     * @return strippedChannel
     */
    public String strippedChannel()
    {
        return strippedChannel;
    }

    /**
     * originalChannel of the recorded stream provided to the start recording request, {@link Subscription#channel()}.
     *
     * @return originalChannel
     */
    public String originalChannel()
    {
        return originalChannel;
    }

    /**
     * sourceIdentity of the recorded stream, the {@link Image#sourceIdentity()}.
     *
     * @return sourceIdentity
     */
    public String sourceIdentity()
    {
        return sourceIdentity;
    }

    /**
     * Indicate that this descriptor instance should not be returned to the pool for reuse.
     *
     * @return this for a fluent API.
     */
    public RecordingDescriptor retain()
    {
        isRetained = true;
        return this;
    }

    /**
     * Has this instance been retained to prevent reuse.
     *
     * @return true if this instance has been flagged to be excluded from the pool.
     */
    public boolean isRetained()
    {
        return isRetained;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "RecordingDescriptor{" +
            "controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", recordingId=" + recordingId +
            ", startTimestamp=" + startTimestamp +
            ", stopTimestamp=" + stopTimestamp +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", initialTermId=" + initialTermId +
            ", segmentFileLength=" + segmentFileLength +
            ", termBufferLength=" + termBufferLength +
            ", mtuLength=" + mtuLength +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", strippedChannel='" + strippedChannel + '\'' +
            ", originalChannel='" + originalChannel + '\'' +
            ", sourceIdentity='" + sourceIdentity + '\'' +
            '}';
    }
}
