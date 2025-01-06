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
package io.aeron.protocol;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for an RTT Measurement Frame Header.
 * <p>
 * <a target="_blank"
 *    href="https://github.com/real-logic/aeron/wiki/Transport-Protocol-Specification#rtt-measurement-header">
 * RTT Measurement Frame Header</a> wiki page.
 */
public class RttMeasurementFlyweight extends HeaderFlyweight
{
    /**
     * Flag set to indicate this is a reply message.
     */
    public static final short REPLY_FLAG = 0x80;

    /**
     * Length of the header of the frame.
     */
    public static final int HEADER_LENGTH = 40;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    private static final int SESSION_ID_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    private static final int STREAM_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the echo timestamp field begins.
     */
    private static final int ECHO_TIMESTAMP_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the reception delta field begins.
     */
    private static final int RECEPTION_DELTA_FIELD_OFFSET = 24;

    /**
     * Offset in the frame at which the receiver-id field begins.
     */
    private static final int RECEIVER_ID_FIELD_OFFSET = 32;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public RttMeasurementFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public RttMeasurementFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public RttMeasurementFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * The session-id for the stream.
     *
     * @return session-id for the stream.
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set session-id for the stream.
     *
     * @param sessionId session-id for the stream.
     * @return this for a fluent API.
     */
    public RttMeasurementFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The stream-id for the stream.
     *
     * @return stream-id for the stream.
     */
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set stream-id for the stream.
     *
     * @param streamId stream-id for the stream.
     * @return this for a fluent API.
     */
    public RttMeasurementFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Timestamp to echo in a reply or the timestamp in the original RTT Measurement.
     *
     * @return timestamp to echo in a reply or the timestamp in the original RTT Measurement.
     */
    public long echoTimestampNs()
    {
        return getLong(ECHO_TIMESTAMP_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set timestamp to echo in a reply or the timestamp in the original RTT Measurement.
     *
     * @param timestampNs to echo in a reply or the timestamp in the original RTT Measurement.
     * @return this for a fluent API.
     */
    public RttMeasurementFlyweight echoTimestampNs(final long timestampNs)
    {
        putLong(ECHO_TIMESTAMP_FIELD_OFFSET, timestampNs, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Time in nanoseconds between receiving original RTT Measurement and sending Reply RTT Measurement.
     *
     * @return time in nanoseconds between receiving original RTT Measurement and sending Reply RTT Measurement.
     */
    public long receptionDelta()
    {
        return getLong(RECEPTION_DELTA_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set time in nanoseconds between receiving original RTT Measurement and sending Reply RTT Measurement.
     *
     * @param deltaNs in nanoseconds between receiving original RTT Measurement and sending Reply RTT Measurement.
     * @return this for a fluent API.
     */
    public RttMeasurementFlyweight receptionDelta(final long deltaNs)
    {
        putLong(RECEPTION_DELTA_FIELD_OFFSET, deltaNs, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The receiver-id which uniquely identifies a receiver of a stream.
     *
     * @return receiver-id which uniquely identifies a receiver of a stream.
     */
    public long receiverId()
    {
        return getLong(RECEIVER_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set receiver-id which uniquely identifies a receiver of a stream.
     *
     * @param id for the receiver of the stream.
     * @return this for a fluent API.
     */
    public RttMeasurementFlyweight receiverId(final long id)
    {
        putLong(RECEIVER_ID_FIELD_OFFSET, id, LITTLE_ENDIAN);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "RTTM{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " echo-timestamp=" + echoTimestampNs() +
            " reception-delta=" + receptionDelta() +
            " receiver-id=" + receiverId() +
            "}";
    }
}