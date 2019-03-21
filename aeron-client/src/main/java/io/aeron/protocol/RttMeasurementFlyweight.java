/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * <a target="_blank" href="https://github.com/real-logic/aeron/wiki/Protocol-Specification#rtt-measurement-header">
 * RTT Measurement Frame Header</a> wiki page.
 */
public class RttMeasurementFlyweight extends HeaderFlyweight
{
    public static final short REPLY_FLAG = 0x80;
    public static final int HEADER_LENGTH = 40;

    private static final int SESSION_ID_FIELD_OFFSET = 8;
    private static final int STREAM_ID_FIELD_OFFSET = 12;
    private static final int ECHO_TIMESTAMP_FIELD_OFFSET = 16;
    private static final int RECEPTION_DELTA_FIELD_OFFSET = 24;
    private static final int RECEIVER_ID_FIELD_OFFSET = 32;

    public RttMeasurementFlyweight()
    {
    }

    public RttMeasurementFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    public RttMeasurementFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * return session id field
     *
     * @return session id field
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set session id field
     *
     * @param sessionId field value
     * @return flyweight
     */
    public RttMeasurementFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * return stream id field
     *
     * @return stream id field
     */
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * set stream id field
     *
     * @param streamId field value
     * @return flyweight
     */
    public RttMeasurementFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    public long echoTimestampNs()
    {
        return getLong(ECHO_TIMESTAMP_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    public RttMeasurementFlyweight echoTimestampNs(final long timestamp)
    {
        putLong(ECHO_TIMESTAMP_FIELD_OFFSET, timestamp, LITTLE_ENDIAN);

        return this;
    }

    public long receptionDelta()
    {
        return getLong(RECEPTION_DELTA_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    public RttMeasurementFlyweight receptionDelta(final long delta)
    {
        putLong(RECEPTION_DELTA_FIELD_OFFSET, delta, LITTLE_ENDIAN);

        return this;
    }

    public long receiverId()
    {
        return getLong(RECEIVER_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    public RttMeasurementFlyweight receiverId(final long id)
    {
        putLong(RECEIVER_ID_FIELD_OFFSET, id, LITTLE_ENDIAN);

        return this;
    }

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