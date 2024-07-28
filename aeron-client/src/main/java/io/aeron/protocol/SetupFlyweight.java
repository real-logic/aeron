/*
 * Copyright 2014-2024 Real Logic Limited.
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
 * HeaderFlyweight for Setup Message Frames.
 * <p>
 * <a target="_blank" href="https://github.com/real-logic/aeron/wiki/Transport-Protocol-Specification#stream-setup">
 *     Stream Setup</a> wiki page.
 */
public class SetupFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Setup Message Frame.
     */
    public static final int HEADER_LENGTH = 40;

    /**
     * Subscriber should send response channel setup message.
     */
    public static final short SEND_RESPONSE_SETUP_FLAG = 0x80;

    /**
     * Publication uses group/multicast semantics.
     */
    public static final short GROUP_FLAG = 0x40;

    /**
     * Offset in the frame at which the term-offset field begins.
     */
    private static final int TERM_OFFSET_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    private static final int SESSION_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    private static final int STREAM_ID_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the initial-term-id field begins.
     */
    private static final int INITIAL_TERM_ID_FIELD_OFFSET = 20;

    /**
     * Offset in the frame at which the active-term-id field begins.
     */
    private static final int ACTIVE_TERM_ID_FIELD_OFFSET = 24;

    /**
     * Offset in the frame at which the term-length field begins.
     */
    private static final int TERM_LENGTH_FIELD_OFFSET = 28;

    /**
     * Offset in the frame at which the mtu-length field begins.
     */
    private static final int MTU_LENGTH_FIELD_OFFSET = 32;

    /**
     * Offset in the frame at which the multicast TTL (Time To Live) field begins.
     */
    private static final int TTL_FIELD_OFFSET = 36;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public SetupFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public SetupFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public SetupFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Get term offset field.
     *
     * @return term offset field.
     */
    public int termOffset()
    {
        return getInt(TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set term offset field.
     *
     * @param termOffset field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight termOffset(final int termOffset)
    {
        putInt(TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get session id field.
     *
     * @return session id field.
     */
    public int sessionId()
    {
        return getInt(SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set session id field.
     *
     * @param sessionId field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight sessionId(final int sessionId)
    {
        putInt(SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get stream id field.
     *
     * @return stream id field.
     */
    public int streamId()
    {
        return getInt(STREAM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set stream id field.
     *
     * @param streamId field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get initial term id field.
     *
     * @return initial term id field.
     */
    public int initialTermId()
    {
        return getInt(INITIAL_TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set initial term id field.
     *
     * @param termId field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight initialTermId(final int termId)
    {
        putInt(INITIAL_TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get active term id field.
     *
     * @return term id field.
     */
    public int activeTermId()
    {
        return getInt(ACTIVE_TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set active term id field.
     *
     * @param termId field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight activeTermId(final int termId)
    {
        putInt(ACTIVE_TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get term length field.
     *
     * @return term length field value.
     */
    public int termLength()
    {
        return getInt(TERM_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set term length field.
     *
     * @param termLength field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight termLength(final int termLength)
    {
        putInt(TERM_LENGTH_FIELD_OFFSET, termLength, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get MTU length field.
     *
     * @return MTU length field value.
     */
    public int mtuLength()
    {
        return getInt(MTU_LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set MTU length field.
     *
     * @param mtuLength field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight mtuLength(final int mtuLength)
    {
        putInt(MTU_LENGTH_FIELD_OFFSET, mtuLength, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the TTL field.
     *
     * @return TTL field value.
     */
    public int ttl()
    {
        return getInt(TTL_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set the TTL field.
     *
     * @param ttl field value.
     * @return this for a fluent API.
     */
    public SetupFlyweight ttl(final int ttl)
    {
        putInt(TTL_FIELD_OFFSET, ttl, LITTLE_ENDIAN);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SETUP{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " term-offset=" + termOffset() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " initial-term-id=" + initialTermId() +
            " active-term-id=" + activeTermId() +
            " term-length=" + termLength() +
            " mtu-length=" + mtuLength() +
            " ttl=" + ttl() +
            "}";
    }
}
