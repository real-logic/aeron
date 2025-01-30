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

// CHECKSTYLE:OFF:LineLength
/**
 * Flyweight for a NAK Message Frame.
 * <p>
 * <a target="_blank"
 *    href="https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification#data-recovery-via-retransmit-request">
 *    Data Loss Recovery</a> wiki page.
 */
// CHECKSTYLE:ON:LineLength
public class NakFlyweight extends HeaderFlyweight
{
    /**
     * Length of the frame in bytes.
     */
    public static final int HEADER_LENGTH = 28;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    private static final int SESSION_ID_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    private static final int STREAM_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the term-id field begins.
     */
    private static final int TERM_ID_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the term-offset field begins.
     */
    private static final int TERM_OFFSET_FIELD_OFFSET = 20;

    /**
     * Offset in the frame at which the length of the NAK range field begins.
     */
    private static final int LENGTH_FIELD_OFFSET = 24;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public NakFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public NakFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public NakFlyweight(final UnsafeBuffer buffer)
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
    public NakFlyweight sessionId(final int sessionId)
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
    public NakFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The term-id for the stream.
     *
     * @return term-id for the stream.
     */
    public int termId()
    {
        return getInt(TERM_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set term-id for the stream.
     *
     * @param termId term-id for the stream.
     * @return this for a fluent API.
     */
    public NakFlyweight termId(final int termId)
    {
        putInt(TERM_ID_FIELD_OFFSET, termId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The term-offset for the stream.
     *
     * @return term-offset for the stream.
     */
    public int termOffset()
    {
        return getInt(TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set term-offset for the stream.
     *
     * @param termOffset term-offset for the stream.
     * @return for a fluent API.
     */
    public NakFlyweight termOffset(final int termOffset)
    {
        putInt(TERM_OFFSET_FIELD_OFFSET, termOffset, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The length of the encoded frame.
     *
     * @return length of the encoded frame.
     */
    public int length()
    {
        return getInt(LENGTH_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set length of the encoded frame.
     *
     * @param length of the encoded frame.
     * @return this for a fluent API.
     */
    public NakFlyweight length(final int length)
    {
        putInt(LENGTH_FIELD_OFFSET, length, LITTLE_ENDIAN);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "NAK{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " term-offset=" + termOffset() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " term-id=" + termId() +
            " length=" + length() +
            "}";
    }
}
