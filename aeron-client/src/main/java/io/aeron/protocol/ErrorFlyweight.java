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

import io.aeron.command.ErrorResponseFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Flyweight for general Aeron network protocol error frame
 * <pre>
 *    0                   1                   2                   3
 *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * 0  |R|                 Frame Length (varies)                       |
 *    +---------------+---------------+-------------------------------+
 * 4  |   Version     |     Flags     |         Type (=0x04)          |
 *    +---------------+---------------+-------------------------------+
 * 8  |                          Session ID                           |
 *    +---------------------------------------------------------------+
 * 12 |                           Stream ID                           |
 *    +---------------------------------------------------------------+
 * 16 |                          Error Code                           |
 *    +---------------------------------------------------------------+
 * 20 |                     Error String Length                       |
 *    +---------------------------------------------------------------+
 * 24 |                         Error String                        ...
 *    +---------------------------------------------------------------+
 *    ...                                                             |
 *    +---------------------------------------------------------------+
 * </pre>
 */
public class ErrorFlyweight extends HeaderFlyweight
{
    /**
     * Length of the Error Header.
     */
    public static final int HEADER_LENGTH = 24;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    public static final int SESSION_ID_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    public static final int STREAM_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the error code field begins.
     */
    public static final int ERROR_CODE_FIELD_OFFSET = 16;

    /**
     * Offset in the frame at which the error string field begins. Specifically this will be the length of the string
     * using the Agrona buffer standard of using 4 bytes for the length. Followed by the variable bytes for the string.
     */
    public static final int ERROR_STRING_FIELD_OFFSET = 20;

    public static final int MAX_ERROR_MESSAGE_LENGTH = 1023;

    public static final int MAX_ERROR_FRAME_LENGTH = HEADER_LENGTH + MAX_ERROR_MESSAGE_LENGTH;

    /**
     * Default constructor for the ErrorFlyweight so that it can be wrapped over a buffer later.
     */
    public ErrorFlyweight()
    {
    }

    /**
     * Construct the ErrorFlyweight over an NIO ByteBuffer frame.
     *
     * @param buffer containing the frame.
     */
    public ErrorFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the ErrorFlyweight over an UnsafeBuffer frame.
     *
     * @param buffer containing the frame.
     */
    public ErrorFlyweight(final UnsafeBuffer buffer)
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
    public ErrorFlyweight sessionId(final int sessionId)
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
    public ErrorFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * The error-code for the message.
     *
     * @return error-code for the message.
     */
    public int errorCode()
    {
        return getInt(ERROR_CODE_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set error-code for the message.
     *
     * @param errorCode for the message.
     * @return this for a fluent API.
     */
    public ErrorFlyweight errorCode(final int errorCode)
    {
        putInt(ERROR_CODE_FIELD_OFFSET, errorCode, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get the error string for the message.
     *
     * @return the error string for the message.
     */
    public String errorMessage()
    {
        return getStringUtf8(ERROR_STRING_FIELD_OFFSET);
    }

    /**
     * Set the error string for the message.
     *
     * @param errorString the error string in UTF-8.
     * @return this for a fluent API.
     */
    public ErrorFlyweight errorMessage(final String errorString)
    {
        putStringUtf8(ERROR_STRING_FIELD_OFFSET, errorString, LITTLE_ENDIAN);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ERROR{" +
            "frame-length=" + frameLength() +
            " version=" + version() +
            " flags=" + String.valueOf(flagsToChars(flags())) +
            " type=" + headerType() +
            " session-id=" + sessionId() +
            " stream-id=" + streamId() +
            " error-code=" + errorCode() +
            " error-message=" + errorMessage() +
            "}";
    }
}
