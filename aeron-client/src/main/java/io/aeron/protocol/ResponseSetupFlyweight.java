/*
 * Copyright 2014-2023 Real Logic Limited.
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
 * HeaderFlyweight for Response Setup Message Frames.
 * <p>
 * <a target="_blank" href="https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification#stream-setup">
 *     Stream Response Setup</a> wiki page.
 */
public class ResponseSetupFlyweight extends HeaderFlyweight
{
    /**
     * Header length in bytes.
     */
    public static final int HEADER_LENGTH = 20;

    /**
     * Offset in the frame at which the session-id field begins.
     */
    private static final int SESSION_ID_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the stream-id field begins.
     */
    private static final int STREAM_ID_FIELD_OFFSET = 12;

    /**
     * Offset in the frame at which the response session-id field begins.
     */
    private static final int RESPONSE_SESSION_ID_FIELD_OFFSET = 16;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public ResponseSetupFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public ResponseSetupFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public ResponseSetupFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
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
    public ResponseSetupFlyweight sessionId(final int sessionId)
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
    public ResponseSetupFlyweight streamId(final int streamId)
    {
        putInt(STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }

    /**
     * Get response session id field.
     *
     * @return response session id field.
     */
    public int responseSessionId()
    {
        return getInt(RESPONSE_SESSION_ID_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Set stream id field.
     *
     * @param streamId field value.
     * @return this for a fluent API.
     */
    public ResponseSetupFlyweight responseSessionId(final int streamId)
    {
        putInt(RESPONSE_SESSION_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);

        return this;
    }
}
