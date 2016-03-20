/*
 * Copyright 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.status;

import uk.co.real_logic.agrona.concurrent.status.CountersManager;
import uk.co.real_logic.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;
import uk.co.real_logic.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.charset.StandardCharsets;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Allocates {@link Position} counters on a stream of messages.
 */
public class StreamPositionCounter
{
    /**
     * Offset in the key meta data for the registration id of the counter.
     */
    public static final int REGISTRATION_ID_OFFSET = 0;

    /**
     * Offset in the key meta data for the session id of the counter.
     */
    public static final int SESSION_ID_OFFSET = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;

    /**
     * Offset in the key meta data for the stream id of the counter.
     */
    public static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;

    /**
     * Offset in the key meta data for the channel of the counter.
     */
    public static final int CHANNEL_OFFSET = STREAM_ID_OFFSET + SIZE_OF_INT;

    /**
     * The maximum length in bytes of the encoded channel identity.
     */
    public static final int MAX_CHANNEL_LENGTH = CountersReader.MAX_KEY_LENGTH - (CHANNEL_OFFSET + SIZE_OF_INT);

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which to allocated the underlying storage.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @return a new {@link Position} for tracking the stream.
     */
    public static Position allocate(
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final String label = name + ": " + registrationId + ' ' + sessionId + ' ' + streamId + ' ' + channel;

        final int counterId = countersManager.allocate(
            label,
            typeId,
            (buffer) ->
            {
                buffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
                buffer.putInt(SESSION_ID_OFFSET, sessionId);
                buffer.putInt(STREAM_ID_OFFSET, streamId);

                final byte[] channelBytes = channel.getBytes(StandardCharsets.UTF_8);
                final int length = Math.min(channelBytes.length, MAX_CHANNEL_LENGTH);

                buffer.putInt(CHANNEL_OFFSET, length);
                buffer.putBytes(CHANNEL_OFFSET + SIZE_OF_INT, channelBytes, 0, length);
            }
        );

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }
}
