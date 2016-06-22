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
package io.aeron.driver.status;

import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.nio.charset.StandardCharsets;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Allocates {@link Position} counters on a stream of messages. Positions tracked in bytes include:
 * <ul>
 *     <li>{@link PublisherLimit}: Limit for flow controlling a {@link io.aeron.Publication} steam.</li>
 *     <li>{@link SenderPos}: Highest position on a {@link io.aeron.Publication} stream sent to the media.</li>
 *     <li>{@link ReceiverHwm}: Highest position by the Receiver when rebuilding an {@link io.aeron.Image} of a stream.</li>
 *     <li>{@link SubscriberPos}: Consumption position in an {@link io.aeron.Image} of a stream for each Subscriber.</li>
 * </ul>
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
        return allocate(name, typeId, countersManager, registrationId, sessionId, streamId, channel, "");
    }

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
     * @param suffix          for the label.
     * @return a new {@link Position} for tracking the stream.
     */
    public static Position allocate(
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel,
        final String suffix)
    {
        final String label =
            name + ": " + registrationId + ' ' + sessionId + ' ' + streamId + ' ' + channel + ' ' + suffix;

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

    /**
     * Return the label name for a counter type identifier.
     *
     * @param typeId of the counter.
     * @return the label name as a String.
     */
    public static String labelName(final int typeId)
    {
        switch (typeId)
        {
            case PublisherLimit.PUBLISHER_LIMIT_TYPE_ID:
                return PublisherLimit.NAME;

            case SenderPos.SENDER_POSITION_TYPE_ID:
                return SenderPos.NAME;

            case ReceiverHwm.RECEIVER_HWM_TYPE_ID:
                return ReceiverHwm.NAME;

            case SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID:
                return SubscriberPos.NAME;

            case ReceiverPos.RECEIVER_POS_TYPE_ID:
                return ReceiverPos.NAME;

            default:
                return "<unknown>";
        }
    }
}
