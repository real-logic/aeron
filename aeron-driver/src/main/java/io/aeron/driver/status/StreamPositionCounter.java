/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;

/**
 * Allocates {@link UnsafeBufferPosition} counters on a stream of messages.
 * <p>
 * Positions tracked in bytes include:
 * <ul>
 * <li>{@link PublisherPos}: Highest position on a {@link io.aeron.Publication} reached for offers and claims as an approximation sampled once per second.</li>
 * <li>{@link PublisherLimit}: Limit for flow controlling a {@link io.aeron.Publication} offers and claims.</li>
 * <li>{@link SenderPos}: Highest position on a {@link io.aeron.Publication} stream sent to the media.</li>
 * <li>{@link SenderLimit}: Limit for flow controlling a {@link io.aeron.driver.Sender} of a stream.</li>
 * <li>{@link ReceiverHwm}: Highest position observed by the Receiver when rebuilding an {@link io.aeron.Image} of a stream.</li>
 * <li>{@link ReceiverPos}: Highest contiguous position rebuilt by the Receiver on an {@link io.aeron.Image} of a stream.</li>
 * <li>{@link SubscriberPos}: Consumption position on an {@link io.aeron.Image} of a stream by individual Subscriber.</li>
 * </ul>
 * <p>
 * <b>Note:</b> All counters are real-time with the exception of {@link PublisherPos} which is sampled once per second
 * which means it can appear to be behind.
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
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which to allocated the underlying storage.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @return a new {@link UnsafeBufferPosition} for tracking the stream.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        return new UnsafeBufferPosition(
            (UnsafeBuffer)countersManager.valuesBuffer(),
            allocateCounterId(tempBuffer, name, typeId, countersManager, registrationId, sessionId, streamId, channel),
            countersManager);
    }

    public static int allocateCounterId(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(STREAM_ID_OFFSET, streamId);

        final int channelLength = tempBuffer.putStringWithoutLengthAscii(
            CHANNEL_OFFSET + SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
        tempBuffer.putInt(CHANNEL_OFFSET, channelLength);
        final int keyLength = CHANNEL_OFFSET + SIZE_OF_INT + channelLength;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(keyLength + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            keyLength + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        return countersManager.allocate(
            typeId,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            keyLength,
            labelLength);
    }

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which to allocated the underlying storage.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @param joinPosition    for the label.
     * @return a new {@link UnsafeBufferPosition} for tracking the stream.
     */
    public static UnsafeBufferPosition allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel,
        final long joinPosition)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(STREAM_ID_OFFSET, streamId);

        final int channelLength = tempBuffer.putStringWithoutLengthAscii(
            CHANNEL_OFFSET + SIZE_OF_INT, channel, 0, MAX_CHANNEL_LENGTH);
        tempBuffer.putInt(CHANNEL_OFFSET, channelLength);
        final int keyLength = CHANNEL_OFFSET + SIZE_OF_INT + channelLength;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(keyLength + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            keyLength + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        if (labelLength < (MAX_LABEL_LENGTH - 20))
        {
            labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " @");
            labelLength += tempBuffer.putLongAscii(keyLength + labelLength, joinPosition);
        }

        final int counterId = countersManager.allocate(
            typeId,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            keyLength,
            labelLength);

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

            case SenderLimit.SENDER_LIMIT_TYPE_ID:
                return SenderLimit.NAME;

            case PublisherPos.PUBLISHER_POS_TYPE_ID:
                return PublisherPos.NAME;

            default:
                return "<unknown>";
        }
    }
}
