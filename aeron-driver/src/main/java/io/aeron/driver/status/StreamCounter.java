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
package io.aeron.driver.status;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import static io.aeron.status.CounterKeyOffset.REGISTRATION_ID_OFFSET;
import static io.aeron.status.CounterKeyOffset.SESSION_ID_OFFSET;
import static io.aeron.status.CounterKeyOffset.STREAM_ID_OFFSET;
import static io.aeron.status.CounterKeyOffset.CHANNEL_OFFSET;
import static io.aeron.status.CounterKeyOffset.MAX_CHANNEL_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;

/**
 * Allocates counters on a stream of messages.
 * <p>
 * Positions tracked in bytes include:
 * <ul>
 * <li>{@link PublisherPos}: Highest position on a {@link io.aeron.Publication} reached for offers and claims as an
 *     approximation which is sampled once per second.</li>
 * <li>{@link PublisherLimit}: Limit for flow controlling a {@link io.aeron.Publication} offers and claims.</li>
 * <li>{@link SenderPos}: Highest position on a {@link io.aeron.Publication} stream sent to the media.</li>
 * <li>{@link SenderLimit}: Limit for flow controlling a {@link io.aeron.driver.Sender} of a stream.</li>
 * <li>{@link ReceiverHwm}: Highest position observed by the {@link io.aeron.driver.Receiver} when rebuilding an
 *     {@link io.aeron.Image} of a stream.</li>
 * <li>{@link ReceiverPos}: Highest contiguous position rebuilt by the {@link io.aeron.driver.Receiver} on an
 *     {@link io.aeron.Image} of a stream.</li>
 * <li>{@link SubscriberPos}: Consumption position on an {@link io.aeron.Image} of a stream by an individual
 *     Subscriber.</li>
 * </ul>
 * <p>
 * <b>Note:</b> All counters are real-time except {@link PublisherPos} which is sampled once per second
 * and as a result it can appear to be behind the others.
 */
public class StreamCounter
{

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
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
        final int counterId = allocateCounterId(
            tempBuffer, name, typeId, countersManager, registrationId, sessionId, streamId, channel);

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }

    /**
     * Allocate a counter id for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @param sessionId       for the stream of messages.
     * @param streamId        for the stream of messages.
     * @param channel         for the stream of messages.
     * @return the id of the allocated counter.
     */
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

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            labelOffset + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        final int counterId = countersManager.allocate(
            typeId, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counterId, registrationId);

        return counterId;
    }

    /**
     * Allocate a counter for tracking a position on a stream of messages.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
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

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": ");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(labelOffset + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            labelOffset + labelLength, channel, 0, MAX_LABEL_LENGTH - labelLength);

        if (labelLength < (MAX_LABEL_LENGTH - 20))
        {
            labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, " @");
            labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, joinPosition);
        }

        final int counterId = countersManager.allocate(
            typeId, tempBuffer, 0, keyLength, tempBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counterId, registrationId);

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }
}
