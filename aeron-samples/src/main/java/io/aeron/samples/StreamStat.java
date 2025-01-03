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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.driver.status.SubscriberPos;
import org.agrona.collections.Hashing;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.status.CountersReader;

import java.io.PrintStream;
import java.util.*;

import static io.aeron.driver.status.PublisherLimit.PUBLISHER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.PublisherPos.PUBLISHER_POS_TYPE_ID;
import static io.aeron.driver.status.ReceiverPos.RECEIVER_POS_TYPE_ID;
import static io.aeron.driver.status.SenderLimit.SENDER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.StreamCounter.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Tool for taking a snapshot of Aeron streams and relevant position counters.
 * <p>
 * Each stream managed by the {@link io.aeron.driver.MediaDriver} will be sampled and a line of text
 * output per stream with each of the position counters for that stream.
 * <p>
 * Each counter has the format:
 * {@code <label-name>:<registration-id>:<position value>}
 */
public final class StreamStat
{
    private static final Comparator<StreamCompositeKey> LINES_COMPARATOR =
        Comparator.comparingLong(StreamCompositeKey::sessionId)
        .thenComparingInt(StreamCompositeKey::streamId)
        .thenComparing(StreamCompositeKey::channel);

    private final CountersReader counters;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final CountersReader counters = SamplesUtil.mapCounters();
        final StreamStat streamStat = new StreamStat(counters);

        streamStat.print(System.out);
    }

    /**
     * Construct by using a {@link CountersReader} which can be obtained from {@link Aeron#countersReader()}.
     *
     * @param counters to read for tracking positions.
     */
    public StreamStat(final CountersReader counters)
    {
        this.counters = counters;
    }

    /**
     * Take a snapshot of all the counters and group them by streams.
     *
     * @return a snapshot of all the counters and group them by streams.
     */
    public Map<StreamCompositeKey, List<StreamPosition>> snapshot()
    {
        final Map<StreamCompositeKey, List<StreamPosition>> streams = new TreeMap<>(LINES_COMPARATOR);
        final Object2ObjectHashMap<StreamCompositeKey, String> fullChannelByStream = new Object2ObjectHashMap<>();

        counters.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if ((typeId >= PUBLISHER_LIMIT_TYPE_ID && typeId <= RECEIVER_POS_TYPE_ID) ||
                    typeId == SENDER_LIMIT_TYPE_ID || typeId == PUBLISHER_POS_TYPE_ID)
                {
                    final int channelLength = keyBuffer.getInt(CHANNEL_OFFSET, LITTLE_ENDIAN);
                    final String channel =
                        keyBuffer.getStringWithoutLengthAscii(CHANNEL_OFFSET + SIZE_OF_INT, channelLength);

                    final int uriIndex = label.indexOf("aeron:");
                    final String fullChannel;
                    if (uriIndex >= 0)
                    {
                        int joinPositionIndex = -1;
                        if (SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID == typeId &&
                            (joinPositionIndex = label.lastIndexOf(" @")) > uriIndex)
                        {
                            fullChannel = label.substring(uriIndex, joinPositionIndex);
                        }
                        else
                        {
                            fullChannel = label.substring(uriIndex);
                        }
                    }
                    else
                    {
                        fullChannel = channel;
                    }

                    final StreamCompositeKey key = new StreamCompositeKey(
                        keyBuffer.getInt(SESSION_ID_OFFSET), keyBuffer.getInt(STREAM_ID_OFFSET), channel);

                    final StreamPosition position = new StreamPosition(
                        keyBuffer.getLong(REGISTRATION_ID_OFFSET), counters.getCounterValue(counterId), typeId);

                    List<StreamPosition> positions = streams.get(key);
                    if (null == positions)
                    {
                        positions = new ArrayList<>();
                        streams.put(key, positions);
                    }
                    positions.add(position);

                    final String existingFullChannel = fullChannelByStream.get(key);
                    if (null == existingFullChannel || fullChannel.length() > existingFullChannel.length())
                    {
                        fullChannelByStream.put(key, fullChannel);
                    }
                }
            });

        streams.keySet().forEach(k -> k.fullChannel = fullChannelByStream.get(k));

        return streams;
    }

    /**
     * Print a snapshot of the stream positions to a {@link PrintStream}.
     * <p>
     * Each stream will be printed on its own line.
     *
     * @param out to which the stream snapshot will be written.
     * @return the number of streams printed.
     */
    public int print(final PrintStream out)
    {
        final Map<StreamCompositeKey, List<StreamPosition>> streams = snapshot();
        final StringBuilder builder = new StringBuilder();

        for (final Map.Entry<StreamCompositeKey, List<StreamPosition>> entry : streams.entrySet())
        {
            builder.setLength(0);
            final StreamCompositeKey key = entry.getKey();

            builder
                .append("sessionId=").append(key.sessionId())
                .append(" streamId=").append(key.streamId())
                .append(" channel=").append(key.fullChannel)
                .append(" :");

            for (final StreamPosition streamPosition : entry.getValue())
            {
                builder
                    .append(' ')
                    .append(labelName(streamPosition.typeId()))
                    .append(':').append(streamPosition.id())
                    .append(':').append(streamPosition.value());
            }

            out.println(builder);
        }

        return streams.size();
    }

    /**
     * Composite key which identifies an Aeron stream of messages.
     */
    public static final class StreamCompositeKey
    {
        private final int sessionId;
        private final int streamId;
        private final String channel;
        String fullChannel;

        /**
         * Construct a new key representing a unique stream.
         *
         * @param sessionId to identify the stream.
         * @param streamId  within a channel.
         * @param channel   as a URI.
         */
        public StreamCompositeKey(final int sessionId, final int streamId, final String channel)
        {
            Objects.requireNonNull(channel, "Channel cannot be null");

            this.sessionId = sessionId;
            this.streamId = streamId;
            this.channel = channel;
        }

        /**
         * The session id of the stream.
         *
         * @return session id of the stream.
         */
        public int sessionId()
        {
            return sessionId;
        }

        /**
         * The stream id within a channel.
         *
         * @return stream id within a channel.
         */
        public int streamId()
        {
            return streamId;
        }

        /**
         * The channel as a URI.
         *
         * @return channel as a URI.
         */
        public String channel()
        {
            return channel;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (!(o instanceof StreamCompositeKey))
            {
                return false;
            }

            final StreamCompositeKey that = (StreamCompositeKey)o;

            return this.sessionId == that.sessionId &&
                this.streamId == that.streamId &&
                this.channel.equals(that.channel);
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            int result = sessionId;
            result = 31 * result + streamId;
            result = 31 * result + channel.hashCode();

            return Hashing.hash(result);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "StreamCompositeKey{" +
                "sessionId=" + sessionId +
                ", streamId=" + streamId +
                ", channel='" + channel + '\'' +
                '}';
        }
    }

    /**
     * Represents a position within a particular stream of messages.
     */
    public static final class StreamPosition
    {
        private final long id;
        private final long value;
        private final int typeId;

        /**
         * Stream position representation.
         *
         * @param id     of the registered entity.
         * @param value  of the position.
         * @param typeId of the counter.
         */
        public StreamPosition(final long id, final long value, final int typeId)
        {
            this.id = id;
            this.value = value;
            this.typeId = typeId;
        }

        /**
         * The identifier for the registered entity, such as publication or subscription, to which the counter relates.
         *
         * @return the identifier for the registered entity to which the counter relates.
         */
        public long id()
        {
            return id;
        }

        /**
         * The value of the counter.
         *
         * @return the value of the counter.
         */
        public long value()
        {
            return value;
        }

        /**
         * The type category of the counter for the stream position.
         *
         * @return the type category of the counter for the stream position.
         */
        public int typeId()
        {
            return typeId;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (!(o instanceof StreamPosition))
            {
                return false;
            }

            final StreamPosition that = (StreamPosition)o;

            return this.id == that.id && this.value == that.value && this.typeId == that.typeId;
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            int result = Hashing.hash(id);
            result = 31 * result + Hashing.hash(value);
            result = 31 * result + typeId;

            return Hashing.hash(result);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "StreamPosition{" +
                "id=" + id +
                ", value=" + value +
                ", typeId=" + typeId +
                '}';
        }
    }
}
