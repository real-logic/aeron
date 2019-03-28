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
package io.aeron.samples;

import org.agrona.concurrent.status.CountersReader;

import java.io.PrintStream;
import java.util.*;

import static io.aeron.driver.status.PerImageIndicator.PER_IMAGE_TYPE_ID;
import static io.aeron.driver.status.PublisherLimit.PUBLISHER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.PublisherPos.PUBLISHER_POS_TYPE_ID;
import static io.aeron.driver.status.ReceiverPos.RECEIVER_POS_TYPE_ID;
import static io.aeron.driver.status.SenderLimit.SENDER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.StreamCounter.*;

/**
 * Tool for taking a snapshot of Aeron streams and relevant position counters.
 * <p>
 * Each stream managed by the {@link io.aeron.driver.MediaDriver} will be sampled and a line of text
 * output per stream with each of the position counters for that stream.
 * <p>
 * Each counter has the format:
 * {@code <label-name>:<registration-id>:<position value>}
 */
public class StreamStat
{
    private final CountersReader counters;

    public static void main(final String[] args)
    {
        final CountersReader counters = SamplesUtil.mapCounters();
        final StreamStat streamStat = new StreamStat(counters);

        streamStat.print(System.out);
    }

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
        final Map<StreamCompositeKey, List<StreamPosition>> streams = new HashMap<>();

        counters.forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if ((typeId >= PUBLISHER_LIMIT_TYPE_ID && typeId <= RECEIVER_POS_TYPE_ID) ||
                    typeId == SENDER_LIMIT_TYPE_ID || typeId == PER_IMAGE_TYPE_ID || typeId == PUBLISHER_POS_TYPE_ID)
                {
                    final StreamCompositeKey key = new StreamCompositeKey(
                        keyBuffer.getInt(SESSION_ID_OFFSET),
                        keyBuffer.getInt(STREAM_ID_OFFSET),
                        keyBuffer.getStringAscii(CHANNEL_OFFSET));

                    final StreamPosition position = new StreamPosition(
                        keyBuffer.getLong(REGISTRATION_ID_OFFSET),
                        counters.getCounterValue(counterId),
                        typeId);

                    streams
                        .computeIfAbsent(key, (ignore) -> new ArrayList<>())
                        .add(position);
                }
            });

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
                .append(" channel=").append(key.channel())
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
    public static class StreamCompositeKey
    {
        private final int sessionId;
        private final int streamId;
        private final String channel;

        public StreamCompositeKey(final int sessionId, final int streamId, final String channel)
        {
            Objects.requireNonNull(channel, "Channel cannot be null");

            this.sessionId = sessionId;
            this.streamId = streamId;
            this.channel = channel;
        }

        public int sessionId()
        {
            return sessionId;
        }

        public int streamId()
        {
            return streamId;
        }

        public String channel()
        {
            return channel;
        }

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

        public int hashCode()
        {
            int result = sessionId;
            result = 31 * result + streamId;
            result = 31 * result + channel.hashCode();

            return result;
        }

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
    public static class StreamPosition
    {
        private final long id;
        private final long value;
        private final int typeId;

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

        public int hashCode()
        {
            int result = (int)(id ^ (id >>> 32));
            result = 31 * result + (int)(value ^ (value >>> 32));
            result = 31 * result + typeId;

            return result;
        }

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
