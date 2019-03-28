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

import static io.aeron.driver.status.PerImageIndicator.PER_IMAGE_TYPE_ID;
import static io.aeron.driver.status.PublisherLimit.PUBLISHER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.PublisherPos.PUBLISHER_POS_TYPE_ID;
import static io.aeron.driver.status.ReceiverPos.RECEIVER_POS_TYPE_ID;
import static io.aeron.driver.status.SenderLimit.SENDER_LIMIT_TYPE_ID;
import static io.aeron.driver.status.StreamCounter.CHANNEL_OFFSET;
import static io.aeron.driver.status.StreamCounter.REGISTRATION_ID_OFFSET;
import static io.aeron.driver.status.StreamCounter.SESSION_ID_OFFSET;
import static io.aeron.driver.status.StreamCounter.STREAM_ID_OFFSET;

import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import org.agrona.concurrent.status.CountersReader;

/**
 * Tool for taking a snapshot of Aeron streams backlog information and some explanation for the
 * {@link StreamStat} counters.
 * <p>
 * Each stream managed by the {@link io.aeron.driver.MediaDriver} will be sampled and printed out on {@link System#out}.
 */
public class BacklogStat
{
    private final CountersReader counters;

    public static void main(final String[] args)
    {
        final CountersReader counters = SamplesUtil.mapCounters();
        final BacklogStat backlogStat = new BacklogStat(counters);

        backlogStat.print(System.out);
    }

    public BacklogStat(final CountersReader counters)
    {
        this.counters = counters;
    }

    /**
     * Take a snapshot of all the backlog information and group by stream.
     *
     * @return a snapshot of all the backlog information and group by stream.
     */
    public Map<StreamCompositeKey, StreamBacklog> snapshot()
    {
        final Map<StreamCompositeKey, StreamBacklog> streams = new HashMap<>();

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

                    final StreamBacklog streamBacklog = streams.computeIfAbsent(key, (ignore) -> new StreamBacklog());
                    final long registrationId = keyBuffer.getLong(REGISTRATION_ID_OFFSET);
                    final long value = counters.getCounterValue(counterId);
                    switch (typeId)
                    {
                        case PublisherLimit.PUBLISHER_LIMIT_TYPE_ID:
                            streamBacklog.createPublisherIfAbsent().registrationId(registrationId);
                            streamBacklog.createPublisherIfAbsent().limit(value);
                            break;

                        case PublisherPos.PUBLISHER_POS_TYPE_ID:
                            streamBacklog.createPublisherIfAbsent().registrationId(registrationId);
                            streamBacklog.createPublisherIfAbsent().position(value);
                            break;

                        case SenderPos.SENDER_POSITION_TYPE_ID:
                            streamBacklog.createSenderIfAbsent().registrationId(registrationId);
                            streamBacklog.createSenderIfAbsent().position(value);
                            break;

                        case SenderLimit.SENDER_LIMIT_TYPE_ID:
                            streamBacklog.createSenderIfAbsent().registrationId(registrationId);
                            streamBacklog.createSenderIfAbsent().limit(value);
                            break;

                        case ReceiverHwm.RECEIVER_HWM_TYPE_ID:
                            streamBacklog.createReceiverIfAbsent().registrationId(registrationId);
                            streamBacklog.createReceiverIfAbsent().highWaterMark(value);
                            break;

                        case ReceiverPos.RECEIVER_POS_TYPE_ID:
                            streamBacklog.createReceiverIfAbsent().registrationId(registrationId);
                            streamBacklog.createReceiverIfAbsent().position(value);
                            break;

                        case SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID:
                            streamBacklog.subscriberBacklogs().put(registrationId, new Subscriber(value));
                            break;
                    }
                }
            });

        return streams;
    }

    /**
     * Print a snapshot of the stream backlog with some explanation to a {@link PrintStream}.
     * <p>
     * Each stream will be printed in its own section.
     *
     * @param out to which the stream backlog will be written.
     */
    public void print(final PrintStream out)
    {
        final StringBuilder builder = new StringBuilder();

        for (final Map.Entry<StreamCompositeKey, StreamBacklog> entry : snapshot().entrySet())
        {
            builder.setLength(0);
            final StreamCompositeKey key = entry.getKey();

            builder
                .append("sessionId=").append(key.sessionId())
                .append(" streamId=").append(key.streamId())
                .append(" channel=").append(key.channel())
                .append(" : ");


            final StreamBacklog streamBacklog = entry.getValue();
            if (streamBacklog.publisher() != null)
            {
                builder
                    .append("\n┌─for publisher ")
                    .append(streamBacklog.publisher().registrationId())
                    .append(" the last sampled position is ")
                    .append(streamBacklog.publisher().position())
                    .append(" (~")
                    .append(streamBacklog.publisher().remainingWindow())
                    .append(" bytes before back-pressure)");

                final Sender sender = streamBacklog.sender();
                if (sender != null)
                {
                    final long senderBacklog = sender.backlog(streamBacklog.publisher().position());

                    builder.append("\n└─sender ").append(sender.registrationId());

                    if (senderBacklog >= 0)
                    {
                        builder.append(" has to send ").append(senderBacklog).append(" bytes");
                    }
                    else
                    {
                        builder.append(" is at position ").append(sender.position());
                    }

                    builder.append(" (").append(sender.window()).append(" bytes remaining in the sender window)");
                }
                else
                {
                    builder.append("\n└─no sender yet...");
                }
            }

            if (streamBacklog.receiver() != null)
            {
                builder
                    .append("\n┌─receiver ")
                    .append(streamBacklog.receiver().registrationId())
                    .append(" is at position ")
                    .append(streamBacklog.receiver().position());

                final Iterator<Map.Entry<Long, Subscriber>> subscriberIterator =
                    streamBacklog.subscriberBacklogs().entrySet().iterator();

                while (subscriberIterator.hasNext())
                {
                    final Map.Entry<Long, Subscriber> subscriber = subscriberIterator.next();
                    builder
                        .append(subscriberIterator.hasNext() ? "\n├" : "\n└")
                        .append("─subscriber ")
                        .append(subscriber.getKey())
                        .append(" has ")
                        .append(subscriber.getValue().backlog(streamBacklog.receiver().highWaterMark()))
                        .append(" backlog bytes");
                }
            }

            builder.append('\n');
            out.println(builder);
        }
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
            Objects.requireNonNull(channel, "channel cannot be null");

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
     * Represents the backlog information for a particular stream of messages.
     */
    public static class StreamBacklog
    {
        private Publisher publisher;
        private Sender sender;
        private Receiver receiver;
        private final SortedMap<Long, Subscriber> subscriberBacklogs = new TreeMap<>();

        Publisher publisher()
        {
            return publisher;
        }

        Sender sender()
        {
            return sender;
        }

        Receiver receiver()
        {
            return receiver;
        }

        Map<Long, Subscriber> subscriberBacklogs()
        {
            return subscriberBacklogs;
        }

        Publisher createPublisherIfAbsent()
        {
            return publisher == null ? publisher = new Publisher() : publisher;
        }

        Sender createSenderIfAbsent()
        {
            return sender == null ? sender = new Sender() : sender;
        }

        Receiver createReceiverIfAbsent()
        {
            return receiver == null ? receiver = new Receiver() : receiver;
        }
    }

    static class AeronEntity
    {
        private long registrationId;

        long registrationId()
        {
            return registrationId;
        }

        void registrationId(final long registrationId)
        {
            this.registrationId = registrationId;
        }
    }

    static class Publisher extends AeronEntity
    {
        private long limit;
        private long position;

        void limit(final long limit)
        {
            this.limit = limit;
        }

        void position(final long position)
        {
            this.position = position;
        }

        long position()
        {
            return position;
        }

        long remainingWindow()
        {
            return limit - position;
        }
    }

    static class Sender extends AeronEntity
    {
        private long position;
        private long limit;

        void position(final long publisherPosition)
        {
            this.position = publisherPosition;
        }

        long position()
        {
            return position;
        }

        void limit(final long limit)
        {
            this.limit = limit;
        }

        long backlog(final long publisherPosition)
        {
            return publisherPosition - position;
        }

        long window()
        {
            return limit - position;
        }
    }

    static class Receiver extends AeronEntity
    {
        private long highWaterMark;
        private long position;

        void highWaterMark(final long highWaterMark)
        {
            this.highWaterMark = highWaterMark;
        }

        long highWaterMark()
        {
            return highWaterMark;
        }

        void position(final long position)
        {
            this.position = position;
        }

        long position()
        {
            return position;
        }
    }

    static class Subscriber
    {
        private final long position;

        Subscriber(final long position)
        {
            this.position = position;
        }

        long backlog(final long receiverHwm)
        {
            return receiverHwm - position;
        }
    }
}
