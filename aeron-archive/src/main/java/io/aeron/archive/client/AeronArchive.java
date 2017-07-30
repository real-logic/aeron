/*
 *  Copyright 2017 Real Logic Ltd.
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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.exceptions.TimeoutException;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archive.client.ControlResponseAdapter.dispatchDescriptor;

/**
 * Client for interacting with a local or remote Aeron Archive that records and replays message streams.
 * <p>
 * This client provides a simple interaction model which is mostly synchronous and may not be optimal.
 * The underlying components such as the {@link ArchiveProxy} and the {@link ControlResponsePoller} may be used
 * directly if a more asynchronous pattern of interaction is required.
 */
public final class AeronArchive implements AutoCloseable
{
    private static final int RESPONSE_FRAGMENT_LIMIT = 10;

    private final long messageTimeoutNs;
    private final Context context;
    private final Aeron aeron;
    private final ArchiveProxy archiveProxy;
    private final IdleStrategy idleStrategy;
    private final ControlResponsePoller controlResponsePoller;

    private AeronArchive(final Context context)
    {
        try
        {
            context.conclude();
            this.context = context;
            aeron = context.aeron();
            idleStrategy = context.idleStrategy();
            messageTimeoutNs = context.messageTimeoutNs();

            archiveProxy = context.archiveProxy();
            if (!archiveProxy.connect(context.controlResponseChannel(), context.controlResponseStreamId()))
            {
                throw new IllegalStateException("Cannot connect to aeron archive: " + context.controlRequestChannel());
            }

            controlResponsePoller = new ControlResponsePoller(
                aeron.addSubscription(context.controlResponseChannel(), context.controlResponseStreamId()),
                RESPONSE_FRAGMENT_LIMIT);
        }
        catch (final Exception ex)
        {
            context.close();
            throw ex;
        }
    }

    public void close()
    {
        context.close();
    }

    /**
     * Connect to an Aeron archive using a default {@link Context}.
     *
     * @return the newly created Aeron Archive client.
     */
    public static AeronArchive connect()
    {
        return new AeronArchive(new Context());
    }

    /**
     * Connect to an Aeron archive by providing a context. Before connecting {@link Context#conclude()} will be called.
     * If an exception occurs then {@link Context#close()} will be called.
     *
     * @param context for connection configuration.
     * @return the newly created Aeron Archive client.
     */
    public static AeronArchive connect(final Context context)
    {
        return new AeronArchive(context);
    }

    /**
     * The {@link ArchiveProxy} for send asynchronous messages to the connected archive.
     *
     * @return the {@link ArchiveProxy} for send asynchronous messages to the connected archive.
     */
    public ArchiveProxy archiveProxy()
    {
        return archiveProxy;
    }

    /**
     * Get the {@link ControlResponsePoller} for polling additional events on the control channel.
     *
     * @return the {@link ControlResponsePoller} for polling additional events on the control channel.
     */
    public ControlResponsePoller controlResponsePoller()
    {
        return controlResponsePoller;
    }

    /**
     * Add a {@link Publication} and set it up to be recorded.
     *
     * @param channel  for the publication.
     * @param streamId for the publication.
     * @return the {@link Publication} ready for use.
     */
    public Publication addRecordedPublication(final String channel, final int streamId)
    {
        startRecording(channel, streamId);

        final Publication publication = aeron.addPublication(channel, streamId);
        if (!publication.isOriginal())
        {
            publication.close();

            throw new IllegalStateException(
                "Publication already added for channel=" + channel + " streamId=" + streamId);
        }

        return publication;
    }

    /**
     * Add a {@link ExclusivePublication} and set it up to be recorded.
     *
     * @param channel  for the publication.
     * @param streamId for the publication.
     * @return the {@link ExclusivePublication} ready for use.
     */
    public ExclusivePublication addRecordedExclusivePublication(final String channel, final int streamId)
    {
        startRecording(channel, streamId);

        return aeron.addExclusivePublication(channel, streamId);
    }

    /**
     * Start recording a channel and stream pairing.
     *
     * @param channel  to be recorded.
     * @param streamId to be recorded.
     */
    public void startRecording(final String channel, final int streamId)
    {
        final String recordingChannel = channel.startsWith(IPC_CHANNEL) ? channel : SPY_PREFIX + channel;
        final long correlationId = aeron.nextCorrelationId();

        if (!archiveProxy.startRecording(recordingChannel, streamId, correlationId))
        {
            throw new IllegalStateException("Failed to send start recording request");
        }

        pollForResponse(correlationId, ControlResponseDecoder.class);
    }

    /**
     * Stop recording for a channel and stream pairing.
     *
     * @param channel  to stop recording for.
     * @param streamId to stop recording for.
     */
    public void stopRecording(final String channel, final int streamId)
    {
        final String recordingChannel = channel.startsWith(IPC_CHANNEL) ? channel : SPY_PREFIX + channel;
        final long correlationId = aeron.nextCorrelationId();

        if (!archiveProxy.stopRecording(recordingChannel, streamId, correlationId))
        {
            throw new IllegalStateException("Failed to send stop recording request");
        }

        pollForResponse(correlationId, ControlResponseDecoder.class);
    }

    /**
     * Replay a length of a recording from a position.
     *
     * @param recordingId    to be replayed.
     * @param position       from which the replay should be started.
     * @param length         of the stream to be replayed.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @return the {@link Subscription} for consuming the replay.
     */
    public Subscription replay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId)
    {
        final long correlationId = aeron.nextCorrelationId();

        if (!archiveProxy.replay(recordingId, position, length, replayChannel, replayStreamId, correlationId))
        {
            throw new IllegalStateException("Failed to send replay request");
        }

        pollForResponse(correlationId, ControlResponseDecoder.class);

        return aeron.addSubscription(replayChannel, replayStreamId);
    }

    /**
     * List all recording descriptors from a recording id with a limit of record count.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param fromRecordingId at which to begin the listing.
     * @param recordCount     to limit for each query.
     * @param consumer        to which the descriptors are dispatched.
     * @return the number of descriptors found and consumed.
     */
    public int listRecordings(
        final long fromRecordingId, final int recordCount, final RecordingDescriptorConsumer consumer)
    {
        final long correlationId = aeron.nextCorrelationId();

        if (!archiveProxy.listRecordings(fromRecordingId, recordCount, correlationId))
        {
            throw new IllegalStateException("Failed to send list recordings request");
        }

        return pollForDescriptors(correlationId, recordCount, consumer);
    }

    /**
     * List recording descriptors from a recording id with a limit of record count for a given channel and stream id.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param fromRecordingId at which to begin the listing.
     * @param recordCount     to limit for each query.
     * @param channel         to match.
     * @param streamId        to match.
     * @param consumer        to which the descriptors are dispatched.
     * @return the number of descriptors found and consumed.
     */
    public int listRecordingsForUri(
        final long fromRecordingId,
        final int recordCount,
        final String channel,
        final int streamId,
        final RecordingDescriptorConsumer consumer)
    {
        final long correlationId = aeron.nextCorrelationId();

        if (!archiveProxy.listRecordingsForUri(fromRecordingId, recordCount, channel, streamId, correlationId))
        {
            throw new IllegalStateException("Failed to send list recordings request");
        }

        return pollForDescriptors(correlationId, recordCount, consumer);
    }

    private void pollForResponse(final long expectedCorrelationId, final Class expectedMessage)
    {
        final long deadline = System.nanoTime() + messageTimeoutNs;
        final ControlResponsePoller poller = controlResponsePoller;
        idleStrategy.reset();

        while (true)
        {
            while (poller.poll() <= 0 && !poller.isPollComplete())
            {
                if (System.nanoTime() > deadline)
                {
                    throw new TimeoutException(
                        "Waiting for correlationId=" + expectedCorrelationId + " type=" + expectedMessage.getName());
                }

                idleStrategy.idle();
            }

            if (poller.correlationId() != expectedCorrelationId)
            {
                continue;
            }

            if (poller.templateId() == ControlResponseDecoder.TEMPLATE_ID)
            {
                final ControlResponseCode code = poller.controlResponseDecoder().code();
                switch (code)
                {
                    case OK:
                        return;

                    case ERROR:
                        throw new IllegalStateException("correlationId=" + expectedCorrelationId +
                            " error: " + poller.controlResponseDecoder().errorMessage());

                    default:
                        throw new IllegalStateException("Unexpected code: " + code);
                }
            }
            else
            {
                throw new IllegalStateException("Unknown response: templateId=" + poller.templateId());
            }
        }
    }

    private int pollForDescriptors(
        final long expectedCorrelationId, final int recordCount, final RecordingDescriptorConsumer consumer)
    {
        int count = 0;
        final long deadline = System.nanoTime() + messageTimeoutNs;
        final ControlResponsePoller poller = controlResponsePoller;
        idleStrategy.reset();

        while (true)
        {
            while (poller.poll() <= 0 && !poller.isPollComplete())
            {
                if (System.nanoTime() > deadline)
                {
                    throw new TimeoutException(
                        "Waiting for recording descriptors correlationId=" + expectedCorrelationId);
                }

                idleStrategy.idle();
            }

            if (poller.correlationId() != expectedCorrelationId)
            {
                continue;
            }

            switch (poller.templateId())
            {
                case RecordingDescriptorDecoder.TEMPLATE_ID:
                    dispatchDescriptor(poller.recordingDescriptorDecoder(), consumer);
                    if (++count >= recordCount)
                    {
                        return count;
                    }
                    break;

                case ControlResponseDecoder.TEMPLATE_ID:
                    final ControlResponseCode code = poller.controlResponseDecoder().code();
                    switch (code)
                    {
                        case RECORDING_UNKNOWN:
                            return count;

                        case ERROR:
                            throw new IllegalStateException("correlationId=" + expectedCorrelationId +
                                " error: " + poller.controlResponseDecoder().errorMessage());

                        default:
                            throw new IllegalStateException("Unexpected code: " + code);
                    }

                default:
                    throw new IllegalStateException("Unknown response: templateId=" + poller.templateId());
            }
        }
    }

    /**
     * Common configuration properties for communicating with an Aeron archive.
     */
    public static class Configuration
    {
        /**
         * Timeout when waiting on a message to be sent or received.
         */
        public static final String MESSAGE_TIMEOUT_PROP_NAME = "aeron.archive.message.timeout";

        /**
         * Default to 5 seconds in nanoseconds.
         */
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Channel for sending control messages to an archive.
         */
        public static final String CONTROL_CHANNEL_PROP_NAME = "aeron.archive.control.channel";

        /**
         * Default to localhost.
         */
        public static final String CONTROL_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8010";

        /**
         * Stream id within a channel for sending control messages to an archive.
         */
        public static final String CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.control.stream.id";

        /**
         * Default to stream id of 0.
         */
        public static final int CONTROL_STREAM_ID_DEFAULT = 0;

        /**
         * Channel for receiving control response messages from an archive.
         */
        public static final String CONTROL_RESPONSE_CHANNEL_PROP_NAME = "aeron.archive.control.response.channel";

        /**
         * Default to localhost.
         */
        public static final String CONTROL_RESPONSE_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8020";

        /**
         * Stream id within a channel for receiving control messages from an archive.
         */
        public static final String CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

        /**
         * Default to stream id of 0.
         */
        public static final int CONTROL_RESPONSE_STREAM_ID_DEFAULT = 0;

        /**
         * Channel for receiving events related to the progress of recordings from an archive.
         */
        public static final String RECORDING_EVENTS_CHANNEL_PROP_NAME = "aeron.archive.recording.events.channel";

        /**
         * Defaults to localhost.
         */
        public static final String RECORDING_EVENTS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8011";

        /**
         * Stream id within a channel for receiving events related to the progress of recordings from an archive.
         */
        public static final String RECORDING_EVENTS_STREAM_ID_PROP_NAME = "aeron.archive.recording.events.stream.id";

        /**
         * Default to a stream id of 0.
         */
        public static final int RECORDING_EVENTS_STREAM_ID_DEFAULT = 0;

        /**
         * The timeout in nanoseconds to wait for a message.
         *
         * @return timeout in nanoseconds to wait for a message.
         */
        public static long messageTimeoutNs()
        {
            return Long.getLong(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The value {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         */
        public static String controlChannel()
        {
            return System.getProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_STREAM_ID_DEFAULT} if set.
         *
         * @return {@link #CONTROL_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_STREAM_ID_DEFAULT} if set.
         */
        public static int controlStreamId()
        {
            return Integer.getInteger(CONTROL_STREAM_ID_PROP_NAME, CONTROL_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} if set.
         *
         * @return {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} if set.
         */
        public static String controlResponseChannel()
        {
            return System.getProperty(CONTROL_RESPONSE_CHANNEL_PROP_NAME, CONTROL_RESPONSE_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_RESPONSE_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_RESPONSE_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_STREAM_ID_PROP_NAME} if set.
         */
        public static int controlResponseStreamId()
        {
            return Integer.getInteger(CONTROL_RESPONSE_STREAM_ID_PROP_NAME, CONTROL_RESPONSE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #RECORDING_EVENTS_CHANNEL_DEFAULT} or system property
         * {@link #RECORDING_EVENTS_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #RECORDING_EVENTS_CHANNEL_DEFAULT} or system property
         * {@link #RECORDING_EVENTS_CHANNEL_PROP_NAME} if set.
         */
        public static String recordingEventsChannel()
        {
            return System.getProperty(RECORDING_EVENTS_CHANNEL_PROP_NAME, RECORDING_EVENTS_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #RECORDING_EVENTS_STREAM_ID_DEFAULT} or system property
         * {@link #RECORDING_EVENTS_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #RECORDING_EVENTS_STREAM_ID_DEFAULT} or system property
         * {@link #RECORDING_EVENTS_STREAM_ID_PROP_NAME} if set.
         */
        public static int recordingEventsStreamId()
        {
            return Integer.getInteger(RECORDING_EVENTS_STREAM_ID_PROP_NAME, RECORDING_EVENTS_STREAM_ID_DEFAULT);
        }
    }

    /**
     * Specialised configuration options for communicating with an Aeron Archive.
     */
    public static class Context implements AutoCloseable
    {
        private long messageTimeoutNs = Configuration.messageTimeoutNs();
        private String controlRequestChannel = Configuration.controlChannel();
        private int controlRequestStreamId = Configuration.controlStreamId();
        private String controlResponseChannel = Configuration.controlResponseChannel();
        private int controlResponseStreamId = Configuration.controlResponseStreamId();
        private Aeron aeron;
        private ArchiveProxy archiveProxy;
        private IdleStrategy idleStrategy;

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        public void conclude()
        {
            if (null == aeron)
            {
                aeron = Aeron.connect();
            }

            if (null == idleStrategy)
            {
                idleStrategy = new BackoffIdleStrategy(1, 10, 1, 1);
            }

            if (null == archiveProxy)
            {
                archiveProxy = new ArchiveProxy(
                    aeron.addPublication(controlRequestChannel, controlRequestStreamId),
                    idleStrategy,
                    messageTimeoutNs,
                    ArchiveProxy.DEFAULT_MAX_RETRY_ATTEMPTS);
            }
        }

        /**
         * Set the message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @param messageTimeoutNs to wait for sending or receiving a message.
         * @return this for a fluent API.
         */
        public Context messageTimeoutNs(final long messageTimeoutNs)
        {
            this.messageTimeoutNs = messageTimeoutNs;
            return this;
        }

        /**
         * The message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @return the message timeout in nanoseconds to wait for sending or receiving a message.
         */
        public long messageTimeoutNs()
        {
            return messageTimeoutNs;
        }

        /**
         * Set the channel parameter for the control request channel.
         *
         * @param channel parameter for the control request channel.
         * @return this for a fluent API.
         */
        public Context controlRequestChannel(final String channel)
        {
            controlRequestChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the control request channel.
         *
         * @return the channel parameter for the control request channel.
         */
        public String controlRequestChannel()
        {
            return controlRequestChannel;
        }

        /**
         * Set the stream id for the control request channel.
         *
         * @param streamId for the control request channel.
         * @return this for a fluent API
         */
        public Context controlRequestStreamId(final int streamId)
        {
            controlRequestStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the control request channel.
         *
         * @return the stream id for the control request channel.
         */
        public int controlRequestStreamId()
        {
            return controlRequestStreamId;
        }

        /**
         * Set the channel parameter for the control response channel.
         *
         * @param channel parameter for the control response channel.
         * @return this for a fluent API.
         */
        public Context controlResponseChannel(final String channel)
        {
            controlResponseChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the control response channel.
         *
         * @return the channel parameter for the control response channel.
         */
        public String controlResponseChannel()
        {
            return controlResponseChannel;
        }

        /**
         * Set the stream id for the control response channel.
         *
         * @param streamId for the control response channel.
         * @return this for a fluent API
         */
        public Context controlResponseStreamId(final int streamId)
        {
            controlResponseStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the control response channel.
         *
         * @return the stream id for the control response channel.
         */
        public int controlResponseStreamId()
        {
            return controlResponseStreamId;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link #close()} method is called.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Set the {@link ArchiveProxy} for sending control messages to an archive. If one is not provided then one
         * will be created.
         *
         * @param archiveProxy for sending control messages to an archive.
         * @return this for a fluent API.
         */
        public Context archiveProxy(final ArchiveProxy archiveProxy)
        {
            this.archiveProxy = archiveProxy;
            return this;
        }

        /**
         * Get the {@link ArchiveProxy} for sending control messages to an archive.
         *
         * @return the {@link ArchiveProxy} for sending control messages to an archive.
         */
        public ArchiveProxy archiveProxy()
        {
            return archiveProxy;
        }

        /**
         * Set the {@link IdleStrategy} used when waiting for responses.
         *
         * @param idleStrategy used when waiting for responses.
         * @return this for a fluent API.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} used when waiting for responses.
         *
         * @return the {@link IdleStrategy} used when waiting for responses.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        /**
         * Close the context and free applicable resources.
         */
        public void close()
        {
            aeron.close();
        }
    }
}
