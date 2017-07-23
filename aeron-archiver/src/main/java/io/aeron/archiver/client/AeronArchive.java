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
package io.aeron.archiver.client;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

/**
 * Client for interacting with a local or remote Aeron Archive that records and replays message streams.
 * <p>
 * This client provides a simple interaction model which is mostly synchronous and may not be optimal.
 * The underlying components such as the {@link ArchiveProxy} and the {@link ControlResponseAdapter} may be used
 * directly is a more asynchronous pattern of interaction is required.
 */
public final class AeronArchive implements AutoCloseable, ControlResponseListener
{
    private static final int RESPONSE_FRAGMENT_LIMIT = 10;

    private final Context context;
    private final Aeron aeron;
    private final ArchiveProxy archiveProxy;
    private final IdleStrategy idleStrategy;
    private final ControlResponseAdapter controlResponseAdapter;
    private long expectedCorrelationId;
    private long receivedCorrelationId;
    private ControlResponseCode controlResponseCode;
    private String responseErrorMessage;

    private AeronArchive(final Context context)
    {
        try
        {
            context.conclude();
            this.context = context;
            aeron = context.aeron();
            idleStrategy = context.idleStrategy();

            archiveProxy = context.archiveProxy();
            if (!archiveProxy.connect(context.controlResponseChannel(), context.controlResponseStreamId()))
            {
                throw new IllegalStateException("Cannot connect to aeron archive: " + context.controlRequestChannel());
            }

            controlResponseAdapter = new ControlResponseAdapter(
                this,
                aeron.addSubscription(context.controlRequestChannel(), context.controlRequestStreamId),
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
     * Add a {@link Publication} and set it up to be recorded.
     *
     * @param channel  for the publication.
     * @param streamId for the publication.
     * @return the {@link Publication} ready for use.
     */
    public Publication addRecordedPublication(final String channel, final int streamId)
    {
        final long correlationId = aeron.nextCorrelationId();
        if (!archiveProxy.startRecording(channel, streamId, correlationId))
        {
            throw new IllegalStateException("Failed to send start recording request");
        }

        expectedCorrelationId = correlationId;
        pollForResponse();

        final Publication publication = aeron.addPublication(channel, streamId);
        if (!publication.isOriginal())
        {
            publication.close();

            throw new IllegalStateException(
                "Publication already added for channel=" + channel + " streamId=" + streamId);
        }

        return publication;
    }

    public void onResponse(final long correlationId, final ControlResponseCode code, final String errorMessage)
    {
        receivedCorrelationId = correlationId;
        controlResponseCode = code;
        responseErrorMessage = errorMessage;
    }

    public void onReplayStarted(final long correlationId, final long replayId)
    {

    }

    public void onReplayAborted(final long correlationId, final long stopPosition)
    {

    }

    public void onRecordingNotFound(final long correlationId, final long recordingId, final long maxRecordingId)
    {

    }

    public void onRecordingDescriptor(
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {

    }

    private void pollForResponse()
    {
        idleStrategy.reset();

        while (receivedCorrelationId != expectedCorrelationId && controlResponseAdapter.poll() <= 0)
        {
            idleStrategy.idle();
        }

        if (controlResponseCode != ControlResponseCode.OK)
        {
            throw new IllegalStateException(
                "Response code=" + controlResponseCode + " message=" + responseErrorMessage);
        }
    }

    /**
     * Common configuration properties for communicating with an Aeron archive.
     */
    public static class Configuration
    {
        /**
         * Channel for sending control messages to an archive.
         */
        public static final String CONTROL_REQUEST_CHANNEL_PROP_NAME = "aeron.archive.control.request.channel";

        /**
         * Default to localhost.
         */
        public static final String CONTROL_REQUEST_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8010";

        /**
         * Stream id within a channel for sending control messages to an archive.
         */
        public static final String CONTROL_REQUEST_STREAM_ID_PROP_NAME = "aeron.archive.control.request.stream.id";

        /**
         * Default to stream id of 0.
         */
        public static final int CONTROL_REQUEST_STREAM_ID_DEFAULT = 0;

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
         * The value {@link #CONTROL_REQUEST_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_REQUEST_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_REQUEST_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_REQUEST_CHANNEL_PROP_NAME} if set.
         */
        public static String controlRequestChannel()
        {
            return System.getProperty(CONTROL_REQUEST_CHANNEL_PROP_NAME, CONTROL_REQUEST_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_REQUEST_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_REQUEST_STREAM_ID_DEFAULT} if set.
         *
         * @return {@link #CONTROL_REQUEST_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_REQUEST_STREAM_ID_DEFAULT} if set.
         */
        public static int controlRequestStreamId()
        {
            return Integer.getInteger(CONTROL_REQUEST_STREAM_ID_PROP_NAME, CONTROL_REQUEST_STREAM_ID_DEFAULT);
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
        private String controlRequestChannel = Configuration.controlRequestChannel();
        private int controlRequestStreamId = Configuration.controlRequestStreamId();
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
                idleStrategy = new BackoffIdleStrategy(10, 10, 1, 1);
            }

            if (null == archiveProxy)
            {
                archiveProxy = new ArchiveProxy(
                    aeron.addPublication(controlRequestChannel, controlRequestStreamId),
                    idleStrategy,
                    ArchiveProxy.DEFAULT_CONNECT_TIMEOUT_NS,
                    ArchiveProxy.DEFAULT_MAX_RETRY_ATTEMPTS);
            }
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
