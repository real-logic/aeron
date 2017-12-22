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

import io.aeron.*;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.NoOpLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.aeron.archive.client.ArchiveProxy.DEFAULT_RETRY_ATTEMPTS;
import static io.aeron.driver.Configuration.*;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.SystemUtil.getSizeAsInt;

/**
 * Client for interacting with a local or remote Aeron Archive that records and replays message streams.
 * <p>
 * This client provides a simple interaction model which is mostly synchronous and may not be optimal.
 * The underlying components such as the {@link ArchiveProxy} and the {@link ControlResponsePoller} or
 * {@link RecordingDescriptorPoller} may be used directly if a more asynchronous interaction is required.
 * <p>
 * Note: This class is threadsafe but the lock can be elided for single threaded access via {@link Context#lock(Lock)}
 * being set to {@link NoOpLock}.
 */
public class AeronArchive implements AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 10;

    private final long controlSessionId;
    private final long messageTimeoutNs;
    private final Context context;
    private final Aeron aeron;
    private final ArchiveProxy archiveProxy;
    private final IdleStrategy idleStrategy;
    private final ControlResponsePoller controlResponsePoller;
    private final RecordingDescriptorPoller recordingDescriptorPoller;
    private final Lock lock;
    private final NanoClock nanoClock;

    AeronArchive(final Context ctx)
    {
        Subscription subscription = null;
        Publication publication = null;
        try
        {
            ctx.conclude();

            context = ctx;
            aeron = ctx.aeron();
            idleStrategy = ctx.idleStrategy();
            messageTimeoutNs = ctx.messageTimeoutNs();
            lock = ctx.lock();
            nanoClock = aeron.context().nanoClock();

            subscription = aeron.addSubscription(ctx.controlResponseChannel(), ctx.controlResponseStreamId());
            controlResponsePoller = new ControlResponsePoller(subscription);

            publication = aeron.addExclusivePublication(ctx.controlRequestChannel(), ctx.controlRequestStreamId());
            archiveProxy = new ArchiveProxy(
                publication, idleStrategy, nanoClock, messageTimeoutNs, DEFAULT_RETRY_ATTEMPTS);

            final long correlationId = aeron.nextCorrelationId();
            if (!archiveProxy.connect(ctx.controlResponseChannel(), ctx.controlResponseStreamId(), correlationId))
            {
                throw new IllegalStateException("Cannot connect to aeron archive: " + ctx.controlRequestChannel());
            }

            controlSessionId = pollForConnected(correlationId);
            recordingDescriptorPoller = new RecordingDescriptorPoller(subscription, FRAGMENT_LIMIT, controlSessionId);
        }
        catch (final Exception ex)
        {
            if (!ctx.ownsAeronClient())
            {
                CloseHelper.quietClose(subscription);
                CloseHelper.quietClose(publication);
            }

            CloseHelper.quietClose(ctx);

            throw ex;
        }
    }

    /**
     * Notify the archive that this control session is closed so it can promptly release resources then close the
     * local resources associated with the client.
     */
    public void close()
    {
        lock.lock();
        try
        {
            archiveProxy.closeSession(controlSessionId);

            if (!context.ownsAeronClient())
            {
                controlResponsePoller.subscription().close();
                archiveProxy.publication().close();
            }

            context.close();
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Connect to an Aeron archive using a default {@link Context}. This will create a control session.
     *
     * @return the newly created Aeron Archive client.
     */
    public static AeronArchive connect()
    {
        return new AeronArchive(new Context());
    }

    /**
     * Connect to an Aeron archive by providing a {@link Context}. This will create a control session.
     * <p>
     * Before connecting {@link Context#conclude()} will be called.
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
     * The control session id allocated for this connection to the archive.
     *
     * @return control session id allocated for this connection to the archive.
     */
    public long controlSessionId()
    {
        return controlSessionId;
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
     * Get the {@link RecordingDescriptorPoller} for polling recording descriptors on the control channel.
     *
     * @return the {@link RecordingDescriptorPoller} for polling recording descriptors on the control channel.
     */
    public RecordingDescriptorPoller recordingDescriptorPoller()
    {
        return recordingDescriptorPoller;
    }

    /**
     * Poll the response stream once for an error. If another message is present then it will be skipped over
     * so only call when not expecting another response.
     *
     * @return the error String otherwise null if no error is found.
     */
    public String pollForErrorResponse()
    {
        lock.lock();
        try
        {
            if (controlResponsePoller.poll() != 0 && controlResponsePoller.isPollComplete())
            {
                if (controlResponsePoller.templateId() == ControlResponseDecoder.TEMPLATE_ID &&
                    controlResponsePoller.code() == ControlResponseCode.ERROR)
                {
                    return controlResponsePoller.errorMessage();
                }
            }

            return null;
        }
        finally
        {
            lock.unlock();
        }
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
        lock.lock();
        try
        {
            startRecording(channel, streamId, SourceLocation.LOCAL);

            final Publication publication = aeron.addPublication(channel, streamId);
            if (!publication.isOriginal())
            {
                publication.close();

                throw new IllegalStateException(
                    "Publication already added for channel=" + channel + " streamId=" + streamId);
            }

            return publication;
        }
        finally
        {
            lock.unlock();
        }
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
        lock.lock();
        try
        {
            startRecording(channel, streamId, SourceLocation.LOCAL);

            return aeron.addExclusivePublication(channel, streamId);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Start recording a channel and stream pairing.
     *
     * @param channel        to be recorded.
     * @param streamId       to be recorded.
     * @param sourceLocation of the publication to be recorded.
     * @return the correlationId used to identify the request.
     */
    public long startRecording(final String channel, final int streamId, final SourceLocation sourceLocation)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.startRecording(channel, streamId, sourceLocation, correlationId, controlSessionId))
            {
                throw new IllegalStateException("Failed to send start recording request");
            }

            pollForResponse(correlationId);

            return correlationId;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Stop recording for a channel and stream pairing.
     *
     * @param channel  to stop recording for.
     * @param streamId to stop recording for.
     */
    public void stopRecording(final String channel, final int streamId)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.stopRecording(channel, streamId, correlationId, controlSessionId))
            {
                throw new IllegalStateException("Failed to send stop recording request");
            }

            pollForResponse(correlationId);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Start a replay for a length in bytes of a recording from a position.
     *
     * @param recordingId    to be replayed.
     * @param position       from which the replay should be started.
     * @param length         of the stream to be replayed.
     * @param replayChannel  to which the replay should be sent.
     * @param replayStreamId to which the replay should be sent.
     * @return the id of the replay session.
     */
    public long startReplay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.replay(
                recordingId,
                position,
                length,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId))
            {
                throw new IllegalStateException("Failed to send replay request");
            }

            return pollForResponse(correlationId);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Stop a replay session.
     *
     * @param replaySessionId to stop replay for.
     */
    public void stopReplay(final long replaySessionId)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.stopReplay(replaySessionId, correlationId, controlSessionId))
            {
                throw new IllegalStateException("Failed to send stop recording request");
            }

            pollForResponse(correlationId);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Replay a length in bytes of a recording from a position and for convenience create a {@link Subscription}
     * to receive the replay.
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
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.replay(
                recordingId,
                position,
                length,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId))
            {
                throw new IllegalStateException("Failed to send replay request");
            }

            pollForResponse(correlationId);

            return aeron.addSubscription(replayChannel, replayStreamId);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Replay a length in bytes of a recording from a position and for convenience create a {@link Subscription}
     * to receive the replay.
     *
     * @param recordingId             to be replayed.
     * @param position                from which the replay should be started.
     * @param length                  of the stream to be replayed.
     * @param replayChannel           to which the replay should be sent.
     * @param replayStreamId          to which the replay should be sent.
     * @param availableImageHandler   to be called when the replay image becomes available.
     * @param unavailableImageHandler to be called when the replay image goes unavailable.
     * @return the {@link Subscription} for consuming the replay.
     */
    public Subscription replay(
        final long recordingId,
        final long position,
        final long length,
        final String replayChannel,
        final int replayStreamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.replay(
                recordingId,
                position,
                length,
                replayChannel,
                replayStreamId,
                correlationId,
                controlSessionId))
            {
                throw new IllegalStateException("Failed to send replay request");
            }

            pollForResponse(correlationId);

            return aeron.addSubscription(replayChannel, replayStreamId, availableImageHandler, unavailableImageHandler);
        }
        finally
        {
            lock.unlock();
        }
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
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.listRecordings(fromRecordingId, recordCount, correlationId, controlSessionId))
            {
                throw new IllegalStateException("Failed to send list recordings request");
            }

            return pollForDescriptors(correlationId, recordCount, consumer);
        }
        finally
        {
            lock.unlock();
        }
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
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.listRecordingsForUri(
                fromRecordingId,
                recordCount,
                channel,
                streamId,
                correlationId,
                controlSessionId))
            {
                throw new IllegalStateException("Failed to send list recordings request");
            }

            return pollForDescriptors(correlationId, recordCount, consumer);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * List all recording descriptors from a recording id with a limit of record count.
     * <p>
     * If the recording id is greater than the largest known id then nothing is returned.
     *
     * @param recordingId at which to begin the listing.
     * @param consumer    to which the descriptors are dispatched.
     * @return the number of descriptors found and consumed.
     */
    public int listRecording(final long recordingId, final RecordingDescriptorConsumer consumer)
    {
        lock.lock();
        try
        {
            final long correlationId = aeron.nextCorrelationId();

            if (!archiveProxy.listRecording(recordingId, correlationId, controlSessionId))
            {
                throw new IllegalStateException("Failed to send list recording request");
            }

            return pollForDescriptors(correlationId, 1, consumer);
        }
        finally
        {
            lock.unlock();
        }
    }

    private long pollForConnected(final long expectedCorrelationId)
    {
        final long deadlineNs = nanoClock.nanoTime() + messageTimeoutNs;
        final ControlResponsePoller poller = controlResponsePoller;
        idleStrategy.reset();

        while (!poller.subscription().isConnected())
        {
            if (nanoClock.nanoTime() > deadlineNs)
            {
                throw new TimeoutException("Failed to establish response connection");
            }

            idleStrategy.idle();
        }

        while (true)
        {
            while (true)
            {
                final int fragments = poller.poll();

                if (poller.isPollComplete())
                {
                    break;
                }

                if (fragments > 0)
                {
                    continue;
                }

                if (nanoClock.nanoTime() > deadlineNs)
                {
                    throw new TimeoutException("Waiting for correlationId=" + expectedCorrelationId);
                }

                idleStrategy.idle();
            }

            if (poller.correlationId() != expectedCorrelationId ||
                poller.templateId() != ControlResponseDecoder.TEMPLATE_ID)
            {
                continue;
            }

            final ControlResponseCode code = poller.code();
            if (code != ControlResponseCode.OK)
            {
                if (code == ControlResponseCode.ERROR)
                {
                    throw new IllegalStateException("Error: " + poller.errorMessage());
                }

                throw new IllegalStateException("Unexpected response: code=" + code);
            }

            return poller.controlSessionId();
        }
    }

    private long pollForResponse(final long expectedCorrelationId)
    {
        final long deadlineNs = nanoClock.nanoTime() + messageTimeoutNs;
        final ControlResponsePoller poller = controlResponsePoller;
        idleStrategy.reset();

        while (true)
        {
            while (true)
            {
                final int fragments = poller.poll();

                if (poller.isPollComplete())
                {
                    break;
                }

                if (fragments > 0)
                {
                    continue;
                }

                if (!poller.subscription().isConnected())
                {
                    throw new IllegalStateException("Subscription to archive is not connected");
                }

                if (nanoClock.nanoTime() > deadlineNs)
                {
                    throw new TimeoutException("Waiting for correlationId=" + expectedCorrelationId);
                }

                idleStrategy.idle();
            }

            if (poller.controlSessionId() != controlSessionId ||
                poller.templateId() != ControlResponseDecoder.TEMPLATE_ID)
            {
                continue;
            }

            if (poller.code() == ControlResponseCode.ERROR)
            {
                throw new IllegalStateException("response for expectedCorrelationId=" + expectedCorrelationId +
                    ", error: " + poller.errorMessage());
            }

            final ControlResponseCode code = poller.code();
            if (ControlResponseCode.OK != code)
            {
                throw new IllegalStateException("Unexpected response code: " + code);
            }

            if (poller.correlationId() == expectedCorrelationId)
            {
                return poller.relevantId();
            }
        }
    }

    private int pollForDescriptors(
        final long expectedCorrelationId, final int recordCount, final RecordingDescriptorConsumer consumer)
    {
        final long deadlineNs = nanoClock.nanoTime() + messageTimeoutNs;
        final RecordingDescriptorPoller poller = recordingDescriptorPoller;
        poller.reset(expectedCorrelationId, recordCount, consumer);
        idleStrategy.reset();

        while (true)
        {
            final int fragments = poller.poll();

            if (poller.isDispatchComplete())
            {
                return recordCount - poller.remainingRecordCount();
            }

            if (fragments > 0)
            {
                continue;
            }

            if (!poller.subscription().isConnected())
            {
                throw new IllegalStateException("Subscription to archive is not connected");
            }

            if (nanoClock.nanoTime() > deadlineNs)
            {
                throw new TimeoutException(
                    "Waiting for recording descriptors: correlationId=" + expectedCorrelationId);
            }

            idleStrategy.idle();
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
         * Timeout when waiting on a message to be sent or received. Default to 5 seconds in nanoseconds.
         */
        public static final long MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * Channel for sending control messages to an archive.
         */
        public static final String CONTROL_CHANNEL_PROP_NAME = "aeron.archive.control.channel";

        /**
         * Channel for sending control messages to an archive. Default to localhost:8010.
         */
        public static final String CONTROL_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8010";

        /**
         * Stream id within a channel for sending control messages to an archive.
         */
        public static final String CONTROL_STREAM_ID_PROP_NAME = "aeron.archive.control.stream.id";

        /**
         * Stream id within a channel for sending control messages to an archive. Default to stream id of 0.
         */
        public static final int CONTROL_STREAM_ID_DEFAULT = 0;

        /**
         * Channel for receiving control response messages from an archive.
         */
        public static final String CONTROL_RESPONSE_CHANNEL_PROP_NAME = "aeron.archive.control.response.channel";

        /**
         * Channel for receiving control response messages from an archive. Default to localhost:8020.
         */
        public static final String CONTROL_RESPONSE_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8020";

        /**
         * Stream id within a channel for receiving control messages from an archive.
         */
        public static final String CONTROL_RESPONSE_STREAM_ID_PROP_NAME = "aeron.archive.control.response.stream.id";

        /**
         * Stream id within a channel for receiving control messages from an archive. Default to stream id of 0.
         */
        public static final int CONTROL_RESPONSE_STREAM_ID_DEFAULT = 0;

        /**
         * Channel for receiving progress events of recordings from an archive.
         */
        public static final String RECORDING_EVENTS_CHANNEL_PROP_NAME = "aeron.archive.recording.events.channel";

        /**
         * Channel for receiving progress events of recordings from an archive. Defaults to localhost:8011.
         * For production it is recommended that multicast or dynamic multi-destination-cast (MDC) is used to allow
         * for dynamic subscribers.
         */
        public static final String RECORDING_EVENTS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8011";

        /**
         * Stream id within a channel for receiving progress of recordings from an archive.
         */
        public static final String RECORDING_EVENTS_STREAM_ID_PROP_NAME = "aeron.archive.recording.events.stream.id";

        /**
         * Stream id within a channel for receiving progress of recordings from an archive. Default to a stream id of 0.
         */
        public static final int RECORDING_EVENTS_STREAM_ID_DEFAULT = 0;

        /**
         * Term length for control streams.
         */
        private static final String CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME = "aeron.archive.control.term.buffer.length";

        /**
         * Low term length for control channel reflects expected low bandwidth usage.
         */
        private static final int CONTROL_TERM_BUFFER_LENGTH_DEFAULT = 64 * 1024;

        /**
         * Term length for control streams.
         */
        private static final String CONTROL_MTU_LENGTH_PARAM_NAME = "aeron.archive.control.mtu.length";

        /**
         * MTU to reflect default control term length.
         */
        private static final int CONTROL_MTU_LENGTH_DEFAULT = 4 * 1024;

        /**
         * The timeout in nanoseconds to wait for a message.
         *
         * @return timeout in nanoseconds to wait for a message.
         * @see #MESSAGE_TIMEOUT_PROP_NAME
         */
        public static long messageTimeoutNs()
        {
            return getDurationInNanos(MESSAGE_TIMEOUT_PROP_NAME, MESSAGE_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Term buffer length to be used for control request and response streams.
         *
         * @return term buffer length to be used for control request and response streams.
         * @see #CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME
         */
        public static int controlTermBufferLength()
        {
            return getSizeAsInt(CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME, CONTROL_TERM_BUFFER_LENGTH_DEFAULT);
        }

        /**
         * MTU length to be used for control request and response streams.
         *
         * @return MTU length to be used for control request and response streams.
         * @see #CONTROL_MTU_LENGTH_PARAM_NAME
         */
        public static int controlMtuLength()
        {
            return getSizeAsInt(CONTROL_MTU_LENGTH_PARAM_NAME, CONTROL_MTU_LENGTH_DEFAULT);
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
         * {@link #CONTROL_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_STREAM_ID_DEFAULT} or system property
         * {@link #CONTROL_STREAM_ID_PROP_NAME} if set.
         */
        public static int controlStreamId()
        {
            return Integer.getInteger(CONTROL_STREAM_ID_PROP_NAME, CONTROL_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_RESPONSE_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_RESPONSE_CHANNEL_PROP_NAME} if set.
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
        private String recordingEventsChannel = AeronArchive.Configuration.recordingEventsChannel();
        private int recordingEventsStreamId = AeronArchive.Configuration.recordingEventsStreamId();
        private String controlRequestChannel = Configuration.controlChannel();
        private int controlRequestStreamId = Configuration.controlStreamId();
        private String controlResponseChannel = Configuration.controlResponseChannel();
        private int controlResponseStreamId = Configuration.controlResponseStreamId();
        private int controlTermBufferLength = Configuration.controlTermBufferLength();
        private int controlMtuLength = Configuration.controlMtuLength();
        private IdleStrategy idleStrategy;
        private Lock lock;
        private String aeronDirectoryName = CommonContext.AERON_DIR_PROP_DEFAULT;
        private Aeron aeron;
        private boolean ownsAeronClient = false;

        /**
         * Conclude configuration by setting up defaults when specifics are not provided.
         */
        public void conclude()
        {
            if (null == aeron)
            {
                aeron = Aeron.connect(new Aeron.Context()
                    .aeronDirectoryName(aeronDirectoryName));

                ownsAeronClient = true;
            }

            if (null == idleStrategy)
            {
                idleStrategy = new BackoffIdleStrategy(
                    IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
            }

            if (null == lock)
            {
                lock = new ReentrantLock();
            }

            final ChannelUri uri = ChannelUri.parse(controlRequestChannel);
            uri.put(CommonContext.TERM_LENGTH_PARAM_NAME, Integer.toString(controlTermBufferLength));
            uri.put(CommonContext.MTU_LENGTH_PARAM_NAME, Integer.toString(controlMtuLength));
            controlRequestChannel = uri.toString();
        }

        /**
         * Set the message timeout in nanoseconds to wait for sending or receiving a message.
         *
         * @param messageTimeoutNs to wait for sending or receiving a message.
         * @return this for a fluent API.
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
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
         * @see Configuration#MESSAGE_TIMEOUT_PROP_NAME
         */
        public long messageTimeoutNs()
        {
            return messageTimeoutNs;
        }

        /**
         * Get the channel URI on which the recording events publication will publish.
         *
         * @return the channel URI on which the recording events publication will publish.
         */
        public String recordingEventsChannel()
        {
            return recordingEventsChannel;
        }

        /**
         * Set the channel URI on which the recording events publication will publish.
         * <p>
         * To support dynamic subscribers then this can be set to multicast or MDC (Multi-Destination-Cast) if
         * multicast cannot be supported for on the available the network infrastructure.
         *
         * @param recordingEventsChannel channel URI on which the recording events publication will publish.
         * @return this for a fluent API.
         * @see io.aeron.CommonContext#MDC_CONTROL_PARAM_NAME
         */
        public Context recordingEventsChannel(final String recordingEventsChannel)
        {
            this.recordingEventsChannel = recordingEventsChannel;
            return this;
        }

        /**
         * Get the stream id on which the recording events publication will publish.
         *
         * @return the stream id on which the recording events publication will publish.
         */
        public int recordingEventsStreamId()
        {
            return recordingEventsStreamId;
        }

        /**
         * Set the stream id on which the recording events publication will publish.
         *
         * @param recordingEventsStreamId stream id on which the recording events publication will publish.
         * @return this for a fluent API.
         */
        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Set the channel parameter for the control request channel.
         *
         * @param channel parameter for the control request channel.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
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
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
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
         * @see Configuration#CONTROL_STREAM_ID_PROP_NAME
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
         * @see Configuration#CONTROL_STREAM_ID_PROP_NAME
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
         * @see Configuration#CONTROL_RESPONSE_CHANNEL_PROP_NAME
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
         * @see Configuration#CONTROL_RESPONSE_CHANNEL_PROP_NAME
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
         * @see Configuration#CONTROL_RESPONSE_STREAM_ID_PROP_NAME
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
         * @see Configuration#CONTROL_RESPONSE_STREAM_ID_PROP_NAME
         */
        public int controlResponseStreamId()
        {
            return controlResponseStreamId;
        }

        /**
         * Set the term buffer length for the control stream.
         *
         * @param controlTermBufferLength for the control stream.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME
         */
        public Context controlTermBufferLength(final int controlTermBufferLength)
        {
            this.controlTermBufferLength = controlTermBufferLength;
            return this;
        }

        /**
         * Get the term buffer length for the control steam.
         *
         * @return the term buffer length for the control steam.
         * @see Configuration#CONTROL_TERM_BUFFER_LENGTH_PARAM_NAME
         */
        public int controlTermBufferLength()
        {
            return controlTermBufferLength;
        }

        /**
         * Set the MTU length for the control stream.
         *
         * @param controlMtuLength for the control stream.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_MTU_LENGTH_PARAM_NAME
         */
        public Context controlMtuLength(final int controlMtuLength)
        {
            this.controlMtuLength = controlMtuLength;
            return this;
        }

        /**
         * Get the MTU length for the control steam.
         *
         * @return the MTU length for the control steam.
         * @see Configuration#CONTROL_MTU_LENGTH_PARAM_NAME
         */
        public int controlMtuLength()
        {
            return controlMtuLength;
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
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link AeronArchive#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
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
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * The {@link Lock} that is used to provide mutual exclusion in the {@link AeronArchive} client.
         * <p>
         * If the {@link AeronArchive} is used from only a single thread then the lock can be set to
         * {@link NoOpLock} to elide the lock overhead.
         *
         * @param lock that is used to provide mutual exclusion in the {@link AeronArchive} client.
         * @return this for a fluent API.
         */
        public Context lock(final Lock lock)
        {
            this.lock = lock;
            return this;
        }

        /**
         * Get the {@link Lock} that is used to provide mutual exclusion in the {@link AeronArchive} client.
         *
         * @return the {@link Lock} that is used to provide mutual exclusion in the {@link AeronArchive} client.
         */
        public Lock lock()
        {
            return lock;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                aeron.close();
            }
        }
    }
}
