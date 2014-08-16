/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.conductor.*;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.IoUtil.mapExistingFile;

/**
 * Encapsulation of media driver and client for source and receiver construction
 */
public final class Aeron implements AutoCloseable, Runnable
{
    private static final int CONDUCTOR_TICKS_PER_WHEEL = 1024;
    private static final int CONDUCTOR_TICK_DURATION_US = 10 * 1000;

    private static final long UNSET_TIMEOUT = -1;
    private static final long DEFAULT_MEDIA_DRIVER_TIMEOUT = 10_000;

    private final ClientConductor conductor;
    private final Context savedCtx;

    private Aeron(final Context ctx)
    {
        try
        {
            ctx.conclude();
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException("Unable to start Aeron", ex);
        }

        final DriverProxy driverProxy = new DriverProxy(ctx.toDriverBuffer);
        final Signal correlationSignal = new Signal();
        final DriverBroadcastReceiver broadcastReceiver = new DriverBroadcastReceiver(ctx.toClientBuffer, ctx.errorHandler);
        final TimerWheel wheel =
            new TimerWheel(CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, CONDUCTOR_TICKS_PER_WHEEL);

        conductor = new ClientConductor(broadcastReceiver,
                                        ctx.bufferManager,
                                        ctx.countersBuffer(),
                                        driverProxy,
                                        correlationSignal,
                                        wheel,
                                        ctx.errorHandler,
                                        ctx.newConnectionHandler,
                                        ctx.inactiveConnectionHandler,
                                        ctx.mediaDriverTimeout(),
                                        ctx.mtuLength());

        this.savedCtx = ctx;
    }

    // TODO: Should this not be start and create a thread?
    /**
     * Run Aeron {@link Agent}s from the calling thread.
     */
    public void run()
    {
        conductor.run();
    }

    /**
     * Run Aeron {@link uk.co.real_logic.aeron.common.Agent}s from a provided executor.
     *
     * @param executor to start from
     */
    public void start(final ExecutorService executor)
    {
        executor.submit(conductor);
    }

    /**
     * Clean up and release all Aeron internal resources
     */
    public void close()
    {
        conductor.close();
        savedCtx.close();
    }

    /**
     * Creates an Aeron client associated with this Aeron instance that can be used to create sources and
     * subscriptions on.
     *
     * @param ctx of the media driver and Aeron configuration
     * @return Aeron instance
     */
    public static Aeron newClient(final Context ctx)
    {
        return new Aeron(ctx);
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers.
     *
     * If the sessionId is 0, then a random one will be generated.
     *
     * @param channel    for receiving the messages known to the media layer.
     * @param streamId   within the channel scope.
     * @param sessionId  to scope the source of the Publication.
     * @return the new Publication.
     */
    public Publication addPublication(final String channel, final int streamId, final int sessionId)
    {
        int sessionIdToRequest = sessionId;

        if (0 == sessionId)
        {
            sessionIdToRequest = BitUtil.generateRandomizedId();
        }

        return conductor.addPublication(channel, sessionIdToRequest, streamId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * @param channel   for receiving the messages known to the media layer.
     * @param streamId  within the channel scope.
     * @param handler   to be called back for each message received.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        return conductor.addSubscription(channel, streamId, handler);
    }

    public ClientConductor conductor()
    {
        return conductor;
    }

    public static class Context extends CommonContext
    {
        private CopyBroadcastReceiver toClientBuffer;
        private RingBuffer toDriverBuffer;

        private MappedByteBuffer defaultToClientBuffer;
        private MappedByteBuffer defaultToDriverBuffer;
        private MappedByteBuffer defaultCounterLabelsBuffer;
        private MappedByteBuffer defaultCounterValuesBuffer;

        private BufferManager bufferManager;

        private Consumer<Exception> errorHandler;
        private NewConnectionHandler newConnectionHandler;
        private InactiveConnectionHandler inactiveConnectionHandler;
        private long mediaDriverTimeout = UNSET_TIMEOUT;

        public Context conclude() throws IOException
        {
            super.conclude();

            if (mediaDriverTimeout == UNSET_TIMEOUT)
            {
                mediaDriverTimeout = DEFAULT_MEDIA_DRIVER_TIMEOUT;
            }

            try
            {
                if (null == toClientBuffer)
                {
                    defaultToClientBuffer = mapExistingFile(toClientsFile(), TO_CLIENTS_FILE);
                    final BroadcastReceiver receiver = new BroadcastReceiver(new AtomicBuffer(defaultToClientBuffer));
                    toClientBuffer = new CopyBroadcastReceiver(receiver);
                }

                if (null == toDriverBuffer)
                {
                    defaultToDriverBuffer = mapExistingFile(toDriverFile(), TO_DRIVER_FILE);
                    toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(defaultToDriverBuffer));
                }

                if (counterLabelsBuffer() == null)
                {
                    defaultCounterLabelsBuffer = mapExistingFile(new File(countersDirName(), LABELS_FILE), LABELS_FILE);
                    counterLabelsBuffer(new AtomicBuffer(defaultCounterLabelsBuffer));
                }

                if (countersBuffer() == null)
                {
                    defaultCounterValuesBuffer = mapExistingFile(new File(countersDirName(), VALUES_FILE), VALUES_FILE);
                    countersBuffer(new AtomicBuffer(defaultCounterValuesBuffer));
                }

                if (null == bufferManager)
                {
                    bufferManager = new MappedBufferManager();
                }

                if (null == errorHandler)
                {
                    errorHandler = Throwable::printStackTrace;
                }
            }
            catch (final IOException ex)
            {
                throw new IllegalStateException("Could not initialise buffers", ex);
            }

            return this;
        }

        public Context toClientBuffer(final CopyBroadcastReceiver toClientBuffer)
        {
            this.toClientBuffer = toClientBuffer;
            return this;
        }

        public Context toDriverBuffer(final RingBuffer toDriverBuffer)
        {
            this.toDriverBuffer = toDriverBuffer;
            return this;
        }

        public Context bufferManager(final BufferManager bufferManager)
        {
            this.bufferManager = bufferManager;
            return this;
        }

        public Context errorHandler(final Consumer<Exception> handler)
        {
            this.errorHandler = handler;
            return this;
        }

        public Context newConnectionHandler(final NewConnectionHandler handler)
        {
            this.newConnectionHandler = handler;
            return this;
        }

        public Context inactiveConnectionHandler(final InactiveConnectionHandler handler)
        {
            this.inactiveConnectionHandler = handler;
            return this;
        }

        public Context mediaDriverTimeout(final long value)
        {
            this.mediaDriverTimeout = value;
            return this;
        }

        public void close()
        {
            if (null != defaultToDriverBuffer)
            {
                IoUtil.unmap(defaultToDriverBuffer);
            }

            if (null != defaultToClientBuffer)
            {
                IoUtil.unmap(defaultToClientBuffer);
            }

            if (null != defaultCounterLabelsBuffer)
            {
                IoUtil.unmap(defaultCounterLabelsBuffer);
            }

            if (null != defaultCounterValuesBuffer)
            {
                IoUtil.unmap(defaultCounterValuesBuffer);
            }

            try
            {
                super.close();
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }

        public long mediaDriverTimeout()
        {
            return mediaDriverTimeout;
        }
    }
}
