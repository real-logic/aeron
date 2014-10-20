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
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.common.IoUtil.mapExistingFile;

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 */
public final class Aeron implements AutoCloseable
{
    public static final Consumer<Exception> DEFAULT_ERROR_HANDLER =
        (throwable) ->
        {
            throwable.printStackTrace();
            if (throwable instanceof DriverTimeoutException)
            {
                System.err.printf("\n***\n*** Timeout from the Media Driver - is it currently running? Exiting.\n***\n");
                System.exit(-1);
            }
        };

    private static final long IDLE_MAX_SPINS = 0;
    private static final long IDLE_MAX_YIELDS = 0;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);

    private static final int CONDUCTOR_TICKS_PER_WHEEL = 1024;
    private static final int CONDUCTOR_TICK_DURATION_US = 10_000;

    private static final long NULL_TIMEOUT = -1;
    private static final long DEFAULT_MEDIA_DRIVER_TIMEOUT = 10_000;

    private final ClientConductor conductor;
    private final Context ctx;

    Aeron(final Context ctx)
    {
        ctx.conclude();

        final TimerWheel wheel = new TimerWheel(CONDUCTOR_TICK_DURATION_US, TimeUnit.MICROSECONDS, CONDUCTOR_TICKS_PER_WHEEL);

        conductor = new ClientConductor(
            ctx.idleStrategy,
            ctx.toClientBuffer,
            ctx.bufferManager,
            ctx.countersBuffer(),
            new DriverProxy(ctx.toDriverBuffer),
            new Signal(),
            wheel,
            ctx.errorHandler,
            ctx.newConnectionHandler,
            ctx.inactiveConnectionHandler,
            ctx.mediaDriverTimeout());

        this.ctx = ctx;
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param ctx for configuration of the client.
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect(final Context ctx)
    {
        return new Aeron(ctx).start();
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads for interacting with the media driver are run via the the provided {@link Executor}.
     *
     * @param ctx      for configuration of the client.
     * @param executor to run the internal threads for communicating with the media driver.
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect(final Context ctx, final Executor executor)
    {
        return new Aeron(ctx).start(executor);
    }

    /**
     * Clean up and release all Aeron internal resources and shutdown threads.
     */
    public void close()
    {
        conductor.close();
        ctx.close();
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers.
     * <p>
     * A session id will be generated for this publication.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the new Publication.
     */
    public Publication addPublication(final String channel, final int streamId)
    {
        return addPublication(channel, streamId, 0);
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers.
     * <p>
     * If the sessionId is 0, then a random one will be generated.
     *
     * @param channel   for receiving the messages known to the media layer.
     * @param streamId  within the channel scope.
     * @param sessionId to scope the source of the Publication.
     * @return the new Publication.
     */
    public Publication addPublication(final String channel, final int streamId, final int sessionId)
    {
        int sessionIdToRequest = sessionId;

        if (0 == sessionId)
        {
            sessionIdToRequest = BitUtil.generateRandomisedId();
        }

        return conductor.addPublication(channel, streamId, sessionIdToRequest);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @param handler  to be called back for each message received.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(final String channel, final int streamId, final DataHandler handler)
    {
        return conductor.addSubscription(channel, streamId, handler);
    }

    /**
     * Used for testing.
     */
    ClientConductor conductor()
    {
        return conductor;
    }

    private Aeron start()
    {
        final Thread thread = new Thread(conductor);
        thread.setName("aeron-client-conductor");
        thread.start();

        return this;
    }

    private Aeron start(final Executor executor)
    {
        executor.execute(conductor);

        return this;
    }

    public static class Context extends CommonContext
    {
        private IdleStrategy idleStrategy;
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
        private long mediaDriverTimeout = NULL_TIMEOUT;

        public Context conclude()
        {
            super.conclude();

            try
            {
                if (mediaDriverTimeout == NULL_TIMEOUT)
                {
                    mediaDriverTimeout = DEFAULT_MEDIA_DRIVER_TIMEOUT;
                }

                if (null == idleStrategy)
                {
                    idleStrategy = new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
                }

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
                    errorHandler = DEFAULT_ERROR_HANDLER;
                }
            }
            catch (final Exception ex)
            {
                System.err.printf("\n***\n*** Failed to connect to the Media Driver - is it currently running?\n***\n");

                throw new IllegalStateException("Could not initialise communication buffers", ex);
            }

            return this;
        }

        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
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

        public long mediaDriverTimeout()
        {
            return mediaDriverTimeout;
        }

        public void close()
        {
            IoUtil.unmap(defaultToDriverBuffer);
            IoUtil.unmap(defaultToClientBuffer);
            IoUtil.unmap(defaultCounterLabelsBuffer);
            IoUtil.unmap(defaultCounterValuesBuffer);

            super.close();
        }
    }
}
