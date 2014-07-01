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

import uk.co.real_logic.aeron.conductor.*;
import uk.co.real_logic.aeron.util.Agent;
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.CommonContext;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Encapsulation of media driver and client for source and receiver construction
 */
public final class Aeron implements AutoCloseable
{
    private static final int COMMAND_BUFFER_SIZE = 4096 + TRAILER_LENGTH;
    private static final int DEFAULT_COUNTERS_LABELS_BUFFER_SIZE = 1024;
    private static final int DEFAULT_COUNTERS_VALUES_BUFFER_SIZE = 1024;

    // TODO: make configurable
    public static final long AWAIT_TIMEOUT = 1000_000;

    private final ManyToOneRingBuffer clientConductorCommandBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(COMMAND_BUFFER_SIZE)));

    private final AtomicArray<Subscription> subscriptions = new AtomicArray<>();
    private final ClientConductor conductor;
    private final ClientContext savedCtx;

    private Future conductorFuture;

    private Aeron(final ClientContext ctx)
    {
        try
        {
            ctx.conclude();
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException("Unable to start Aeron", ex);
        }

        final ConductorErrorHandler errorHandler = new ConductorErrorHandler(ctx.invalidDestinationHandler);
        final MediaDriverProxy mediaDriverProxy = new MediaDriverProxy(ctx.toDriverBuffer);
        final Signal correlationSignal = new Signal();

        conductor = new ClientConductor(
            clientConductorCommandBuffer,
            ctx.toClientBuffer,
            ctx.toDriverBuffer,
            subscriptions,
            errorHandler,
            ctx.bufferUsageStrategy,
            ctx.counterValuesBuffer(),
            mediaDriverProxy,
            correlationSignal,
            AWAIT_TIMEOUT);

        this.savedCtx = ctx;
    }

    /**
     * Run Aeron {@link Agent}s from the calling thread.
     */
    public void run()
    {
        conductor.run();
    }

    /**
     * Invoke Aeron {@link uk.co.real_logic.aeron.util.Agent}s from the executor.
     *
     * @param executor to execute from
     */
    public void invoke(final ExecutorService executor)
    {
        conductorFuture = executor.submit(conductor);
    }

    /**
     * Stop running Aeron {@link Agent}s. Waiting for termination if started from an executor.
     *
     * @throws Exception
     */
    public void shutdown() throws Exception
    {
        if (null != conductorFuture)
        {
            conductor.stop();

            try
            {
                conductorFuture.get(100, TimeUnit.MILLISECONDS);
            }
            catch (final TimeoutException ex)
            {
                conductorFuture.cancel(true);
            }
        }
        else
        {
            conductor.stop();
        }
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
     * Creates an media driver associated with this Aeron instance that can be used to create sources and
     * subscriptions on.
     *
     * @param ctx of the media driver and Aeron configuration or null for default configuration
     * @return Aeron instance
     */
    public static Aeron newSingleMediaDriver(final ClientContext ctx)
    {
        return new Aeron(ctx);
    }

    /**
     * Create a new {@link Publication} for sending messages via.
     *
     * @param destination address to send all data to
     * @param channelId for the publication
     * @param sessionId to scope the publication
     * @return the new Publication.
     */
    public Publication addPublication(final String destination, final long channelId, final long sessionId)
    {
        return conductor.addPublication(destination, channelId, sessionId);
    }

    /**
     * Create a new {@link Subscription} for a destination and channel pairing.
     *
     * @return the new Subscription.
     */
    public Subscription newSubscription(final String destination,
                                        final long channelId,
                                        final Subscription.DataHandler handler)
    {
        final MediaDriverProxy mediaDriverProxy = new MediaDriverProxy(clientConductorCommandBuffer);

        return new Subscription(mediaDriverProxy, handler, destination, channelId, subscriptions);
    }

    public ClientConductor conductor()
    {
        return conductor;
    }

    public static class ClientContext extends CommonContext
    {
        private ErrorHandler errorHandler = new DummyErrorHandler();
        private InvalidDestinationHandler invalidDestinationHandler;

        private CopyBroadcastReceiver toClientBuffer;
        private RingBuffer toDriverBuffer;

        private MappedByteBuffer defaultToClientBuffer;
        private MappedByteBuffer defaultToDriverBuffer;

        private BufferUsageStrategy bufferUsageStrategy;

        public ClientContext conclude() throws IOException
        {
            super.conclude();

            try
            {
                if (null == toClientBuffer)
                {
                    defaultToClientBuffer = IoUtil.mapExistingFile(toClientsFile(), TO_CLIENTS_FILE);
                    final BroadcastReceiver receiver = new BroadcastReceiver(new AtomicBuffer(defaultToClientBuffer));
                    toClientBuffer = new CopyBroadcastReceiver(receiver);
                }

                if (null == toDriverBuffer)
                {
                    defaultToDriverBuffer = IoUtil.mapExistingFile(toDriverFile(), TO_DRIVER_FILE);
                    toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(defaultToDriverBuffer));
                }

                if (counterLabelsBuffer() == null)
                {
                    counterLabelsBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(DEFAULT_COUNTERS_LABELS_BUFFER_SIZE)));
                }

                if (counterValuesBuffer() == null)
                {
                    counterValuesBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(DEFAULT_COUNTERS_VALUES_BUFFER_SIZE)));
                }

                if (null == bufferUsageStrategy)
                {
                    bufferUsageStrategy = new MappingBufferUsageStrategy();
                }
            }
            catch (IOException e)
            {
                throw new IllegalStateException("Could not initialise buffers", e);
            }

            return this;
        }

        public ClientContext errorHandler(ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        public ClientContext invalidDestinationHandler(final InvalidDestinationHandler invalidDestinationHandler)
        {
            this.invalidDestinationHandler = invalidDestinationHandler;
            return this;
        }

        public ClientContext toClientBuffer(final CopyBroadcastReceiver toClientBuffer)
        {
            this.toClientBuffer = toClientBuffer;
            return this;
        }

        public ClientContext toDriverBuffer(final RingBuffer toDriverBuffer)
        {
            this.toDriverBuffer = toDriverBuffer;
            return this;
        }

        public ClientContext bufferUsageStrategy(final BufferUsageStrategy strategy)
        {
            this.bufferUsageStrategy = strategy;
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

            try
            {
                super.close();
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }
}
