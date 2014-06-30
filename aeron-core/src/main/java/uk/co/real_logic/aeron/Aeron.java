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
import java.util.function.Consumer;

import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Encapsulation of media driver and client for source and receiver construction
 */
public final class Aeron implements AutoCloseable
{
    private static final int COMMAND_BUFFER_SIZE = 4096 + TRAILER_LENGTH;
    private static final int DEFAULT_COUNTERS_LABELS_BUFFER_SIZE = 1024;
    private static final int DEFAULT_COUNTERS_VALUES_BUFFER_SIZE = 1024;

    private final ManyToOneRingBuffer clientConductorCommandBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(COMMAND_BUFFER_SIZE)));

    private final AtomicArray<Publication> channels = new AtomicArray<>();
    private final AtomicArray<SubscriberChannel> receivers = new AtomicArray<>();
    private final ClientConductor clientConductor;
    private final ClientContext savedCtx;
    private Future conductorFuture;

    private Aeron(final ClientContext ctx)
    {
        try
        {
            ctx.conclude();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Unable to start Aeron", e);
        }

        final ConductorErrorHandler errorHandler = new ConductorErrorHandler(ctx.invalidDestinationHandler);

        clientConductor = new ClientConductor(clientConductorCommandBuffer,
                                              ctx.toClientBuffer,
                                              ctx.toDriverBuffer,
                                              channels,
                                              receivers,
                                              errorHandler,
                                              ctx.bufferUsageStrategy,
                                              ctx);

        this.savedCtx = ctx;
    }

    /**
     * Run Aeron {@link Agent}s from the calling thread.
     */
    public void run()
    {
        clientConductor.run();
    }

    /**
     * Invoke Aeron {@link uk.co.real_logic.aeron.util.Agent}s from the executor.
     *
     * @param executor to execute from
     */
    public void invoke(final ExecutorService executor)
    {
        conductorFuture = executor.submit(clientConductor);
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
            clientConductor.stop();

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
            clientConductor.stop();
        }
    }

    /**
     * Clean up and release all Aeron internal resources
     */
    public void close()
    {
        clientConductor.close();
        savedCtx.close();
    }

    /**
     * Creates an media driver associated with this Aeron instance that can be used to create sources and receivers on.
     *
     * @param ctx of the media driver and Aeron configuration or null for default configuration
     * @return Aeron instance
     */
    public static Aeron newSingleMediaDriver(final ClientContext ctx)
    {
        return new Aeron(ctx);
    }

    /**
     * Create a new source that is to send to {@link Destination}.
     * <p>
     * A unique, random, session ID will be generated for the source if the ctx does not
     * set it. If the ctx sets the Session ID, then it will be checked for conflicting with existing session Ids.
     *
     * @param ctx for source options, etc.
     * @return new source
     */
    public Source newSource(final Source.Context ctx)
    {
        ctx.clientConductorProxy(new ClientConductorProxy(clientConductorCommandBuffer));

        return new Source(channels, ctx);
    }

    /**
     * Create a new source that is to send to {@link Destination}
     *
     * @param destination address to send all data to
     * @return new source
     */
    public Source newSource(final Destination destination)
    {
        return newSource(new Source.Context().destination(destination));
    }

    /**
     * Create a new subscription that will listen on {@link Destination}
     *
     * @param ctx ctx for subscription options.
     * @return new receiver
     */
    public Subscription newSubscription(final Subscription.Context ctx)
    {
        final ClientConductorProxy clientConductorProxy = new ClientConductorProxy(clientConductorCommandBuffer);

        return new Subscription(clientConductorProxy, ctx, receivers);
    }

    public ClientConductor conductor()
    {
        return clientConductor;
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
                    toClientBuffer = new CopyBroadcastReceiver(new BroadcastReceiver(new AtomicBuffer(defaultToClientBuffer)));
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
