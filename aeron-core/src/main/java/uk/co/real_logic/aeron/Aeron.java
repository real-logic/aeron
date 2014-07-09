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
import uk.co.real_logic.aeron.util.CommonContext;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.broadcast.BroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.broadcast.CopyBroadcastReceiver;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Long.getLong;
import static uk.co.real_logic.aeron.util.IoUtil.mapExistingFile;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Encapsulation of media driver and client for source and receiver construction
 */
public final class Aeron implements AutoCloseable
{
    private static final int COMMAND_BUFFER_SIZE = 4096 + TRAILER_LENGTH;

    // TODO: make configurable
    public static final long AWAIT_TIMEOUT = 10_000;

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
        final MediaDriverBroadcastReceiver receiver = new MediaDriverBroadcastReceiver(ctx.toClientBuffer);

        conductor = new ClientConductor(
            receiver,
            errorHandler,
            ctx.bufferUsageStrategy,
            ctx.counterValuesBuffer(),
            mediaDriverProxy,
            correlationSignal,
            AWAIT_TIMEOUT,
            ctx.publicationWindow);

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
     * Add a {@link Publication} for publishing messages to subscribers.
     *
     * @param destination for receiving the messages know to the media layer.
     * @param channelId within the destination scope.
     * @param sessionId to scope the source of the Publication.
     * @return the new Publication.
     */
    public Publication addPublication(final String destination, final long channelId, final long sessionId)
    {
        return conductor.addPublication(destination, channelId, sessionId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.

     * @param destination for receiving the messages know to the media layer.
     * @param channelId within the destination scope.
     * @param handler to be called back for each message received.
     * @return the {@link Subscription} for the destination and channelId pair.
     */
    public Subscription addSubscription(final String destination,
                                        final long channelId,
                                        final Subscription.DataHandler handler)
    {
        return conductor.addSubscription(destination, channelId, handler);
    }

    public ClientConductor conductor()
    {
        return conductor;
    }

    public static class ClientContext extends CommonContext
    {
        public static final long PUBLICATION_WINDOW_DEFAULT = 1024;
        public static final String PUBLICATION_WINDOW_NAME = "aeron.client.publication.window";

        private ErrorHandler errorHandler = new DummyErrorHandler();
        private InvalidDestinationHandler invalidDestinationHandler;

        private CopyBroadcastReceiver toClientBuffer;
        private RingBuffer toDriverBuffer;

        private MappedByteBuffer defaultToClientBuffer;
        private MappedByteBuffer defaultToDriverBuffer;

        private BufferUsageStrategy bufferUsageStrategy;
        private long publicationWindow;

        public ClientContext conclude() throws IOException
        {
            super.conclude();

            publicationWindow(getLong(PUBLICATION_WINDOW_NAME, PUBLICATION_WINDOW_DEFAULT));

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
                    final MappedByteBuffer labels = mapExistingFile(new File(countersDirName(), LABELS_FILE), LABELS_FILE);
                    counterLabelsBuffer(new AtomicBuffer(labels));
                }

                if (counterValuesBuffer() == null)
                {
                    final MappedByteBuffer values = mapExistingFile(new File(countersDirName(), VALUES_FILE), VALUES_FILE);
                    counterValuesBuffer(new AtomicBuffer(values));
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

        public ClientContext publicationWindow(final long publicationWindow)
        {
            this.publicationWindow = publicationWindow;
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
