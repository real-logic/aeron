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
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
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

import static uk.co.real_logic.aeron.util.CommonConfiguration.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

/**
 * Encapsulation of media driver and client for source and receiver construction
 */
public final class Aeron implements AutoCloseable
{
    private static final int COMMAND_BUFFER_SIZE = 4096 + TRAILER_LENGTH;

    private final ManyToOneRingBuffer clientConductorCommandBuffer =
        new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocateDirect(COMMAND_BUFFER_SIZE)));

    private final AtomicArray<Channel> channels = new AtomicArray<>();
    private final AtomicArray<SubscriberChannel> receivers = new AtomicArray<>();
    private final ClientConductor clientConductor;

    private Future conductorFuture;

    private Aeron(final Context context)
    {
        context.initialiseDefaults();
        final RingBuffer toClientBuffer = context.toClientBuffer;
        final RingBuffer toDriverBuffer = context.toDriverBuffer;
        final BufferUsageStrategy bufferUsage = new MappingBufferUsageStrategy();
        final ConductorErrorHandler errorHandler = new ConductorErrorHandler(context.invalidDestinationHandler);

        clientConductor = new ClientConductor(clientConductorCommandBuffer,
                                              toClientBuffer, toDriverBuffer,
                                              bufferUsage,
                                              channels, receivers,
                                              errorHandler);
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
    }

    /**
     * Creates an media driver associated with this Aeron instance that can be used to create sources and receivers on.
     *
     * @param context of the media driver and Aeron configuration or null for default configuration
     * @return Aeron instance
     */
    public static Aeron newSingleMediaDriver(final Context context)
    {
        return new Aeron(context);
    }

    /**
     * Creates multiple media drivers associated with multiple Aeron instances that can be used to create sources
     * and receivers.
     *
     * @param contexts of the media drivers
     * @return array of Aeron instances
     */
    public static Aeron[] newMultipleMediaDrivers(final Context[] contexts)
    {
        final Aeron[] aerons = new Aeron[contexts.length];

        for (int i = 0, max = contexts.length; i < max; i++)
        {
            aerons[i] = new Aeron(contexts[i]);
        }

        return aerons;
    }

    /**
     * Create a new source that is to send to {@link Destination}.
     * <p>
     * A unique, random, session ID will be generated for the source if the context does not
     * set it. If the context sets the Session ID, then it will be checked for conflicting with existing session Ids.
     *
     * @param context for source options, etc.
     * @return new source
     */
    public Source newSource(final Source.Context context)
    {
        context.clientConductorProxy(new ClientConductorProxy(clientConductorCommandBuffer));

        return new Source(channels, context);
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
     * Create a new receiver that will listen on {@link Destination}
     *
     * @param context context for receiver options.
     * @return new receiver
     */
    public Subscriber newSubscriber(final Subscriber.Context context)
    {
        final ClientConductorProxy clientConductorProxy = new ClientConductorProxy(clientConductorCommandBuffer);

        return new Subscriber(clientConductorProxy, context, receivers);
    }

    /**
     * Create a new receiver that will listen on a given destination, etc.
     *
     * @param block to fill in receiver context
     * @return new Subscriber
     */
    public Subscriber newSubscriber(final Consumer<Subscriber.Context> block)
    {
        final Subscriber.Context context = new Subscriber.Context();
        block.accept(context);

        return newSubscriber(context);
    }

    public ClientConductor conductor()
    {
        return clientConductor;
    }

    public static class Context
    {
        private ErrorHandler errorHandler = new DummyErrorHandler();
        private InvalidDestinationHandler invalidDestinationHandler;

        private RingBuffer toClientBuffer;
        private RingBuffer toDriverBuffer;

        private MappedByteBuffer defaultToClientBuffer;
        private MappedByteBuffer defaultToDriverBuffer;

        public void initialiseDefaults()
        {
            try
            {
                if (toClientBuffer == null)
                {
                    defaultToClientBuffer = IoUtil.mapExistingFile(TO_CLIENTS_PATH, TO_CLIENTS_FILE);
                    toClientBuffer = new ManyToOneRingBuffer(new AtomicBuffer(defaultToClientBuffer));
                }
                if (toDriverBuffer == null)
                {
                    defaultToDriverBuffer = IoUtil.mapExistingFile(TO_DRIVER_PATH, TO_DRIVER_FILE);
                    toDriverBuffer = new ManyToOneRingBuffer(new AtomicBuffer(defaultToDriverBuffer));
                }
            } catch (IOException e)
            {
                throw new IllegalStateException("Could not initialise buffers", e);
            }
        }

        public Context errorHandler(ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        public Context invalidDestinationHandler(final InvalidDestinationHandler invalidDestinationHandler)
        {
            this.invalidDestinationHandler = invalidDestinationHandler;
            return this;
        }

        public Context toClientBuffer(final RingBuffer toClientBuffer)
        {
            this.toClientBuffer = toClientBuffer;
            return this;
        }

        public Context toDriverBuffer(final RingBuffer toDriverBuffer)
        {
            this.toDriverBuffer = toDriverBuffer;
            return this;
        }

        public void close()
        {

        }
    }
}
