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
import uk.co.real_logic.aeron.util.AtomicArray;
import uk.co.real_logic.aeron.util.CommonConfiguration;
import uk.co.real_logic.aeron.util.ConductorBufferStrategy;
import uk.co.real_logic.aeron.util.MappingConductorBufferStrategy;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

/**
 * Encapsulation of media driver and API for source and receiver construction
 */
public final class Aeron
{
    private static final int ADMIN_BUFFER_SIZE = 512 + TRAILER_LENGTH;

    private final ProducerControlFactory producerControl;
    private final ManyToOneRingBuffer adminCommandBuffer;
    private final ErrorHandler errorHandler;
    private final ClientConductor adminThread;
    private final ConductorBufferStrategy adminBuffers;
    private final AtomicArray<Channel> channels;
    private final AtomicArray<ConsumerChannel> receivers;

    private Aeron(final Context context)
    {
        errorHandler = context.errorHandler;
        adminBuffers = context.adminBuffers;
        producerControl = context.producerControl;
        channels = new AtomicArray<>();
        receivers = new AtomicArray<>();
        adminCommandBuffer = new ManyToOneRingBuffer(new AtomicBuffer(ByteBuffer.allocate(ADMIN_BUFFER_SIZE)));

        try
        {
            final RingBuffer recvBuffer = new ManyToOneRingBuffer(new AtomicBuffer(adminBuffers.toApi()));
            final RingBuffer sendBuffer = new ManyToOneRingBuffer(new AtomicBuffer(adminBuffers.toMediaDriver()));
            final BufferUsageStrategy bufferUsage = new MappingBufferUsageStrategy(CommonConfiguration.DATA_DIR);
            final ConductorErrorHandler conductorErrorHandler = new ConductorErrorHandler(context.invalidDestinationHandler);

            adminThread = new ClientConductor(adminCommandBuffer,
                                                recvBuffer, sendBuffer,
                                                bufferUsage,
                                                channels, receivers,
                    conductorErrorHandler,
                                                producerControl);
        }
        catch (final Exception ex)
        {
            throw new IllegalArgumentException("Unable to create Aeron", ex);
        }
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
     * Create a new source that is to send to {@link uk.co.real_logic.aeron.Destination}.
     *
     * A unique, random, session ID will be generated for the source if the context does not
     * set it. If the context sets the Session ID, then it will be checked for conflicting with existing session Ids.
     *
     * @param context for source options, etc.
     * @return new source
     */
    public Source newSource(final Source.Context context)
    {
        context.adminThread(new ClientConductorCursor(adminCommandBuffer));
        return new Source(channels, context);
    }

    /**
     * Create a new source that is to send to {@link Destination}
     * @param destination address to send all data to
     * @return new source
     */
    public Source newSource(final Destination destination)
    {
        return newSource(new Source.Context().destination(destination));
    }

    /**
     * Create an array of sources.
     *
     * Convenience function to make it easier to create a number of Sources easier.
     *
     * @param contexts for the source options, etc.
     * @return array of new sources.
     */
    public Source[] newSources(final Source.Context[] contexts)
    {
        final Source[] sources = new Source[contexts.length];

        for (int i = 0, max = contexts.length; i < max; i++)
        {
            sources[i] = newSource(contexts[i]);
        }

        return sources;
    }

    /**
     * Create a new receiver that will listen on {@link uk.co.real_logic.aeron.Destination}
     * @param context context for receiver options.
     * @return new receiver
     */
    public Consumer newConsumer(final Consumer.Context context)
    {
        final ClientConductorCursor adminThread = new ClientConductorCursor(adminCommandBuffer);

        return new Consumer(adminThread, context, receivers);
    }

    /**
     * Create a new receiver that will listen on a given destination, etc.
     *
     * @param block to fill in receiver context
     * @return new receiver
     */
    public Consumer newConsumer(final java.util.function.Consumer<Consumer.Context> block)
    {
        Consumer.Context context = new Consumer.Context();
        block.accept(new Consumer.Context());

        return newConsumer(context);
    }

    public ClientConductor conductor()
    {
        return adminThread;
    }

    public static class Context
    {
        private ErrorHandler errorHandler;
        private ConductorBufferStrategy adminBuffers;
        private InvalidDestinationHandler invalidDestinationHandler;
        private ProducerControlFactory producerControl;

        public Context()
        {
            errorHandler = new DummyErrorHandler();
            adminBuffers = new MappingConductorBufferStrategy(CommonConfiguration.ADMIN_DIR);
            producerControl = DefaultProducerControlStrategy::new;
        }

        public Context errorHandler(ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        public Context adminBufferStrategy(ConductorBufferStrategy adminBuffers)
        {
            this.adminBuffers = adminBuffers;
            return this;
        }

        public Context invalidDestinationHandler(final InvalidDestinationHandler invalidDestination)
        {
            this.invalidDestinationHandler = invalidDestination;
            return this;
        }

        public Context producerControl(final ProducerControlFactory producerControl)
        {
            this.producerControl = producerControl;
            return this;
        }
    }
}
