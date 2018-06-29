/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use an {@link Aeron.Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 * <p>
 * <b>Note:</b> If {@link Aeron.Context#errorHandler(ErrorHandler)} is not set and a {@link DriverTimeoutException}
 * occurs then the process will face the wrath of {@link System#exit(int)}. See {@link #DEFAULT_ERROR_HANDLER}.
 */
public class Aeron implements AutoCloseable
{
    /**
     * Used to represent a null value for when some value is not yet set.
     */
    public static final int NULL_VALUE = -1;

    /**
     * Using an integer because there is no support for boolean. 1 is closed and 0 is not closed.
     */
    private static final AtomicIntegerFieldUpdater<Aeron> IS_CLOSED_UPDATER = newUpdater(Aeron.class, "isClosed");

    /**
     * The Default handler for Aeron runtime exceptions.
     * When a {@link io.aeron.exceptions.DriverTimeoutException} is encountered, this handler will
     * exit the program.
     * <p>
     * The error handler can be overridden by supplying an {@link Aeron.Context} with a custom handler.
     *
     * @see Aeron.Context#errorHandler(ErrorHandler)
     */
    public static final ErrorHandler DEFAULT_ERROR_HANDLER =
        (throwable) ->
        {
            throwable.printStackTrace();
            if (throwable instanceof DriverTimeoutException)
            {
                System.err.printf(
                    "%n***%n*** Timeout from the MediaDriver - is it currently running? Exiting.%n***%n");
                System.exit(-1);
            }
        };

    /**
     * Duration in milliseconds for which the client conductor will sleep between duty cycles.
     */
    public static final long IDLE_SLEEP_MS = 16;

    /**
     * Duration in nanoseconds for which the client conductor will sleep between duty cycles.
     */
    public static final long IDLE_SLEEP_NS = TimeUnit.MILLISECONDS.toNanos(IDLE_SLEEP_MS);

    /**
     * Default interval between sending keepalive control messages to the driver.
     */
    public static final long KEEPALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(500);

    @SuppressWarnings("unused") private volatile int isClosed;
    private final long clientId;
    private final ClientConductor conductor;
    private final RingBuffer commandBuffer;
    private final AgentInvoker conductorInvoker;
    private final AgentRunner conductorRunner;
    private final Context ctx;

    Aeron(final Context ctx)
    {
        ctx.conclude();

        this.ctx = ctx;
        clientId = ctx.clientId();
        commandBuffer = ctx.toDriverBuffer();
        conductor = new ClientConductor(ctx);

        if (ctx.useConductorAgentInvoker())
        {
            conductorInvoker = new AgentInvoker(ctx.errorHandler(), null, conductor);
            conductorRunner = null;
        }
        else
        {
            conductorInvoker = null;
            conductorRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), null, conductor);
        }
    }

    /**
     * Create an Aeron instance and connect to the media driver with a default {@link Context}.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect()
    {
        return connect(new Context());
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     * <p>
     * If an exception occurs while trying to establish a connection then the {@link Context#close()} method
     * will be called on the passed context.
     *
     * @param ctx for configuration of the client.
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect(final Context ctx)
    {
        try
        {
            final Aeron aeron = new Aeron(ctx);

            if (ctx.useConductorAgentInvoker())
            {
                aeron.conductorInvoker.start();
            }
            else
            {
                AgentRunner.startOnThread(aeron.conductorRunner, ctx.threadFactory);
            }

            return aeron;
        }
        catch (final Exception ex)
        {
            ctx.close();
            throw ex;
        }
    }

    /**
     * Print out the values from {@link #countersReader()} which can be useful for debugging.
     *
     *  @param out to where the counters get printed.
     */
    public void printCounters(final PrintStream out)
    {
        final CountersReader counters = countersReader();
        counters.forEach((value, id, label) -> out.format("%3d: %,20d - %s%n", id, value, label));
    }

    /**
     * Has the client been closed? If not then the CnC file may not be unmapped.
     *
     * @return true if the client has been explicitly closed otherwise false.
     */
    public boolean isClosed()
    {
        return 1 == isClosed;
    }

    /**
     * Get the {@link Aeron.Context} that is used by this client.
     *
     * @return the {@link Aeron.Context} that is use by this client.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    public long clientId()
    {
        return clientId;
    }

    /**
     * Get the {@link AgentInvoker} for the client conductor.
     *
     * @return the {@link AgentInvoker} for the client conductor.
     */
    public AgentInvoker conductorAgentInvoker()
    {
        return conductorInvoker;
    }

    /**
     * Clean up and release all Aeron client resources and shutdown conductor thread if not using
     * {@link Context#useConductorAgentInvoker(boolean)}.
     * <p>
     * This will close all currently open {@link Publication}s, {@link Subscription}s, and {@link Counter}s created
     * from this client.
     */
    public void close()
    {
        if (IS_CLOSED_UPDATER.compareAndSet(this, 0, 1))
        {
            if (null != conductorRunner)
            {
                conductorRunner.close();
            }
            else
            {
                conductorInvoker.close();
            }

            ctx.close();
        }
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers. The publication returned is threadsafe.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return a new {@link ConcurrentPublication}.
     */
    public ConcurrentPublication addPublication(final String channel, final int streamId)
    {
        return conductor.addPublication(channel, streamId);
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return a new {@link ExclusivePublication}.
     */
    public ExclusivePublication addExclusivePublication(final String channel, final int streamId)
    {
        return conductor.addExclusivePublication(channel, streamId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     * <p>
     * The method will set up the {@link Subscription} to use the
     * {@link Aeron.Context#availableImageHandler(AvailableImageHandler)} and
     * {@link Aeron.Context#unavailableImageHandler(UnavailableImageHandler)} from the {@link Aeron.Context}.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(final String channel, final int streamId)
    {
        return conductor.addSubscription(channel, streamId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     * <p>
     * This method will override the default handlers from the {@link Aeron.Context}, i.e.
     * {@link Aeron.Context#availableImageHandler(AvailableImageHandler)} and
     * {@link Aeron.Context#unavailableImageHandler(UnavailableImageHandler)}. Null values are valid and will
     * result in no action being taken.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption. Null is valid if no
     *                                action is to be taken.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null is valid if no
     *                                action is to be taken.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        return conductor.addSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
    }

    /**
     * Generate the next correlation id that is unique for the connected Media Driver.
     * <p>
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     * <p>
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    public long nextCorrelationId()
    {
        if (1 == isClosed)
        {
            throw new AeronException("Client is closed");
        }

        return commandBuffer.nextCorrelationId();
    }

    /**
     * Get the {@link CountersReader} for the Aeron media driver counters.
     *
     * @return new {@link CountersReader} for the Aeron media driver in use.
     */
    public CountersReader countersReader()
    {
        if (1 == isClosed)
        {
            throw new AeronException("Client is closed");
        }

        return conductor.countersReader();
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     * <p>
     * The counter should be freed by calling {@link Counter#close()}.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyOffset   within the keyBuffer at which the key begins.
     * @param keyLength   of the key in the keyBuffer.
     * @param labelBuffer containing the mandatory label for the counter. The label should not be length prefixed.
     * @param labelOffset within the labelBuffer at which the label begins.
     * @param labelLength of the label in the labelBuffer.
     * @return the newly allocated counter.
     * @see org.agrona.concurrent.status.CountersManager#allocate(int, DirectBuffer, int, int, DirectBuffer, int, int)
     */
    public Counter addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        return conductor.addCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     * <p>
     * The counter should be freed by calling {@link Counter#close()}.
     *
     * @param typeId for the counter.
     * @param label  for the counter. It should be US-ASCII.
     * @return the newly allocated counter.
     * @see org.agrona.concurrent.status.CountersManager#allocate(String, int)
     */
    public Counter addCounter(final int typeId, final String label)
    {
        return conductor.addCounter(typeId, label);
    }

    /**
     * This class provides configuration for the {@link Aeron} class via the {@link Aeron#connect(Aeron.Context)}
     * method and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
     * It can also set up error handling as well as application callbacks for image information from the
     * Media Driver.
     * <p>
     * A number of the properties are for testing and should not be set by end users.
     * <p>
     * <b>Note:</b> Do not reuse instances of the context across different {@link Aeron} clients.
     */
    public static class Context extends CommonContext
    {
        private long clientId;
        private boolean useConductorAgentInvoker = false;
        private AgentInvoker driverAgentInvoker;
        private Lock clientLock;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private IdleStrategy idleStrategy;
        private CopyBroadcastReceiver toClientBuffer;
        private RingBuffer toDriverBuffer;
        private DriverProxy driverProxy;
        private MappedByteBuffer cncByteBuffer;
        private AtomicBuffer cncMetaDataBuffer;
        private LogBuffersFactory logBuffersFactory;
        private ErrorHandler errorHandler;
        private AvailableImageHandler availableImageHandler;
        private UnavailableImageHandler unavailableImageHandler;
        private AvailableCounterHandler availableCounterHandler;
        private UnavailableCounterHandler unavailableCounterHandler;
        private long keepAliveInterval = KEEPALIVE_INTERVAL_NS;
        private long interServiceTimeout = 0;
        private ThreadFactory threadFactory = Thread::new;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            return (Context)super.clone();
        }

        /**
         * This is called automatically by {@link Aeron#connect(Aeron.Context)} and its overloads.
         * There is no need to call it from a client application. It is responsible for providing default
         * values for options that are not individually changed through field setters.
         *
         * @return this Aeron.Context for method chaining.
         */
        public Context conclude()
        {
            super.conclude();

            if (null == clientLock)
            {
                clientLock = new ReentrantLock();
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == nanoClock)
            {
                nanoClock = new SystemNanoClock();
            }

            if (null == idleStrategy)
            {
                idleStrategy = new SleepingMillisIdleStrategy(IDLE_SLEEP_MS);
            }

            if (cncFile() != null)
            {
                connectToDriver();
            }

            if (null == toDriverBuffer)
            {
                toDriverBuffer = new ManyToOneRingBuffer(
                    CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            if (null == toClientBuffer)
            {
                toClientBuffer = new CopyBroadcastReceiver(new BroadcastReceiver(
                    CncFileDescriptor.createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer)));
            }

            if (countersMetaDataBuffer() == null)
            {
                countersMetaDataBuffer(
                    CncFileDescriptor.createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            if (countersValuesBuffer() == null)
            {
                countersValuesBuffer(CncFileDescriptor.createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            interServiceTimeout = CncFileDescriptor.clientLivenessTimeout(cncMetaDataBuffer);

            if (null == logBuffersFactory)
            {
                logBuffersFactory = new MappedLogBuffersFactory();
            }

            if (null == errorHandler)
            {
                errorHandler = DEFAULT_ERROR_HANDLER;
            }

            if (null == driverProxy)
            {
                clientId = toDriverBuffer.nextCorrelationId();
                driverProxy = new DriverProxy(toDriverBuffer, clientId);
            }

            return this;
        }

        /**
         * Get the client identity that has been allocated for communicating with the media driver.
         *
         * @return the client identity that has been allocated for communicating with the media driver.
         */
        public long clientId()
        {
            return clientId;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClientConductor} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @param useConductorAgentInvoker use {@link AgentInvoker} be used for running the {@link ClientConductor}?
         * @return this for a fluent API.
         */
        public Context useConductorAgentInvoker(final boolean useConductorAgentInvoker)
        {
            this.useConductorAgentInvoker = useConductorAgentInvoker;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClientConductor} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @return true if the {@link ClientConductor} will be run with an {@link AgentInvoker} otherwise false.
         */
        public boolean useConductorAgentInvoker()
        {
            return useConductorAgentInvoker;
        }

        /**
         * Set the {@link AgentInvoker} for the Media Driver to be used while awaiting a synchronous response.
         * <p>
         * Useful for when running on a low thread count scenario.
         *
         * @param driverAgentInvoker to be invoked while awaiting a response in the client.
         * @return this for a fluent API.
         */
        public Context driverAgentInvoker(final AgentInvoker driverAgentInvoker)
        {
            this.driverAgentInvoker = driverAgentInvoker;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} that is used to run the Media Driver while awaiting a synchronous response.
         *
         * @return the {@link AgentInvoker} that is used for running the Media Driver.
         */
        public AgentInvoker driverAgentInvoker()
        {
            return driverAgentInvoker;
        }

        /**
         * The {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         * <p>
         * If the {@link #useConductorAgentInvoker()} is set and only one thread accesses the client
         * then the lock can be set to {@link NoOpLock} to elide the lock overhead.
         *
         * @param lock that is used to provide mutual exclusion in the Aeron client.
         * @return this for a fluent API.
         */
        public Context clientLock(final Lock lock)
        {
            clientLock = lock;
            return this;
        }

        /**
         * Get the {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         *
         * @return the {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         */
        public Lock clientLock()
        {
            return clientLock;
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the driver.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the driver.
         * @return this Aeron.Context for method chaining
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link java.time.Clock} used by the client for the epoch time in milliseconds.
         *
         * @return the {@link java.time.Clock} used by the client for the epoch time in milliseconds.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Set the {@link NanoClock} to be used for tracking high resolution time.
         *
         * @param clock {@link NanoClock} to be used for tracking high resolution time.
         * @return this Aeron.Context for method chaining
         */
        public Context nanoClock(final NanoClock clock)
        {
            this.nanoClock = clock;
            return this;
        }

        /**
         * Get the {@link NanoClock} to be used for tracking high resolution time.
         *
         * @return the {@link NanoClock} to be used for tracking high resolution time.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * Provides an IdleStrategy for the thread responsible for communicating with the Aeron Media Driver.
         *
         * @param idleStrategy Thread idle strategy for communication with the Media Driver.
         * @return this Aeron.Context for method chaining.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} employed by the client conductor thread.
         *
         * @return the {@link IdleStrategy} employed by the client conductor thread.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param toClientBuffer Injected CopyBroadcastReceiver
         * @return this Aeron.Context for method chaining.
         */
        Context toClientBuffer(final CopyBroadcastReceiver toClientBuffer)
        {
            this.toClientBuffer = toClientBuffer;
            return this;
        }

        /**
         * The buffer used for communicating from the media driver to the Aeron client.
         *
         * @return the buffer used for communicating from the media driver to the Aeron client.
         */
        public CopyBroadcastReceiver toClientBuffer()
        {
            return toClientBuffer;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param toDriverBuffer Injected RingBuffer.
         * @return this Aeron.Context for method chaining.
         */
        Context toDriverBuffer(final RingBuffer toDriverBuffer)
        {
            this.toDriverBuffer = toDriverBuffer;
            return this;
        }

        /**
         * Get the {@link RingBuffer} used for sending commands to the media driver.
         *
         * @return the {@link RingBuffer} used for sending commands to the media driver.
         */
        public RingBuffer toDriverBuffer()
        {
            return toDriverBuffer;
        }

        /**
         * Set the proxy for communicating with the media driver.
         *
         * @param driverProxy for communicating with the media driver.
         * @return this Aeron.Context for method chaining.
         */
        Context driverProxy(final DriverProxy driverProxy)
        {
            this.driverProxy = driverProxy;
            return this;
        }

        /**
         * Get the proxy for communicating with the media driver.
         *
         * @return the proxy for communicating with the media driver.
         */
        public DriverProxy driverProxy()
        {
            return driverProxy;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param logBuffersFactory Injected LogBuffersFactory
         * @return this Aeron.Context for method chaining.
         */
        Context logBuffersFactory(final LogBuffersFactory logBuffersFactory)
        {
            this.logBuffersFactory = logBuffersFactory;
            return this;
        }

        /**
         * Get the factory for making log buffers.
         *
         * @return the factory for making log buffers.
         */
        public LogBuffersFactory logBuffersFactory()
        {
            return logBuffersFactory;
        }

        /**
         * Handle Aeron exceptions in a callback method. The default behavior is defined by
         * {@link Aeron#DEFAULT_ERROR_HANDLER}. This is the error handler which will be used if an error occurs
         * during the callback for poll operations such as {@link Subscription#poll(FragmentHandler, int)}.
         * <p>
         * The error handler can be reset after {@link Aeron#connect()} and the latest version will always be used
         * so that the boot strapping process can be performed such as replacing the default one with a
         * {@link CountedErrorHandler}.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this Aeron.Context for method chaining.
         * @see io.aeron.exceptions.DriverTimeoutException
         * @see io.aeron.exceptions.RegistrationException
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the error handler that will be called for errors reported back from the media driver.
         *
         * @return the error handler that will be called for errors reported back from the media driver.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Setup a default callback for when an {@link Image} is available.
         *
         * @param handler Callback method for handling available image notifications.
         * @return this Aeron.Context for method chaining.
         */
        public Context availableImageHandler(final AvailableImageHandler handler)
        {
            this.availableImageHandler = handler;
            return this;
        }

        /**
         * Get the default callback handler for notifying when {@link Image}s become available.
         *
         * @return the callback handler for notifying when {@link Image}s become available.
         */
        public AvailableImageHandler availableImageHandler()
        {
            return availableImageHandler;
        }

        /**
         * Setup a default callback for when an {@link Image} is unavailable.
         *
         * @param handler Callback method for handling unavailable image notifications.
         * @return this Aeron.Context for method chaining.
         */
        public Context unavailableImageHandler(final UnavailableImageHandler handler)
        {
            this.unavailableImageHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when an {@link Image} is unavailable.
         *
         * @return the callback handler for when an {@link Image} is unavailable.
         */
        public UnavailableImageHandler unavailableImageHandler()
        {
            return unavailableImageHandler;
        }

        /**
         * Setup a callback for when a counter is available.
         *
         * @param handler to be called for handling available counter notifications.
         * @return this Aeron.Context for fluent API.
         */
        public Context availableCounterHandler(final AvailableCounterHandler handler)
        {
            this.availableCounterHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when a counter is available.
         *
         * @return the callback handler for when a counter is available.
         */
        public AvailableCounterHandler availableCounterHandler()
        {
            return this.availableCounterHandler;
        }

        /**
         * Setup a callback for when a counter is unavailable.
         *
         * @param handler to be called for handling unavailable counter notifications.
         * @return this Aeron.Context for fluent API.
         */
        public Context unavailableCounterHandler(final UnavailableCounterHandler handler)
        {
            this.unavailableCounterHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when a counter is unavailable.
         *
         * @return the callback handler for when a counter is unavailable.
         */
        public UnavailableCounterHandler unavailableCounterHandler()
        {
            return this.unavailableCounterHandler;
        }

        /**
         * Set the interval in nanoseconds for which the client will perform keep-alive operations.
         *
         * @param value the interval in nanoseconds for which the client will perform keep-alive operations.
         * @return this Aeron.Context for method chaining.
         */
        public Context keepAliveInterval(final long value)
        {
            keepAliveInterval = value;
            return this;
        }

        /**
         * Get the interval in nanoseconds for which the client will perform keep-alive operations.
         *
         * @return the interval in nanoseconds for which the client will perform keep-alive operations.
         */
        public long keepAliveInterval()
        {
            return keepAliveInterval;
        }

        /**
         * Set the amount of time, in milliseconds, that this client will wait until it determines the
         * Media Driver is unavailable. When this happens a
         * {@link io.aeron.exceptions.DriverTimeoutException} will be generated for the error handler.
         *
         * @param value Number of milliseconds.
         * @return this Aeron.Context for method chaining.
         * @see #errorHandler(ErrorHandler)
         */
        public Context driverTimeoutMs(final long value)
        {
            super.driverTimeoutMs(value);
            return this;
        }

        /**
         * Set the timeout between service calls the to {@link ClientConductor} duty cycles.
         *
         * @param interServiceTimeout the timeout (ns) between service calls the to {@link ClientConductor} duty cycle.
         * @return this Aeron.Context for method chaining.
         */
        Context interServiceTimeout(final long interServiceTimeout)
        {
            this.interServiceTimeout = interServiceTimeout;
            return this;
        }

        /**
         * Return the timeout between service calls to the duty cycle for the client.
         * <p>
         * When exceeded, {@link #errorHandler} will be called and the active {@link Publication}s and {@link Image}s
         * closed.
         * <p>
         * This value is controlled by the driver and included in the CnC file. It can be configured by adjusting
         * the aeron.client.liveness.timeout property on the media driver.
         *
         * @return the timeout in nanoseconds between service calls.
         */
        public long interServiceTimeout()
        {
            return interServiceTimeout;
        }

        /**
         * @see CommonContext#aeronDirectoryName(String)
         */
        public Context aeronDirectoryName(final String dirName)
        {
            super.aeronDirectoryName(dirName);
            return this;
        }

        /**
         * Specify the thread factory to use when starting the conductor thread.
         *
         * @param threadFactory thread factory to construct the thread.
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * The thread factory to be use to construct the conductor thread
         *
         * @return the specified thread factory or {@link Thread#Thread(Runnable)} if none is provided
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Clean up all resources that the client uses to communicate with the Media Driver.
         */
        public void close()
        {
            IoUtil.unmap(cncByteBuffer);
            cncByteBuffer = null;
            super.close();
        }

        private void connectToDriver()
        {
            final long startTimeMs = epochClock.time();
            final long deadlineMs = startTimeMs + driverTimeoutMs();
            final File cncFile = cncFile();

            while (null == toDriverBuffer)
            {
                while (!cncFile.exists() || cncFile.length() <= 0)
                {
                    if (epochClock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException("CnC file not created: " + cncFile.getAbsolutePath());
                    }

                    sleep(IDLE_SLEEP_MS);
                }

                cncByteBuffer = waitForFileMapping(cncFile, deadlineMs, epochClock);
                cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

                int cncVersion;
                while (0 == (cncVersion = cncMetaDataBuffer.getIntVolatile(CncFileDescriptor.cncVersionOffset(0))))
                {
                    if (epochClock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException("CnC file is created but not initialised.");
                    }

                    sleep(1);
                }

                if (CncFileDescriptor.CNC_VERSION != cncVersion)
                {
                    throw new AeronException("CnC file version not supported: version=" + cncVersion);
                }

                final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
                    CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                while (0 == ringBuffer.consumerHeartbeatTime())
                {
                    if (epochClock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException("No driver heartbeat detected.");
                    }

                    sleep(1);
                }

                final long timeMs = epochClock.time();
                if (ringBuffer.consumerHeartbeatTime() < (timeMs - driverTimeoutMs()))
                {
                    if (timeMs > deadlineMs)
                    {
                        throw new DriverTimeoutException("No driver heartbeat detected.");
                    }

                    IoUtil.unmap(cncByteBuffer);
                    cncByteBuffer = null;
                    cncMetaDataBuffer = null;

                    sleep(100);
                    continue;
                }

                toDriverBuffer = ringBuffer;
            }
        }
    }

    private static MappedByteBuffer waitForFileMapping(
        final File cncFile, final long deadlineMs, final EpochClock epochClock)
    {
        try (FileChannel fileChannel = FileChannel.open(cncFile.toPath(), READ, WRITE))
        {
            while (fileChannel.size() < CncFileDescriptor.CNC_VERSION_FIELD_OFFSET + SIZE_OF_INT)
            {
                if (epochClock.time() > deadlineMs)
                {
                    throw new AeronException("CnC file is created but not populated");
                }

                sleep(IDLE_SLEEP_MS);
            }

            return fileChannel.map(READ_WRITE, 0, fileChannel.size());
        }
        catch (final IOException ex)
        {
            throw new AeronException("cannot open CnC file", ex);
        }
    }

    static void sleep(final long durationMs)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }
}
