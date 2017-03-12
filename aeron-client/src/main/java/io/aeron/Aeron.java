/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import io.aeron.exceptions.DriverTimeoutException;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static org.agrona.IoUtil.mapExistingFile;

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use an {@link Aeron.Context} to configure the Aeron object.
 *
 * A client application requires only one Aeron object per Media Driver.
 */
public final class Aeron implements AutoCloseable
{
    /**
     * The Default handler for Aeron runtime exceptions.
     * When a {@link io.aeron.exceptions.DriverTimeoutException} is encountered, this handler will
     * exit the program.
     *
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
     * Duration in nanoseconds for which the client conductor will sleep between duty cycles.
     */
    public static final long IDLE_SLEEP_NS = TimeUnit.MILLISECONDS.toNanos(16);

    /**
     * Default interval between sending keepalive control messages to the driver.
     */
    public static final long KEEPALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(500);

    /**
     * Default interval that if exceeded between duty cycles the conductor will consider itself a zombie and suicide.
     */
    public static final long INTER_SERVICE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Timeout after which if no status messages have been received then a publication is considered not connected.
     */
    public static final long PUBLICATION_CONNECTION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private final ClientConductor conductor;
    private final AgentRunner conductorRunner;
    private final RingBuffer commandBuffer;
    private final CountersReader countersReader;

    Aeron(final Context ctx)
    {
        ctx.conclude();

        commandBuffer = ctx.toDriverBuffer;
        countersReader = new CountersReader(ctx.countersMetaDataBuffer(), ctx.countersValuesBuffer());
        conductor = new ClientConductor(ctx);
        conductorRunner = new AgentRunner(ctx.idleStrategy, ctx.errorHandler, null, conductor);
    }

    /**
     * Create an Aeron instance and connect to the media driver with a default {@link Context}.
     *
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
     *
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
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
            return new Aeron(ctx).start(ctx.threadFactory);
        }
        catch (final Exception ex)
        {
            ctx.close();
            throw ex;
        }
    }

    /**
     * Clean up and release all Aeron internal resources and shutdown threads.
     */
    public void close()
    {
        conductor.clientLock().lock();
        try
        {
            conductorRunner.close();
        }
        finally
        {
            conductor.clientLock().unlock();
        }
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the new Publication.
     */
    public Publication addPublication(final String channel, final int streamId)
    {
        conductor.clientLock().lock();
        try
        {
            return conductor.addPublication(channel, streamId);
        }
        finally
        {
            conductor.clientLock().unlock();
        }
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(final String channel, final int streamId)
    {
        conductor.clientLock().lock();
        try
        {
            return conductor.addSubscription(channel, streamId);
        }
        finally
        {
            conductor.clientLock().unlock();
        }
    }

    /**
     * Generate the next correlation id that is unique for the Media Driver.
     *
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     *
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    public long nextCorrelationId()
    {
        return commandBuffer.nextCorrelationId();
    }

    /**
     * Create and return a {@link CountersReader} for the Aeron media driver counters.
     *
     * @return new {@link CountersReader} for the Aeron media driver in use.
     */
    public CountersReader countersReader()
    {
        return countersReader;
    }

    private Aeron start(final ThreadFactory threadFactory)
    {
        AgentRunner.startOnThread(conductorRunner, threadFactory);

        return this;
    }

    /**
     * This class provides configuration for the {@link Aeron} class via the {@link Aeron#connect(Aeron.Context)}
     * method and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
     * It can also set up error handling as well as application callbacks for image information from the
     * Media Driver.
     *
     * A number of the properties are for testing and should not be set by end users.
     */
    public static class Context extends CommonContext
    {
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
        private long keepAliveInterval = KEEPALIVE_INTERVAL_NS;
        private long interServiceTimeout = 0;
        private long publicationConnectionTimeout = PUBLICATION_CONNECTION_TIMEOUT_MS;
        private FileChannel.MapMode imageMapMode;
        private ThreadFactory threadFactory = Thread::new;

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
                idleStrategy = new SleepingIdleStrategy(IDLE_SLEEP_NS);
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

            if (0 == interServiceTimeout)
            {
                interServiceTimeout = CncFileDescriptor.clientLivenessTimeout(cncMetaDataBuffer);
            }
            else
            {
                interServiceTimeout = INTER_SERVICE_TIMEOUT_NS;
            }

            if (null == logBuffersFactory)
            {
                logBuffersFactory = new MappedLogBuffersFactory();
            }

            if (null == errorHandler)
            {
                errorHandler = DEFAULT_ERROR_HANDLER;
            }

            if (null == availableImageHandler)
            {
                availableImageHandler = (image) -> {};
            }

            if (null == unavailableImageHandler)
            {
                unavailableImageHandler = (image) -> {};
            }

            if (null == imageMapMode)
            {
                imageMapMode = READ_ONLY;
            }

            if (null == driverProxy)
            {
                driverProxy = new DriverProxy(toDriverBuffer);
            }

            return this;
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
        public Context toClientBuffer(final CopyBroadcastReceiver toClientBuffer)
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
        public Context toDriverBuffer(final RingBuffer toDriverBuffer)
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
        public Context driverProxy(final DriverProxy driverProxy)
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
            return  driverProxy;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param logBuffersFactory Injected LogBuffersFactory
         * @return this Aeron.Context for method chaining.
         */
        public Context logBuffersFactory(final LogBuffersFactory logBuffersFactory)
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
         * {@link Aeron#DEFAULT_ERROR_HANDLER}.
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
         * Set up a callback for when an {@link Image} is available.
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
         * Get the callback handler for notifying when {@link Image}s become available.
         *
         * @return the callback handler for notifying when {@link Image}s become available.
         */
        public AvailableImageHandler availableImageHandler()
        {
            return availableImageHandler;
        }

        /**
         * Set up a callback for when an {@link Image} is unavailable.
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
         * @param interServiceTimeout the timeout (ms) between service calls the to {@link ClientConductor} duty cycles.
         * @return this Aeron.Context for method chaining.
         */
        public Context interServiceTimeout(final long interServiceTimeout)
        {
            this.interServiceTimeout = interServiceTimeout;
            return this;
        }

        /**
         * Return the timeout between service calls to the duty cycle for the client.
         *
         * When exceeded, {@link #errorHandler} will be called and the active {@link Publication}s and {@link Image}s
         * closed.
         *
         * This value is controlled by the driver and included in the CnC file.
         *
         * @return the timeout between service calls in nanoseconds.
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
         * Set the amount of time, in milliseconds, that this client will use to determine if a {@link Publication}
         * has active subscribers or not.
         *
         * @param value number of milliseconds.
         * @return this Aeron.Context for method chaining.
         */
        public Context publicationConnectionTimeout(final long value)
        {
            publicationConnectionTimeout = value;
            return this;
        }

        /**
         * Return the timeout, in milliseconds, that this client will use to determine if a {@link Publication}
         * has active subscribers or not.
         *
         * @return timeout in milliseconds.
         */
        public long publicationConnectionTimeout()
        {
            return publicationConnectionTimeout;
        }

        /**
         * The file memory mapping mode for {@link Image}s.
         *
         * @param imageMapMode file memory mapping mode for {@link Image}s.
         * @return this for a fluent API.
         */
        public Context imageMapMode(final FileChannel.MapMode imageMapMode)
        {
            this.imageMapMode = imageMapMode;
            return this;
        }

        /**
         * The file memory mapping mode for {@link Image}s.
         *
         * @return the file memory mapping mode for {@link Image}s.
         */
        public FileChannel.MapMode imageMapMode()
        {
            return imageMapMode;
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
            final File cncFile = cncFile();

            while (true)
            {
                while (!cncFile.exists())
                {
                    if (epochClock.time() > (startTimeMs + driverTimeoutMs()))
                    {
                        throw new DriverTimeoutException("CnC file not found: " + cncFile.getName());
                    }

                    sleep(16);
                }

                cncByteBuffer = mapExistingFile(cncFile(), CncFileDescriptor.CNC_FILE);
                cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

                int cncVersion;
                while (0 == (cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0))))
                {
                    if (epochClock.time() > (startTimeMs + driverTimeoutMs()))
                    {
                        throw new DriverTimeoutException("CnC file is created but not initialised.");
                    }

                    sleep(1);
                }

                if (CncFileDescriptor.CNC_VERSION != cncVersion)
                {
                    throw new IllegalStateException("CnC file version not supported: version=" + cncVersion);
                }

                final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
                    CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                while (0 == ringBuffer.consumerHeartbeatTime())
                {
                    if (epochClock.time() > (startTimeMs + driverTimeoutMs()))
                    {
                        throw new DriverTimeoutException("No driver heartbeat detected.");
                    }

                    sleep(1);
                }

                if (ringBuffer.consumerHeartbeatTime() < (epochClock.time() - driverTimeoutMs()))
                {
                    if (epochClock.time() > (startTimeMs + driverTimeoutMs()))
                    {
                        throw new DriverTimeoutException("No driver heartbeat detected.");
                    }

                    IoUtil.unmap(cncByteBuffer);
                    sleep(100);
                    continue;
                }

                if (null == toDriverBuffer)
                {
                    toDriverBuffer = ringBuffer;
                }

                break;
            }
        }
    }

    static void sleep(final long timeInMs)
    {
        try
        {
            Thread.sleep(timeInMs);
        }
        catch (final InterruptedException ignore)
        {
        }
    }
}
