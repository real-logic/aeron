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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;
import uk.co.real_logic.aeron.util.CommonConfiguration;
import uk.co.real_logic.aeron.util.ConductorMappedBuffers;
import uk.co.real_logic.aeron.util.MediaDriverConductorMappedBuffers;
import uk.co.real_logic.aeron.util.TimerWheel;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static java.lang.Integer.getInteger;
import static uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement.newMappedBufferManager;
import static uk.co.real_logic.aeron.util.CommonConfiguration.ADMIN_DIR_NAME;
import static uk.co.real_logic.aeron.util.CommonConfiguration.DATA_DIR_NAME;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.BufferDescriptor.TRAILER_LENGTH;

/**
 * Main class for JVM-based mediadriver
 *
 *
 * Usage:
 * <code>
 *     $ java -jar aeron-mediadriver.jar
 *     $ java -Doption=value -jar aeron-mediadriver.jar
 * </code>
 * Properties
 * <ul>
 *     <li><code>aeron.conductor.dir</code>: Use value as directory name for conductor buffers.</li>
 *     <li><code>aeron.data.dir</code>: Use value as directory name for data buffers.</li>
 *     <li><code>aeron.rcv.buffer.size</code>: Use int value as size of buffer for receiving from network.</li>
 *     <li><code>aeron.command.buffer.size</code>: Use int value as size of the command buffers between threads.</li>
 *     <li><code>aeron.conductor.buffer.size</code>: Use int value as size of the conductor buffers between the media driver
       and the client.</li>
 *     <li><code>aeron.select.timeout</code>: use int value as default timeout for NIO select calls</li>
 * </ul>
 */
public class MediaDriver implements AutoCloseable
{
    /** Byte buffer size (in bytes) for reads */
    public static final String READ_BUFFER_SZ_PROP_NAME = "aeron.rcv.buffer.size";

    /** Size (in bytes) of the command buffers between threads */
    public static final String COMMAND_BUFFER_SZ_PROP_NAME = "aeron.command.buffer.size";

    /** Size (in bytes) of the conductor buffers between the media driver and the client */
    public static final String ADMIN_BUFFER_SZ_PROP_NAME = "aeron.conductor.buffer.size";

    /** Size (in bytes) of the counter storage buffer */
    public static final String COUNTERS_BUFFER_SZ_PROP_NAME = "aeron.counters.buffer.size";

    /** Size (in bytes) of the counter storage buffer */
    public static final String DESCRIPTOR_BUFFER_SZ_PROP_NAME = "aeron.counters.descriptor.size";

    /** Timeout (in msec) for the basic NIO select call */
    public static final String SELECT_TIMEOUT_PROP_NAME = "aeron.select.timeout";

    /** Default byte buffer size for reads */
    public static final int READ_BYTE_BUFFER_SZ_DEFAULT = 4096;

    /** Default buffer size for command buffers between threads */
    public static final int COMMAND_BUFFER_SZ_DEFAULT = 65536;

    /** Default buffer size for conductor buffers between the media driver and the client */
    public static final int ADMIN_BUFFER_SZ_DEFAULT = 65536 + TRAILER_LENGTH;

    /** Size (in bytes) of the counter storage buffer */
    public static final int COUNTERS_BUFFER_SZ_DEFAULT = 65536;

    /** Size (in bytes) of the counter storage buffer */
    public static final int DESCRIPTOR_BUFFER_SZ_DEFAULT = 65536;

    /** Default timeout for select */
    public static final int SELECT_TIMEOUT_DEFAULT = 20;

    public static final int READ_BYTE_BUFFER_SZ = getInteger(READ_BUFFER_SZ_PROP_NAME, READ_BYTE_BUFFER_SZ_DEFAULT);
    public static final int COMMAND_BUFFER_SZ = getInteger(COMMAND_BUFFER_SZ_PROP_NAME, COMMAND_BUFFER_SZ_DEFAULT);
    public static final int ADMIN_BUFFER_SZ = getInteger(ADMIN_BUFFER_SZ_PROP_NAME, ADMIN_BUFFER_SZ_DEFAULT);
    public static final int SELECT_TIMEOUT = getInteger(SELECT_TIMEOUT_PROP_NAME, SELECT_TIMEOUT_DEFAULT);
    public static final int COUNTERS_BUFFER_SZ = getInteger(COUNTERS_BUFFER_SZ_PROP_NAME, COUNTERS_BUFFER_SZ_DEFAULT);
    public static final int DESCRIPTOR_BUFFER_SZ = getInteger(DESCRIPTOR_BUFFER_SZ_PROP_NAME,
                                                              DESCRIPTOR_BUFFER_SZ_DEFAULT);

    /** ticksPerWheel for TimerWheel in conductor thread */
    public static final int MEDIA_CONDUCTOR_TICKS_PER_WHEEL = 1024;

    /** tickDuration (in MICROSECONDS) for TimerWheel in conductor thread */
    public static final int MEDIA_CONDUCTOR_TICK_DURATION_MICROS = 10 * 1000;

    public static void main(final String[] args)
    {
        try (final MediaDriver mediaDriver = new MediaDriver())
        {
            // 1 for Receive Thread (Sockets to Buffers)
            // 1 for Send Thread (Buffers to Sockets)
            // 1 for Admin Thread (Buffer Management, NAK, Retransmit, etc.)
            final Executor executor = Executors.newFixedThreadPool(3);

            executor.execute(mediaDriver.receiverThread());
            executor.execute(mediaDriver.senderThread());
            executor.execute(mediaDriver.adminThread());
        }
        catch (final InterruptedException ie)
        {
            // catch this OK. We should finally close on it also... oh look, try-with-resources just did.
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private final Receiver receiver;
    private final Sender sender;
    private final MediaConductor mediaConductor;

    private final ConductorMappedBuffers conductorMappedBuffers;
    private final BufferManagement bufferManagement;

    public MediaDriver() throws Exception
    {
        final NioSelector nioSelector = new NioSelector();

        conductorMappedBuffers = new MediaDriverConductorMappedBuffers(ADMIN_DIR_NAME, ADMIN_BUFFER_SZ);
        bufferManagement = newMappedBufferManager(DATA_DIR_NAME);

        final Context context = new Context()
                .conductorCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(nioSelector)
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderControlStrategy::new)
                .conductorCommsBuffers(conductorMappedBuffers)
                .bufferManagement(bufferManagement)
                .mtuLength(CommonConfiguration.MTU_LENGTH);

        context.rcvFrameHandlerFactory(new RcvFrameHandlerFactory(nioSelector,
                new MediaConductorCursor(context.conductorCommandBuffer(), nioSelector)));

        receiver = new Receiver(context);
        sender = new Sender(context);
        mediaConductor = new MediaConductor(context, receiver, sender);
    }

    public Receiver receiverThread()
    {
        return receiver;
    }

    public Sender senderThread()
    {
        return sender;
    }

    public MediaConductor adminThread()
    {
        return mediaConductor;
    }

    public void close() throws Exception
    {
        receiver.close();
        sender.close();
        mediaConductor.close();
        conductorMappedBuffers.close();
        bufferManagement.close();
    }

    public static class Context
    {
        private RingBuffer conductorCommandBuffer;
        private RingBuffer receiverCommandBuffer;
        private BufferManagement bufferManagement;
        private ConductorMappedBuffers conductorMappedBuffers;
        private NioSelector rcvNioSelector;
        private NioSelector adminNioSelector;
        private Supplier<SenderControlStrategy> senderFlowControl;
        private TimerWheel adminTimerWheel;
        private int mtuLength;
        private RcvFrameHandlerFactory rcvFrameHandlerFactory;

        private RingBuffer createNewCommandBuffer(final int sz)
        {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(sz + TRAILER_LENGTH);
            final AtomicBuffer atomicBuffer = new AtomicBuffer(byteBuffer);

            return new ManyToOneRingBuffer(atomicBuffer);
        }

        public Context conductorCommandBuffer(final int sz)
        {
            this.conductorCommandBuffer = createNewCommandBuffer(sz);
            return this;
        }

        public Context receiverCommandBuffer(final int sz)
        {
            this.receiverCommandBuffer = createNewCommandBuffer(sz);
            return this;
        }

        public Context bufferManagement(final BufferManagement bufferManagement)
        {
            this.bufferManagement = bufferManagement;
            return this;
        }

        public Context conductorCommsBuffers(final ConductorMappedBuffers conductorMappedBuffers)
        {
            this.conductorMappedBuffers = conductorMappedBuffers;
            return this;
        }

        public Context rcvNioSelector(final NioSelector nioSelector)
        {
            this.rcvNioSelector = nioSelector;
            return this;
        }

        public Context adminNioSelector(final NioSelector nioSelector)
        {
            this.adminNioSelector = nioSelector;
            return this;
        }

        public Context senderFlowControl(Supplier<SenderControlStrategy> senderFlowControl)
        {
            this.senderFlowControl = senderFlowControl;
            return this;
        }

        public Context mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        public Context adminTimerWheel(final TimerWheel wheel)
        {
            this.adminTimerWheel = wheel;
            return this;
        }

        public RingBuffer conductorCommandBuffer()
        {
            return conductorCommandBuffer;
        }

        public RingBuffer receiverCommandBuffer()
        {
            return receiverCommandBuffer;
        }

        public BufferManagement bufferManagement()
        {
            return bufferManagement;
        }

        public ConductorMappedBuffers conductorCommsBuffers()
        {
            return conductorMappedBuffers;
        }

        public NioSelector rcvNioSelector()
        {
            return rcvNioSelector;
        }

        public NioSelector adminNioSelector()
        {
            return adminNioSelector;
        }

        public Supplier<SenderControlStrategy> senderFlowControl()
        {
            return senderFlowControl;
        }

        public int mtuLength()
        {
            return mtuLength;
        }

        public RcvFrameHandlerFactory rcvFrameHandlerFactory()
        {
            return rcvFrameHandlerFactory;
        }

        public Context rcvFrameHandlerFactory(final RcvFrameHandlerFactory rcvFrameHandlerFactory)
        {
            this.rcvFrameHandlerFactory = rcvFrameHandlerFactory;
            return this;
        }

        public TimerWheel adminTimerWheel()
        {
            return adminTimerWheel;
        }
    }
}
