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

import uk.co.real_logic.aeron.mediadriver.buffer.BasicBufferManagementStrategy;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy;
import uk.co.real_logic.aeron.util.AdminBufferStrategy;
import uk.co.real_logic.aeron.util.CommonConfiguration;
import uk.co.real_logic.aeron.util.CreatingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

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
 *     <li><code>aeron.admin.dir</code>: Use value as directory name for admin buffers.</li>
 *     <li><code>aeron.data.dir</code>: Use value as directory name for data buffers.</li>
 *     <li><code>aeron.recv.bytebuffer.size</code>: Use int value as size of buffer for receiving from network.</li>
 *     <li><code>aeron.command.buffer.size</code>: Use int value as size of the command buffers between threads.</li>
 *     <li><code>aeron.admin.buffer.size</code>: Use int value as size of the admin buffers between the media driver
       and the client.</li>
 *     <li><code>aeron.select.timeout</code>: use int value as default timeout for NIO select calls</li>
 * </ul>
 */
public class MediaDriver implements AutoCloseable
{
    /** Byte buffer size (in bytes) for reads */
    public static final String READ_BYTE_BUFFER_SZ_PROPERTY_NAME = "aeron.recv.bytebuffer.size";

    /** Size (in bytes) of the command buffers between threads */
    public static final String COMMAND_BUFFER_SZ_PROPERTY_NAME = "aeron.command.buffer.size";

    /** Size (in bytes) of the admin buffers between the media driver and the client */
    public static final String ADMIN_BUFFER_SZ_PROPERTY_NAME = "aeron.admin.buffer.size";

    /** Timeout (in msec) for the basic NIO select call */
    public static final String SELECT_TIMEOUT_PROPERTY_NAME = "aeron.select.timeout";

    /** Default byte buffer size for reads */
    public static final int READ_BYTE_BUFFER_SZ_DEFAULT = 4096;

    /** Default buffer size for command buffers between threads */
    public static final int COMMAND_BUFFER_SZ_DEFAULT = 65536;

    /** Default buffer size for admin buffers between the media driver and the client */
    public static final int ADMIN_BUFFER_SZ_DEFAULT = 65536 + TRAILER_LENGTH;

    /** Default timeout for select */
    public static final int SELECT_TIMEOUT_DEFAULT = 20;

    public static final int READ_BYTE_BUFFER_SZ = Integer.getInteger(READ_BYTE_BUFFER_SZ_PROPERTY_NAME,
                                                                     READ_BYTE_BUFFER_SZ_DEFAULT).intValue();
    public static final int COMMAND_BUFFER_SZ = Integer.getInteger(COMMAND_BUFFER_SZ_PROPERTY_NAME,
                                                                   COMMAND_BUFFER_SZ_DEFAULT).intValue();
    public static final int ADMIN_BUFFER_SZ = Integer.getInteger(ADMIN_BUFFER_SZ_PROPERTY_NAME,
                                                                 ADMIN_BUFFER_SZ_DEFAULT).intValue();
    public static final int SELECT_TIMEOUT = Integer.getInteger(SELECT_TIMEOUT_PROPERTY_NAME,
                                                                SELECT_TIMEOUT_DEFAULT).intValue();

    public static void main(final String[] args)
    {
        try (final MediaDriver mediaDriver = new MediaDriver())
        {
            // 1 for Receive Thread (Sockets to Buffers)
            // 1 for Send Thread (Buffers to Sockets)
            // 1 for Admin Thread (Buffer Management, NAK, Retransmit, etc.)
            Executor executor = Executors.newFixedThreadPool(3);

            executor.execute(mediaDriver.receiverThread());
            executor.execute(mediaDriver.senderThread());
            executor.execute(mediaDriver.adminThread());
        }
        catch (final InterruptedException ie)
        {
            // catch this OK. We should finally close on it also... oh look, try-with-resources just did.
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
    }

    private final ReceiverThread receiverThread;
    private final SenderThread senderThread;
    private final MediaDriverAdminThread adminThread;

    public MediaDriver() throws Exception
    {
        NioSelector nioSelector = new NioSelector();
        MediaDriverContext context = new MediaDriverContext().adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(nioSelector)
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(new CreatingAdminBufferStrategy(CommonConfiguration.ADMIN_DIR, ADMIN_BUFFER_SZ))
                .bufferManagementStrategy(new BasicBufferManagementStrategy(CommonConfiguration.DATA_DIR))
                .mtuLength(CommonConfiguration.MTU_LENGTH);

        context.rcvFrameHandlerFactory(new RcvFrameHandlerFactory(nioSelector,
                new MediaDriverAdminThreadCursor(context.adminThreadCommandBuffer(), nioSelector)));

        receiverThread = new ReceiverThread(context);
        senderThread = new SenderThread(context);
        adminThread = new MediaDriverAdminThread(context, receiverThread, senderThread);
    }

    public ReceiverThread receiverThread()
    {
        return receiverThread;
    }

    public SenderThread senderThread()
    {
        return senderThread;
    }

    public MediaDriverAdminThread adminThread()
    {
        return adminThread;
    }

    public void close() throws Exception
    {
        receiverThread.close();
        senderThread.close();
        adminThread.close();
    }

    public static class MediaDriverContext
    {
        private RingBuffer adminThreadCommandBuffer;
        private RingBuffer receiverThreadCommandBuffer;
        private ReceiverThreadCursor receiverThreadCursor;
        private BufferManagementStrategy bufferManagementStrategy;
        private AdminBufferStrategy adminBufferStrategy;
        private NioSelector rcvNioSelector;
        private NioSelector adminNioSelector;
        private Supplier<SenderFlowControlStrategy> senderFlowControl;
        private int mtuLength;
        private RcvFrameHandlerFactory rcvFrameHandlerFactory;

        private RingBuffer createNewCommandBuffer(final int sz)
        {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(sz + TRAILER_LENGTH);
            final AtomicBuffer atomicBuffer = new AtomicBuffer(byteBuffer);

            return new ManyToOneRingBuffer(atomicBuffer);
        }

        public MediaDriverContext adminThreadCommandBuffer(final int sz)
        {
            this.adminThreadCommandBuffer = createNewCommandBuffer(sz);
            return this;
        }

        public MediaDriverContext receiverThreadCommandBuffer(final int sz)
        {
            this.receiverThreadCommandBuffer = createNewCommandBuffer(sz);
            return this;
        }

        public MediaDriverContext bufferManagementStrategy(final BufferManagementStrategy strategy)
        {
            this.bufferManagementStrategy = strategy;
            return this;
        }

        public MediaDriverContext adminBufferStrategy(final AdminBufferStrategy adminBufferStrategy)
        {
            this.adminBufferStrategy = adminBufferStrategy;
            return this;
        }

        public MediaDriverContext rcvNioSelector(final NioSelector nioSelector)
        {
            this.rcvNioSelector = nioSelector;
            return this;
        }

        public MediaDriverContext adminNioSelector(final NioSelector nioSelector)
        {
            this.adminNioSelector = nioSelector;
            return this;
        }

        public MediaDriverContext senderFlowControl(Supplier<SenderFlowControlStrategy> senderFlowControl)
        {
            this.senderFlowControl = senderFlowControl;
            return this;
        }

        public MediaDriverContext mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        public RingBuffer adminThreadCommandBuffer()
        {
            return adminThreadCommandBuffer;
        }

        public RingBuffer receiverThreadCommandBuffer()
        {
            return receiverThreadCommandBuffer;
        }

        public BufferManagementStrategy bufferManagementStrategy()
        {
            return bufferManagementStrategy;
        }

        public AdminBufferStrategy adminBufferStrategy()
        {
            return adminBufferStrategy;
        }

        public NioSelector rcvNioSelector()
        {
            return rcvNioSelector;
        }

        public NioSelector adminNioSelector()
        {
            return adminNioSelector;
        }

        public Supplier<SenderFlowControlStrategy> senderFlowControl()
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

        public MediaDriverContext rcvFrameHandlerFactory(final RcvFrameHandlerFactory rcvFrameHandlerFactory)
        {
            this.rcvFrameHandlerFactory = rcvFrameHandlerFactory;
            return this;
        }
    }
}
