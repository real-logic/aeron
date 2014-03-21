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

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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
 * </ul>
 */
public class MediaDriver
{
    /** Directory of the admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME = "aeron.admin.dir";

    /** Directory of the data buffers */
    public static final String DATA_DIR_PROPERTY_NAME = "aeron.data.dir";

    /** Byte buffer size (in bytes) for reads */
    public static final String READ_BYTE_BUFFER_SZ_PROPERTY_NAME = "aeron.recv.bytebuffer.size";

    /** Size (in bytes) of the command buffers between threads */
    public static final String COMMAND_BUFFER_SZ_PROPERTY_NAME = "aeron.command.buffer.size";

    /** Default directory for admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/admin";

    /** Default directory for data buffers */
    public static final String DATA_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/data";

    /** Default byte buffer size for reads */
    public static final String READ_BYTE_BUFFER_SZ_DEFAULT = "4096";

    /** Default buffer size for command buffers between threads */
    public static final String COMMAND_BUFFER_SZ_DEFAULT = "65536";

    public static final String ADMIN_DIR = System.getProperty(ADMIN_DIR_PROPERTY_NAME, ADMIN_DIR_PROPERTY_NAME_DEFAULT);
    public static final String DATA_DIR = System.getProperty(DATA_DIR_PROPERTY_NAME, DATA_DIR_PROPERTY_NAME_DEFAULT);
    public static final int READ_BYTE_BUFFER_SZ = Integer.parseInt(System.getProperty(READ_BYTE_BUFFER_SZ_PROPERTY_NAME,
            READ_BYTE_BUFFER_SZ_DEFAULT));
    public static final int COMMAND_BUFFER_SZ = Integer.parseInt(System.getProperty(COMMAND_BUFFER_SZ_PROPERTY_NAME,
            COMMAND_BUFFER_SZ_DEFAULT));

    public static void main(final String[] args)
    {
        final ByteBuffer adminThreadBuffer = ByteBuffer.allocateDirect(COMMAND_BUFFER_SZ);
        final ByteBuffer senderThreadBuffer = ByteBuffer.allocateDirect(COMMAND_BUFFER_SZ);
        final ByteBuffer receiverThreadBuffer = ByteBuffer.allocateDirect(COMMAND_BUFFER_SZ);

        // TODO: init buffers so they are ready for writing/reading

        try (final ReceiverThread receiverThread = new ReceiverThread(receiverThreadBuffer, adminThreadBuffer);
             final SenderThread senderThread = new SenderThread(senderThreadBuffer, adminThreadBuffer);
             final AdminThread adminThread = new AdminThread(adminThreadBuffer, receiverThread, senderThread))
        {
            // 1 for Receive Thread (Sockets to Buffers)
            // 1 for Send Thread (Buffers to Sockets)
            // 1 for Admin Thread (Buffer Management, NAK, Retransmit, etc.)
            Executor executor = Executors.newFixedThreadPool(3);

            executor.execute(receiverThread);
            executor.execute(senderThread);
            executor.execute(adminThread);
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
}
