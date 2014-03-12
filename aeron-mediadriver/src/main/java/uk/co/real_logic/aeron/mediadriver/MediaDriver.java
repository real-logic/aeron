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

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Main class for JVM-based mediadriver
 *
 *
 * <p/>
 * Usage:
 * <code>
 *     <pre>
 *     $ java -jar aeron-mediadriver.jar
 *     $ java -Doption=value -jar aeron-mediadriver.jar
 *     </pre>
 * </code>
 * <p/>
 * Properties
 * <ul>
 *     <li><code>aeron.admin.dir</code>: Use value as directory name for admin buffers.</li>
 *     <li><code>aeron.data.dir</code>: Use value as directory name for data buffers.</li>
 * </ul>
 */
public class MediaDriver
{
    /** Directory of the admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME = "aeron.admin.dir";

    /** Default directory for admin buffers */
    public static final String ADMIN_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/admin";

    /** Directory of the data buffers */
    public static final String DATA_DIR_PROPERTY_NAME = "aeron.data.dir";

    /** Default directory for data buffers */
    public static final String DATA_DIR_PROPERTY_NAME_DEFAULT = "/tmp/aeron/data";

    public static final String ADMIN_DIR = System.getProperty(ADMIN_DIR_PROPERTY_NAME, ADMIN_DIR_PROPERTY_NAME_DEFAULT);
    public static final String DATA_DIR = System.getProperty(DATA_DIR_PROPERTY_NAME, DATA_DIR_PROPERTY_NAME_DEFAULT);

    // This is used by senders to associate Session IDs to SrcFrameHandlers for sending.
    private final Map<Long, SrcFrameHandler> sessionIdMap = new HashMap<>();

    // This is used for tracking receivers (local port numbers to RcvFrameHandlers).
    private final Map<Integer, RcvFrameHandler> rcvPortMap = new HashMap<>();

    public static void main(final String[] args)
    {
        try (final EventLoop selectLoop = new EventLoop())
        {
            // 1 for EventLoop (Selectors)
            // 1 for DataBuffersLoop (Data Buffers)
            // 1 for AdminLoop (Admin Thread)
            Executor executor = Executors.newFixedThreadPool(3);

            executor.execute(selectLoop);

            // TODO: need the other thread scanning Term buffers and control buffer or have this thread do it.

            // TODO: spin off admin thread to do

            // Example of using FrameHandler. A receiver and a source sending to it.

            RcvFrameHandler rcv = new RcvFrameHandler(new InetSocketAddress(41234), selectLoop);
            SrcFrameHandler src = new SrcFrameHandler(new InetSocketAddress(0),
                                                      new InetSocketAddress("localhost", 41234),
                                                      selectLoop);

            while (true)
            {
                Thread.sleep(1000);
            }
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
