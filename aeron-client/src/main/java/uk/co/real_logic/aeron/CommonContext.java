/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.UUID;
import java.util.function.Consumer;

import static java.lang.System.getProperty;

/**
 * This class provides the Media Driver and client with common configuration for the Aeron directory.
 * <p>
 * Properties
 * <ul>
 * <li><code>aeron.dir</code>: Use value as directory name for Aeron buffers and stats.</li>
 * </ul>
 */
public class CommonContext implements AutoCloseable
{
    /**
     * The top level Aeron directory used for communication between a Media Driver and client.
     */
    public static final String AERON_DIR_PROP_NAME = "aeron.dir";

    /**
     * The value of the top level Aeron directory unless overridden by {@link #aeronDirectoryName(String)}
     */
    public static final String AERON_DIR_PROP_DEFAULT;

    /**
     * URI used for IPC {@link Publication}s and {@link Subscription}s
     */
    public static final String IPC_CHANNEL = "aeron:ipc";

    /**
     * Timeout in which the driver is expected to respond.
     */
    public static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000;

    private long driverTimeoutMs = DEFAULT_DRIVER_TIMEOUT_MS;
    private String aeronDirectoryName;
    private File cncFile;
    private UnsafeBuffer counterLabelsBuffer;
    private UnsafeBuffer counterValuesBuffer;

    static
    {
        String baseDirName = IoUtil.tmpDirName() + "aeron";

        // Use shared memory on Linux to avoid contention on the page cache.
        if ("Linux".equalsIgnoreCase(System.getProperty("os.name")))
        {
            final File devShmDir = new File("/dev/shm");

            if (devShmDir.exists())
            {
                baseDirName = "/dev/shm/aeron";
            }
        }

        AERON_DIR_PROP_DEFAULT = baseDirName + "-" + System.getProperty("user.name", "default");
    }

    /**
     * Convert the default Aeron directory name to be a random name for use with embedded drivers.
     *
     * @return random directory name with default directory name as base
     */
    public static String generateRandomDirName()
    {
        final String randomDirName = UUID.randomUUID().toString();

        return AERON_DIR_PROP_DEFAULT + "-" + randomDirName;
    }

    /**
     * Create a new context with Aeron directory and delete on exit values based on the current system properties.
     */
    public CommonContext()
    {
        aeronDirectoryName = getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
    }

    /**
     * This completes initialization of the CommonContext object. It is automatically called by subclasses.
     *
     * @return this Object for method chaining.
     */
    public CommonContext conclude()
    {
        cncFile = new File(aeronDirectoryName, CncFileDescriptor.CNC_FILE);
        return this;
    }

    /**
     * Get the top level Aeron directory used for communication between the client and Media Driver, and
     * the location of the data buffers.
     *
     * @return The top level Aeron directory.
     */
    public String aeronDirectoryName()
    {
        return aeronDirectoryName;
    }

    /**
     * Set the top level Aeron directory used for communication between the client and Media Driver, and the location
     * of the data buffers.
     *
     * @param dirName New top level Aeron directory.
     * @return this Object for method chaining.
     */
    public CommonContext aeronDirectoryName(final String dirName)
    {
        this.aeronDirectoryName = dirName;
        return this;
    }

    /**
     * Create a new command and control file in the administration directory.
     *
     * @return The newly created File.
     */
    public static File newDefaultCncFile()
    {
        return new File(getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT), CncFileDescriptor.CNC_FILE);
    }

    /**
     * Get the buffer containing the counter labels.
     *
     * @return The buffer storing the counter labels.
     */
    public UnsafeBuffer counterLabelsBuffer()
    {
        return counterLabelsBuffer;
    }

    /**
     * Set the buffer containing the counter labels.
     *
     * @param counterLabelsBuffer The new counter labels buffer.
     * @return this Object for method chaining.
     */
    public CommonContext counterLabelsBuffer(final UnsafeBuffer counterLabelsBuffer)
    {
        this.counterLabelsBuffer = counterLabelsBuffer;
        return this;
    }

    /**
     * Get the buffer containing the counters.
     *
     * @return The buffer storing the counters.
     */
    public UnsafeBuffer counterValuesBuffer()
    {
        return counterValuesBuffer;
    }

    /**
     * Set the buffer containing the counters
     *
     * @param counterValuesBuffer The new counters buffer.
     * @return this Object for method chaining.
     */
    public CommonContext counterValuesBuffer(final UnsafeBuffer counterValuesBuffer)
    {
        this.counterValuesBuffer = counterValuesBuffer;
        return this;
    }

    /**
     * Get the command and control file.
     *
     * @return The command and control file.
     */
    public File cncFile()
    {
        return cncFile;
    }

    /**
     * Set the driver timeout in milliseconds
     *
     * @param driverTimeoutMs to indicate liveness of driver
     * @return driver timeout in milliseconds
     */
    public CommonContext driverTimeoutMs(final long driverTimeoutMs)
    {
        this.driverTimeoutMs = driverTimeoutMs;
        return this;
    }

    /**
     * Get the driver timeout in milliseconds.
     *
     * @return driver timeout in milliseconds.
     */
    public long driverTimeoutMs()
    {
        return driverTimeoutMs;
    }

    /**
     * Delete the current Aeron directory, throwing errors if not possible.
     */
    public void deleteAeronDirectory()
    {
        final File dirFile = new File(aeronDirectoryName);

        IoUtil.delete(dirFile, false);
    }

    /**
     * Is a media driver active in the current Aeron directory?
     *
     * @param driverTimeoutMs for the driver liveness check
     * @param logHandler      for feedback as liveness checked
     * @return true if a driver is active or false if not
     */
    public boolean isDriverActive(final long driverTimeoutMs, final Consumer<String> logHandler)
    {
        final File dirFile = new File(aeronDirectoryName);

        if (dirFile.exists() && dirFile.isDirectory())
        {
            final File cncFile = new File(aeronDirectoryName, CncFileDescriptor.CNC_FILE);

            logHandler.accept(String.format("INFO: Aeron directory %s exists", dirFile));

            if (cncFile.exists())
            {
                MappedByteBuffer cncByteBuffer = null;

                logHandler.accept(String.format("INFO: Aeron CnC file %s exists", cncFile));

                try
                {
                    cncByteBuffer = IoUtil.mapExistingFile(cncFile, CncFileDescriptor.CNC_FILE);
                    final UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

                    final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

                    if (CncFileDescriptor.CNC_VERSION != cncVersion)
                    {
                        throw new IllegalStateException("aeron cnc file version not understood: version=" + cncVersion);
                    }

                    final ManyToOneRingBuffer toDriverBuffer = new ManyToOneRingBuffer(
                        CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                    final long timestamp = toDriverBuffer.consumerHeartbeatTime();
                    final long now = System.currentTimeMillis();
                    final long diff = now - timestamp;

                    logHandler.accept(String.format("INFO: Aeron toDriver consumer heartbeat is %d ms old", diff));

                    if (diff <= driverTimeoutMs)
                    {
                        return true;
                    }
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
                finally
                {
                    IoUtil.unmap(cncByteBuffer);
                }
            }
        }

        return false;
    }

    /**
     * Release resources used by the CommonContext.
     */
    public void close()
    {
    }
}
