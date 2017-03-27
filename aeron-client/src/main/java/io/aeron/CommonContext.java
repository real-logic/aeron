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

import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.function.Consumer;

import static java.lang.System.getProperty;

/**
 * This class provides the Media Driver and client with common configuration for the Aeron directory.
 *
 * Properties
 * <ul>
 * <li><code>aeron.dir</code>: Use value as directory name for Aeron buffers and status.</li>
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
     * URI used for Spy {@link Subscription}s whereby an outgoing unicast or multicast publication can be spied on
     * by IPC without receiving it again via the network.
     */
    public static final String SPY_PREFIX = "aeron-spy:";

    /**
     * Timeout in which the driver is expected to respond.
     */
    public static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000;

    /**
     * Initial term id to be used when creating an {@link ExclusivePublication}.
     */
    public static final String INITIAL_TERM_ID_PARAM_NAME = "init-term-id";

    /**
     * Current term id to be used when creating an {@link ExclusivePublication}.
     */
    public static final String TERM_ID_PARAM_NAME = "term-id";

    /**
     * Current term offset to be used when creating an {@link ExclusivePublication}.
     */
    public static final String TERM_OFFSET_PARAM_NAME = "term-offset";

    /**
     * The param name to be used for the term length as a channel URI param.
     */
    public static final String TERM_LENGTH_PARAM_NAME = "term-length";

    /**
     * MTU length parameter name for using as a channel URI param.
     */
    public static final String MTU_LENGTH_URI_PARAM_NAME = "mtu";

    /**
     * Key for the mode of control that such be used for multi-destination-cast semantics.
     */
    public static final String MDC_CONTROL_MODE = "control-mode";

    /**
     * Valid value for {@link #MDC_CONTROL_MODE} when manual control is desired.
     */
    public static final String MDC_CONTROL_MODE_MANUAL = "manual";

    /**
     * Parameter name for channel URI param to indicate if a subscribed must be reliable or not. Value is boolean.
     */
    public static final String RELIABLE_STREAM_PARAM_NAME = "reliable";

    private long driverTimeoutMs = DEFAULT_DRIVER_TIMEOUT_MS;
    private String aeronDirectoryName;
    private File cncFile;
    private UnsafeBuffer countersMetaDataBuffer;
    private UnsafeBuffer countersValuesBuffer;

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
     * Get the buffer containing the counter meta data.
     *
     * @return The buffer storing the counter meta data.
     */
    public UnsafeBuffer countersMetaDataBuffer()
    {
        return countersMetaDataBuffer;
    }

    /**
     * Set the buffer containing the counter meta data.
     *
     * @param countersMetaDataBuffer The new counter meta data buffer.
     * @return this Object for method chaining.
     */
    public CommonContext countersMetaDataBuffer(final UnsafeBuffer countersMetaDataBuffer)
    {
        this.countersMetaDataBuffer = countersMetaDataBuffer;
        return this;
    }

    /**
     * Get the buffer containing the counters.
     *
     * @return The buffer storing the counters.
     */
    public UnsafeBuffer countersValuesBuffer()
    {
        return countersValuesBuffer;
    }

    /**
     * Set the buffer containing the counters
     *
     * @param countersValuesBuffer The new counters buffer.
     * @return this Object for method chaining.
     */
    public CommonContext countersValuesBuffer(final UnsafeBuffer countersValuesBuffer)
    {
        this.countersValuesBuffer = countersValuesBuffer;
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
     * Read the error log to a given {@link PrintStream}
     *
     * @param out to write the error log contents to.
     * @return the number of observations from the error log
     */
    public int saveErrorLog(final PrintStream out)
    {
        final File dirFile = new File(aeronDirectoryName);
        int distinctErrorCount = 0;

        if (dirFile.exists() && dirFile.isDirectory())
        {
            final File cncFile = new File(aeronDirectoryName, CncFileDescriptor.CNC_FILE);

            if (cncFile.exists())
            {
                MappedByteBuffer cncByteBuffer = null;

                try
                {
                    cncByteBuffer = IoUtil.mapExistingFile(cncFile, CncFileDescriptor.CNC_FILE);
                    final UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

                    final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

                    if (CncFileDescriptor.CNC_VERSION != cncVersion)
                    {
                        throw new IllegalStateException("aeron cnc file version not understood: version=" + cncVersion);
                    }

                    final AtomicBuffer buffer = CncFileDescriptor.createErrorLogBuffer(
                        cncByteBuffer, cncMetaDataBuffer);
                    final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

                    distinctErrorCount = ErrorLogReader.read(
                        buffer,
                        (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                            out.format(
                                "***%n%d observations from %s to %s for:%n %s%n",
                                observationCount,
                                dateFormat.format(new Date(firstObservationTimestamp)),
                                dateFormat.format(new Date(lastObservationTimestamp)),
                                encodedException));

                    out.format("%n%d distinct errors observed.%n", distinctErrorCount);
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

        return distinctErrorCount;
    }

    /**
     * Release resources used by the CommonContext.
     */
    public void close()
    {
    }
}
