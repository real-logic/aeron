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
package uk.co.real_logic.aeron.common;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.IoUtil;

import java.io.File;

import static java.lang.Boolean.getBoolean;
import static java.lang.System.getProperty;

/**
 * This class provides the Media Driver and client with common configuration for the Aeron directory.
 * <p>
 * Properties
 * <ul>
 * <li><code>aeron.dir</code>: Use value as directory name for Aeron buffers and stats.</li>
 * <li><code>aeron.dir.delete.on.exit</code>: Attempt to delete Aeron directories on exit.</li>
 * </ul>
 */
public class CommonContext implements AutoCloseable
{
    /** The top level Aeron directory used for communication between a Media Driver and client.  */
    public static final String AERON_DIR_PROP_NAME = "aeron.dir";
    /** The value of the top level Aeron directory unless overridden by {@link #dirName(String)} */
    public static final String AERON_DIR_PROP_DEFAULT;
    /** Name of the default multicast interface */
    public static final String MULTICAST_DEFAULT_INTERFACE_PROP_NAME = "aeron.multicast.default.interface";
    /** Attempt to delete directories on exit */
    public static final String DIRS_DELETE_ON_EXIT_PROP_NAME = "aeron.dir.delete.on.exit";

    private static final String DATA_DIR_NAME = "data";
    private static final String CONDUCTOR_DIR_NAME = "conductor";

    private String dirName;
    private boolean dirsDeleteOnExit;
    private File cncFile;
    private UnsafeBuffer counterLabelsBuffer;
    private UnsafeBuffer countersBuffer;

    static
    {
        String aeronDirName = IoUtil.tmpDirName() + "aeron";

        // Use shared memory on Linux to avoid contention on the page cache.
        if ("Linux".equalsIgnoreCase(System.getProperty("os.name")))
        {
            final File devShmDir = new File("/dev/shm");

            if (devShmDir.exists())
            {
                aeronDirName = "/dev/shm/aeron";
            }
        }

        AERON_DIR_PROP_DEFAULT = aeronDirName;
    }

    /**
     * Create a new context with Aeron directory and delete on exit values based on the current system properties.
     */
    public CommonContext()
    {
        dirName = getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);

        dirsDeleteOnExit(getBoolean(DIRS_DELETE_ON_EXIT_PROP_NAME));
    }

    /**
     * This completes initialization of the CommonContext object. It is automatically called by subclasses.
     * @return this Object for method chaining.
     */
    public CommonContext conclude()
    {
        cncFile = new File(adminDirName(), CncFileDescriptor.CNC_FILE);
        return this;
    }

    /**
     * Get the top level Aeron directory used for communication between the client and Media Driver, and
     * the location of the data buffers.
     * @return The top level Aeron directory.
     */
    public String dirName()
    {
        return dirName;
    }

    /**
     * Set the top level Aeron directory used for communication between the client and Media Driver, and the location
     * of the data buffers.
     * @param dirName New top level Aeron directory.
     * @return this Object for method chaining.
     */
    public CommonContext dirName(final String dirName)
    {
        this.dirName = dirName;
        return this;
    }

    /**
     * Get the directory used by the Media Driver and client to transfer channel data.
     * @return The data directory.
     */
    public String dataDirName()
    {
        return dirName + File.separator + DATA_DIR_NAME;
    }

    /**
     * Get the directory used by the Media Driver and client to communicate administrative messages.
     * @return The administration directory.
     */
    public String adminDirName()
    {
        return dirName + File.separator + CONDUCTOR_DIR_NAME;
    }

    /**
     * Create a new command and control file in the administration directory.
     * @return The newly created File.
     */
    public static File newDefaultCncFile()
    {
        return new File(
            getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT) + File.separator + CONDUCTOR_DIR_NAME,
            CncFileDescriptor.CNC_FILE);
    }

    /**
     * Get whether or not this application will attempt to delete the Aeron directories when exiting.
     * @return true when directories will be deleted, otherwise false.
     */
    public boolean dirsDeleteOnExit()
    {
        return dirsDeleteOnExit;
    }

    /**
     * Set whether or not this application will attempt to delete the Aeron directories when exiting.
     * @param dirsDeleteOnExit Attempt deletion.
     * @return this Object for method chaining.
     */
    public CommonContext dirsDeleteOnExit(final boolean dirsDeleteOnExit)
    {
        this.dirsDeleteOnExit = dirsDeleteOnExit;
        return this;
    }

    /**
     * Get the buffer containing the counter labels.
     * @return The buffer storing the counter labels.
     */
    public UnsafeBuffer counterLabelsBuffer()
    {
        return counterLabelsBuffer;
    }

    /**
     * Set the buffer containing the counter labels.
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
     * @return The buffer storing the counters.
     */
    public UnsafeBuffer countersBuffer()
    {
        return countersBuffer;
    }

    /**
     * Set the buffer containing the counters
     * @param countersBuffer The new counters buffer.
     * @return this Object for method chaining.
     */
    public CommonContext countersBuffer(final UnsafeBuffer countersBuffer)
    {
        this.countersBuffer = countersBuffer;
        return this;
    }

    /**
     * Get the command and control file.
     * @return The command and control file.
     */
    public File cncFile()
    {
        return cncFile;
    }

    /**
     * Release resources used by the CommonContext.
     */
    public void close()
    {
    }
}
