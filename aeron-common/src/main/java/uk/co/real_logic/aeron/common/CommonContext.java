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
 * Location of context that is common between the client and the media driver.
 *
 * Properties
 * <ul>
 * <li><code>aeron.dir</code>: Use value as directory name for aeron buffers and stats.</li>
 * </ul>
 */
public class CommonContext implements AutoCloseable
{
    /** Top level Aeron directory */
    public static final String AERON_DIR_PROP_NAME = "aeron.dir";
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

    public CommonContext()
    {
        dirName = getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);

        dirsDeleteOnExit(getBoolean(DIRS_DELETE_ON_EXIT_PROP_NAME));
    }

    public CommonContext conclude()
    {
        cncFile = new File(adminDirName(), CncFileDescriptor.CNC_FILE);
        return this;
    }

    public String dirName()
    {
        return dirName;
    }

    public CommonContext dirName(final String dirName)
    {
        this.dirName = dirName;
        return this;
    }

    public String dataDirName()
    {
        return dirName + File.separator + DATA_DIR_NAME;
    }

    public String adminDirName()
    {
        return dirName + File.separator + CONDUCTOR_DIR_NAME;
    }

    public static File newDefaultCncFile()
    {
        return new File(
            getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT) + File.separator + CONDUCTOR_DIR_NAME,
            CncFileDescriptor.CNC_FILE);
    }

    public boolean dirsDeleteOnExit()
    {
        return dirsDeleteOnExit;
    }

    public CommonContext dirsDeleteOnExit(final boolean dirsDeleteOnExit)
    {
        this.dirsDeleteOnExit = dirsDeleteOnExit;
        return this;
    }

    public UnsafeBuffer counterLabelsBuffer()
    {
        return counterLabelsBuffer;
    }

    public CommonContext counterLabelsBuffer(final UnsafeBuffer counterLabelsBuffer)
    {
        this.counterLabelsBuffer = counterLabelsBuffer;
        return this;
    }

    public UnsafeBuffer countersBuffer()
    {
        return countersBuffer;
    }

    public CommonContext countersBuffer(final UnsafeBuffer countersBuffer)
    {
        this.countersBuffer = countersBuffer;
        return this;
    }

    public File cncFile()
    {
        return cncFile;
    }

    public void close()
    {
    }
}
