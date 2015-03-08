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
 * <li><code>aeron.dir.conductor</code>: Use value as directory name for conductor buffers.</li>
 * <li><code>aeron.dir.data</code>: Use value as directory name for data buffers.</li>
 * </ul>
 */
public class CommonContext implements AutoCloseable
{
    /** Top level Aeron directory */
    public static final String AERON_DIR_PROP_NAME = "aeron.dir";

    public static final String DATA_DIR_NAME = "data";
    public static final String CONDUCTOR_DIR_NAME = "conductor";
    public static final String COUNTERS_DIR_NAME = "counters";

    /** Directory of the data buffers */
    public static final String DATA_DIR_PROP_NAME = "aeron.dir.data";
    /** Default directory for data buffers */
    public static final String DATA_DIR_PROP_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + DATA_DIR_NAME;

    /** Directory of the conductor buffers */
    public static final String ADMIN_DIR_PROP_NAME = "aeron.dir.conductor";
    /** Default directory for conductor buffers */
    public static final String ADMIN_DIR_PROP_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + CONDUCTOR_DIR_NAME;

    /** Name of the default multicast interface */
    public static final String MULTICAST_DEFAULT_INTERFACE_PROP_NAME = "aeron.multicast.default.interface";

    /** Attempt to delete directories on exit */
    public static final String DIRS_DELETE_ON_EXIT_PROP_NAME = "aeron.dir.delete.on.exit";

    private String dataDirName;
    private String adminDirName;
    private boolean dirsDeleteOnExit;
    private File cncFile;
    private UnsafeBuffer counterLabelsBuffer;
    private UnsafeBuffer countersBuffer;

    public CommonContext()
    {
        final String aeronDir = getProperty(AERON_DIR_PROP_NAME);

        if (null == aeronDir)
        {
            dataDirName(getProperty(DATA_DIR_PROP_NAME, DATA_DIR_PROP_DEFAULT));
            adminDirName(getProperty(ADMIN_DIR_PROP_NAME, ADMIN_DIR_PROP_DEFAULT));
        }
        else
        {
            dataDirName(getProperty(DATA_DIR_PROP_NAME, aeronDir + File.separator + DATA_DIR_NAME));
            adminDirName(getProperty(ADMIN_DIR_PROP_NAME, aeronDir + File.separator + CONDUCTOR_DIR_NAME));
        }

        dirsDeleteOnExit(getBoolean(DIRS_DELETE_ON_EXIT_PROP_NAME));
    }

    public CommonContext conclude()
    {
        cncFile = new File(adminDirName(), CncFileDescriptor.CNC_FILE);

        return this;
    }

    public String dataDirName()
    {
        return dataDirName;
    }

    public CommonContext dataDirName(final String dataDirName)
    {
        this.dataDirName = dataDirName;
        return this;
    }

    public static File newDefaultCncFile()
    {
        return new File(getProperty(ADMIN_DIR_PROP_NAME, ADMIN_DIR_PROP_DEFAULT), CncFileDescriptor.CNC_FILE);
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

    public CommonContext adminDirName(final String adminDirName)
    {
        this.adminDirName = adminDirName;
        return this;
    }

    public String adminDirName()
    {
        return adminDirName;
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
