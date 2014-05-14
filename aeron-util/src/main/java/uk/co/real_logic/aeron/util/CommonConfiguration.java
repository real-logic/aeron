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
package uk.co.real_logic.aeron.util;

import java.io.File;

import static java.lang.System.getProperty;

/**
 * Location of configuration that is common between the client and the media driver.
 */
public class CommonConfiguration
{
    /** Directory of the data buffers */
    public static final String DATA_DIR_PROP_NAME = "aeron.data.dir";
    /** Default directory for data buffers */
    public static final String DATA_DIR_PROP_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + "data";

    /** Directory of the conductor buffers */
    public static final String ADMIN_DIR_PROP_NAME = "aeron.conductor.dir";
    /** Default directory for conductor buffers */
    public static final String ADMIN_DIR_PROP_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + "conductor";

    /** Directory for the counters */
    public static final String COUNTERS_DIR_PROP_NAME = "aeron.counters.dir";
    /** Default directory for conductor buffers */
    public static final String COUNTERS_DIR_PROP_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + "counters";

    /** Length of the maximum transport unit of the media driver's protocol */
    private static final String MTU_LENGTH_NAME = "aeron.mtu.length";
    private static final int MTU_LENGTH_DEFAULT = 1280;

    public static final String DATA_DIR_NAME = getProperty(DATA_DIR_PROP_NAME, DATA_DIR_PROP_DEFAULT);
    public static final String ADMIN_DIR_NAME = getProperty(ADMIN_DIR_PROP_NAME, ADMIN_DIR_PROP_DEFAULT);
    public static final String COUNTERS_DIR_NAME = getProperty(COUNTERS_DIR_PROP_NAME, COUNTERS_DIR_PROP_DEFAULT);
    public static final int MTU_LENGTH = Integer.getInteger(MTU_LENGTH_NAME, MTU_LENGTH_DEFAULT);
}
