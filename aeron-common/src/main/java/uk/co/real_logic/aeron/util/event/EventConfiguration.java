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
package uk.co.real_logic.aeron.util.event;

import uk.co.real_logic.aeron.util.IoUtil;

import java.io.File;

/**
 * Common configuration elements between event loggers and event reader side
 */
public class EventConfiguration
{
    /** Event Buffer location system property name */
    public static final String LOCATION_PROPERTY_NAME = "aeron.event.buffer.location";
    /** Event Buffer size system property name */
    public static final String BUFFER_SIZE_PROPERTY_NAME = "aeron.event.buffer.size";
    /** Event Buffer location deleted on exit system property name */
    public static final String DELETE_ON_EXIT_PROPERTY_NAME = "aeron.event.buffer.delete-on-exit";
    /** Event Logging enabled or not system property name */
    public static final String LOGGER_ON_PROPERTY_NAME = "aeron.event.log";

    /** Event Buffer default location */
    public static final String LOCATION_DEFAULT = IoUtil.tmpDirName() + "aeron" + File.separator + "event-buffer";
    /** Event Buffer default size (in bytes) */
    public static final long BUFFER_SIZE_DEFAULT = 65536;

    /** Maximum length of an event in bytes */
    public final static int MAX_EVENT_LENGTH = 1024;
}
