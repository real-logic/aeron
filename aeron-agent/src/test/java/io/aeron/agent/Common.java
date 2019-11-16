/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.agent;

import net.bytebuddy.agent.ByteBuddyAgent;

public class Common
{
    public static void beforeAgent()
    {
        System.setProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME, "all");
        System.setProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME, "all");
        System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, "all");
        EventLogAgent.agentmain("", ByteBuddyAgent.install());
    }

    public static void afterAfter()
    {
        EventLogAgent.removeTransformer();
        System.clearProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME);
    }
}
