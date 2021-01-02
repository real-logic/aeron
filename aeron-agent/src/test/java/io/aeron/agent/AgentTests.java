/*
 * Copyright 2014-2021 Real Logic Limited.
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
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.junit.jupiter.api.Assertions.*;

public class AgentTests
{
    public static void beforeAgent()
    {
        EventLogAgent.agentmain("", ByteBuddyAgent.install());
    }

    public static void afterAgent()
    {
        EventLogAgent.removeTransformer();
        System.clearProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME);
        EventConfiguration.reset();
    }

    public static void verifyLogHeader(
        final UnsafeBuffer logBuffer,
        final int recordOffset,
        final int eventCodeId,
        final int captureLength,
        final int length)
    {
        assertEquals(HEADER_LENGTH + LOG_HEADER_LENGTH + captureLength,
            logBuffer.getInt(lengthOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(eventCodeId, logBuffer.getInt(typeOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(captureLength, logBuffer.getInt(encodedMsgOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(length, logBuffer.getInt(encodedMsgOffset(recordOffset + SIZE_OF_INT), LITTLE_ENDIAN));
        assertNotEquals(0, logBuffer.getLong(encodedMsgOffset(recordOffset + SIZE_OF_INT * 2), LITTLE_ENDIAN));
    }
}
