/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseEncoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveEventDissectorTest
{
    @Test
    void controlResponse()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(1024, 32));
        final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
        responseEncoder.wrapAndApplyHeader(buffer, 16, new MessageHeaderEncoder())
            .controlSessionId(13)
            .correlationId(42)
            .relevantId(8)
            .code(ControlResponseCode.NULL_VAL)
            .version(111)
            .errorMessage("the %ERR% msg");
        final StringBuilder builder = new StringBuilder();

        ArchiveEventDissector.controlResponse(buffer, 16, builder);

        assertEquals("ARCHIVE: CONTROL_RESPONSE" +
            ", controlSessionId=13" +
            ", correlationId=42" +
            ", relevantId=8" +
            ", code=" + ControlResponseCode.NULL_VAL +
            ", version=111" +
            ", errorMessage=the %ERR% msg",
            builder.toString());
    }
}
