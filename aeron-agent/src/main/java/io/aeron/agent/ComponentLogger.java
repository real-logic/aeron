/*
 * Copyright 2014-2022 Real Logic Limited.
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

import net.bytebuddy.agent.builder.AgentBuilder;
import org.agrona.MutableDirectBuffer;

/**
 * Interface that describes a logger for a given Aeron component.
 */
public interface ComponentLogger
{
    /**
     * The type code to distinguish this logger when encoding/decoding messages.
     *
     * @return the type code for this logger.
     */
    int typeCode();

    /**
     * Returns true of there are no event codes defined for this ComponentLogger.
     * @return <code>true</code> if the configured event codes is empty.
     */
    boolean isEventCodesEmpty();

    /**
     * Decode a message on the reader side.
     *
     * @param buffer      containing the message.
     * @param index       offset in the buffer to the message.
     * @param eventCodeId of the event to be decoded.
     * @param builder     to render the message to.
     */
    void decode(MutableDirectBuffer buffer, int index, int eventCodeId, StringBuilder builder);

    /**
     * Add instrumentation to the code to log the appropriate messages.
     *
     * @param agentBuilder builder to use to instrument the java code.
     * @return the updated agent builder after instrumentation has been applied.
     */
    AgentBuilder addInstrumentation(AgentBuilder agentBuilder);
}
