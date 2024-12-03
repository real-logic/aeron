/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.cluster.client;

import org.agrona.DirectBuffer;

/**
 * Interface for consuming messages coming from the cluster for an unknown schema.
 */
@FunctionalInterface
public interface EgressListenerExtension
{
    /**
     * Message of unknown schema to egress that can be handled by specific listener implementation.
     *
     * @param actingBlockLength acting block length from header.
     * @param templateId        template id.
     * @param schemaId          schema id.
     * @param actingVersion     acting version.
     * @param buffer            message buffer.
     * @param offset            message offset.
     * @param length            message length.
     */
    void onExtensionMessage(
        int actingBlockLength,
        int templateId,
        int schemaId,
        int actingVersion,
        DirectBuffer buffer,
        int offset,
        int length);
}
