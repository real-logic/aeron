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
package io.aeron.cluster;

import org.agrona.DirectBuffer;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;

/**
 * Extension for handling messages from external schemas unknown to core Aeron cluster code
 * thus providing an extension to the core ingress consensus module behaviour.
 */
public interface ConsensusModuleExtension extends AutoCloseable
{
    /**
     * schema supported by this extension
     *
     * @return schema id supported.
     */
    int supportedSchemaId();

    /**
     * Callback for handling messages received as ingress to a cluster.
     * <p>
     * Within this callback reentrant calls to the {@link io.aeron.Aeron} client are not permitted and
     * will result in undefined behaviour.
     *
     * @param schemaId   the schema id
     * @param templateId the message template id (already parsed from header)
     * @param buffer     containing the data.
     * @param offset     at which the data begins.
     * @param length     of the data in bytes.
     * @param header     representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    ControlledFragmentHandler.Action onMessage(
        int schemaId,
        int templateId,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * {@inheritDoc}
     */
    default void close()
    {
    }
}
