/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.echo.api;

/**
 * MBean interface for the provisioning service to request manage the creation of pub/sub echo pairs.
 */
public interface ProvisioningMBean
{
    /**
     * Provision a pub/sub echo pair on the remote provisioning service.
     *
     * @param correlationId user supplied correlationId
     * @param subChannel channel for echo subscription.
     * @param subStreamId stream id for echo subscription.
     * @param pubChannel channel for echo publication.
     * @param pubStreamId stream id for echo publication.
     */
    void createEchoPair(
        long correlationId,
        String subChannel,
        int subStreamId,
        String pubChannel,
        int pubStreamId);

    /**
     * Removes all echo pairs on remote provisioning service.
     */
    void removeAll();
}
