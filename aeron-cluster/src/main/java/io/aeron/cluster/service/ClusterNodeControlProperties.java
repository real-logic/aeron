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
package io.aeron.cluster.service;

/**
 * Data class for holding the properties used when interacting with a cluster for local admin control.
 *
 * @see io.aeron.cluster.ClusterTool
 * @see ClusterMarkFile
 */
public final class ClusterNodeControlProperties
{
    /**
     * Directory where the Aeron Media Driver is running.
     */
    public final String aeronDirectoryName;

    /**
     * URI for the control channel.
     */
    public final String controlChannel;

    /**
     * Stream id in the control channel on which the services listen.
     */
    public final int serviceStreamId;

    /**
     * Stream id in the control channel on which the consensus module listens.
     */
    public final int consensusModuleStreamId;

    /**
     * Construct the set of properties for interacting with a cluster.
     *
     * @param serviceStreamId         in the control channel on which the services listen.
     * @param consensusModuleStreamId in the control channel on which the consensus module listens.
     * @param aeronDirectoryName      where the Aeron Media Driver is running.
     * @param controlChannel          for the services and consensus module.
     */
    public ClusterNodeControlProperties(
        final int serviceStreamId,
        final int consensusModuleStreamId,
        final String aeronDirectoryName,
        final String controlChannel)
    {
        this.aeronDirectoryName = aeronDirectoryName;
        this.controlChannel = controlChannel;
        this.serviceStreamId = serviceStreamId;
        this.consensusModuleStreamId = consensusModuleStreamId;
    }
}
