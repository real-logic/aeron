/*
 *  Copyright 2014-2020 Real Logic Limited.
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
 * Data class for holding the properties used when interacting with a cluster for admin control.
 *
 * @see io.aeron.cluster.ClusterTool
 * @see ClusterMarkFile
 */
public class ClusterNodeControlProperties
{
    public final String aeronDirectoryName;
    public final String archiveChannel;
    public final String serviceControlChannel;
    public final int toServiceStreamId;
    public final int toConsensusModuleStreamId;

    public ClusterNodeControlProperties(
        final int toServiceStreamId,
        final int toConsensusModuleStreamId,
        final String aeronDirectoryName,
        final String archiveChannel,
        final String serviceControlChannel)
    {
        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveChannel = archiveChannel;
        this.serviceControlChannel = serviceControlChannel;
        this.toServiceStreamId = toServiceStreamId;
        this.toConsensusModuleStreamId = toConsensusModuleStreamId;
    }
}
