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
package io.aeron.test;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;

import static io.aeron.CommonContext.IPC_CHANNEL;

public class TestContexts
{
    public static final String LOCALHOST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    public static final String LOCALHOST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    public static final String LOCALHOST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    public static final String LOCALHOST_SINGLE_HOST_CLUSTER_MEMBERS =
        "0,localhost:20000,localhost:20001,localhost:20002,localhost:0,localhost:8010";

    public static Archive.Context localhostArchive()
    {
        return new Archive.Context()
            .controlChannel(LOCALHOST_CONTROL_REQUEST_CHANNEL)
            .replicationChannel(LOCALHOST_REPLICATION_CHANNEL);
    }

    public static AeronArchive.Context localhostAeronArchive()
    {
        return new AeronArchive.Context()
            .controlRequestChannel(LOCALHOST_CONTROL_REQUEST_CHANNEL)
            .controlResponseChannel(LOCALHOST_CONTROL_RESPONSE_CHANNEL);
    }

    public static AeronArchive.Context ipcAeronArchive()
    {
        return new AeronArchive.Context()
            .controlRequestChannel(IPC_CHANNEL)
            .controlResponseChannel(IPC_CHANNEL);
    }

    public static ConsensusModule.Context localhostConsensusModule()
    {
        return new ConsensusModule.Context()
            .clusterMembers(LOCALHOST_SINGLE_HOST_CLUSTER_MEMBERS);
    }
}
