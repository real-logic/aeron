/*
 *  Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.Agent;

public class ClusterBackupAgent implements Agent
{
    private AeronArchive backupArchive;

    private AeronArchive clusterArchive;

    ClusterBackupAgent(final ClusterBackup.Context context)
    {

    }

    public void onStart()
    {

    }

    public void onClose()
    {

    }

    public int doWork()
    {
        return 0;
    }

    public String roleName()
    {
        return "cluster-backup";
    }
}
