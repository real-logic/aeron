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
package io.aeron.test.cluster;

import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupMediaDriver;
import io.aeron.cluster.ClusterTool;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochClock;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

public class TestBackupNode implements AutoCloseable
{
    private final ClusterBackupMediaDriver clusterBackupMediaDriver;
    private boolean isClosed = false;

    TestBackupNode(final Context context)
    {
        clusterBackupMediaDriver = ClusterBackupMediaDriver.launch(
            context.mediaDriverContext, context.archiveContext, context.clusterBackupContext);
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.close(clusterBackupMediaDriver);
        }
    }

    void closeAndDelete()
    {
        if (!isClosed)
        {
            close();
        }

        if (null != clusterBackupMediaDriver)
        {
            clusterBackupMediaDriver.clusterBackup().context().deleteDirectory();
            clusterBackupMediaDriver.archive().context().deleteDirectory();
            clusterBackupMediaDriver.mediaDriver().context().deleteDirectory();
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }

    ClusterBackup.State backupState()
    {
        return ClusterBackup.State.get(clusterBackupMediaDriver.clusterBackup().context().stateCounter());
    }

    long liveLogPosition()
    {
        final Counter counter = clusterBackupMediaDriver.clusterBackup().context().liveLogPositionCounter();
        if (counter.isClosed())
        {
            return NULL_POSITION;
        }

        return counter.get();
    }

    public EpochClock epochClock()
    {
        return clusterBackupMediaDriver.clusterBackup().context().epochClock();
    }

    public long nextBackupQueryDeadlineMs()
    {
        return ClusterTool.nextBackupQueryDeadlineMs(clusterBackupMediaDriver.clusterBackup().context().clusterDir());
    }

    public boolean nextBackupQueryDeadlineMs(final long delayMs)
    {
        final long nowMs = clusterBackupMediaDriver.mediaDriver().context().epochClock().time();

        return ClusterTool.nextBackupQueryDeadlineMs(
            clusterBackupMediaDriver.clusterBackup().context().clusterDir(), nowMs + delayMs);
    }

    static class Context
    {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ClusterBackup.Context clusterBackupContext = new ClusterBackup.Context();
    }
}
