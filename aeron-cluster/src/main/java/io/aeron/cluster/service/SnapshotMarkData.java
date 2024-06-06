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
package io.aeron.cluster.service;

import java.util.concurrent.TimeUnit;

/**
 * Common data for snapshots
 */
public class SnapshotMarkData
{
    private final long snapshotTypeId;
    private final long logPosition;
    private final long leadershipTermId;
    private final int snapshotIndex;
    private final TimeUnit timeUnit;
    private final int appVersion;

    /**
     * constructor
     * @param snapshotTypeId type to identify snapshot within a cluster.
     * @param logPosition at which the snapshot was taken.
     * @param leadershipTermId at which the snapshot was taken.
     * @param snapshotIndex so the snapshot can be sectioned.
     * @param timeUnit of the cluster timestamps stored in the snapshot.
     * @param appVersion associated with the snapshot from {@link ClusteredServiceContainer.Context#appVersion()}.
     */
    public SnapshotMarkData(
        final long snapshotTypeId,
        final long logPosition,
        final long leadershipTermId,
        final int snapshotIndex,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        this.snapshotTypeId = snapshotTypeId;
        this.logPosition = logPosition;
        this.leadershipTermId = leadershipTermId;
        this.snapshotIndex = snapshotIndex;
        this.timeUnit = timeUnit;
        this.appVersion = appVersion;
    }

    /**
     * @return  type to identify snapshot within a cluster.
     */
    public long getSnapshotTypeId()
    {
        return snapshotTypeId;
    }

    /**
     *  @return at which the snapshot was taken.
     */
    public long getLogPosition()
    {
        return logPosition;
    }

    /**
     * @return at which the snapshot was taken.
     */
    public long getLeadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * @return so the snapshot can be sectioned.
     */
    public int getSnapshotIndex()
    {
        return snapshotIndex;
    }

    /**
     * @return of the cluster timestamps stored in the snapshot.
     */
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    /**
     * @return associated with the snapshot from {@link ClusteredServiceContainer.Context#appVersion()}.
     */
    public int getAppVersion()
    {
        return appVersion;
    }
}