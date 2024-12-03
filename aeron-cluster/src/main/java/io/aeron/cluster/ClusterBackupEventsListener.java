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

import java.util.List;

/**
 * Listener which can be registered via {@link ClusterBackup.Context} for tracking backup progress.
 */
public interface ClusterBackupEventsListener
{
    /**
     * Backup has moved into backup query state. Backup process has been started.
     */
    void onBackupQuery();

    /**
     * Possible failure of cluster leader detected.
     *
     * @param ex the underlying exception.
     */
    void onPossibleFailure(Exception ex);

    /**
     * Backup response was received for a backup query.
     *
     * @param clusterMembers      in the backup response.
     * @param logSourceMember     to be used to replicate data from.
     * @param snapshotsToRetrieve snapshots to be retrieved.
     */
    void onBackupResponse(
        ClusterMember[] clusterMembers, ClusterMember logSourceMember, List<RecordingLog.Snapshot> snapshotsToRetrieve);

    /**
     * Updated recording log.
     *
     * @param recordingLog       that was updated.
     * @param snapshotsRetrieved the snapshots that were retrieved.
     */
    void onUpdatedRecordingLog(RecordingLog recordingLog, List<RecordingLog.Snapshot> snapshotsRetrieved);

    /**
     * Update to the live log position as recorded to the local archive.
     *
     * @param recordingId           of the live log.
     * @param recordingPosCounterId {@link io.aeron.archive.status.RecordingPos} counter id for the live log.
     * @param logPosition           of the live log.
     */
    void onLiveLogProgress(long recordingId, long recordingPosCounterId, long logPosition);
}
