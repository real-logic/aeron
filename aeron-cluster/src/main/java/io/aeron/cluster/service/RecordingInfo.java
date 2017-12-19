/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import org.agrona.collections.Long2ObjectHashMap;

class RecordingInfo implements RecordingDescriptorConsumer
{
    public long recordingId;
    public long startTimestamp;
    public long stopTimestamp;
    public long startPosition;
    public long stopPosition;
    public int sessionId;

    public static Long2ObjectHashMap<RecordingInfo> mapRecordings(
        final AeronArchive archive,
        final long fromRecordingId,
        final int recordCount,
        final String recordingChannel,
        final int recordingStreamId)
    {
        final Long2ObjectHashMap<RecordingInfo> map = new Long2ObjectHashMap<>();

        archive.listRecordingsForUri(
            fromRecordingId,
            recordCount,
            recordingChannel,
            recordingStreamId,
            (
                controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity
            ) ->
            {
                final RecordingInfo info = new RecordingInfo();

                info.recordingId = recordingId;
                info.startTimestamp = startTimestamp;
                info.stopTimestamp = stopTimestamp;
                info.startPosition = startPosition;
                info.stopPosition = stopPosition;
                info.sessionId = sessionId;

                map.put(recordingId, info);
            });

        return map;
    }

    public static RecordingInfo findLatestRecording(final Long2ObjectHashMap<RecordingInfo> recordingInfoByIdMap)
    {
        RecordingInfo savedInfo = null;
        long startTimestamp = 0;

        for (final RecordingInfo recordingInfo : recordingInfoByIdMap.values())
        {
            if (recordingInfo.startTimestamp >= startTimestamp)
            {
                savedInfo = recordingInfo;
                startTimestamp = savedInfo.startTimestamp;
            }
        }

        return savedInfo;
    }

    public void onRecordingDescriptor(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        this.recordingId = recordingId;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.sessionId = sessionId;
    }

    public String toString()
    {
        return "RecordingInfo{" +
            "recordingId=" + recordingId +
            ", startTimestamp=" + startTimestamp +
            ", stopTimestamp=" + stopTimestamp +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", sessionId=" + sessionId +
            '}';
    }
}
