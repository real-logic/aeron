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
package io.aeron.archive.client;

/**
 * Consumer of events describing Aeron stream recordings.
 */
@FunctionalInterface
public interface RecordingDescriptorConsumer
{
    /**
     * A recording descriptor returned as a result of requesting a listing of recordings.
     *
     * @param controlSessionId  of the originating session requesting to list recordings.
     * @param correlationId     of the associated request to list recordings.
     * @param recordingId       of this recording descriptor.
     * @param startTimestamp    for the recording.
     * @param stopTimestamp     for the recording.
     * @param startPosition     for the recording against the recorded publication.
     * @param stopPosition      reached for the recording.
     * @param initialTermId     for the recorded publication.
     * @param segmentFileLength for the recording which is a multiple of termBufferLength.
     * @param termBufferLength  for the recorded publication.
     * @param mtuLength         for the recorded publication.
     * @param sessionId         for the recorded publication.
     * @param streamId          for the recorded publication.
     * @param strippedChannel   for the recorded publication.
     * @param originalChannel   for the recorded publication.
     * @param sourceIdentity    for the recorded publication.
     */
    void onRecordingDescriptor(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long startTimestamp,
        long stopTimestamp,
        long startPosition,
        long stopPosition,
        int initialTermId,
        int segmentFileLength,
        int termBufferLength,
        int mtuLength,
        int sessionId,
        int streamId,
        String strippedChannel,
        String originalChannel,
        String sourceIdentity);
}
