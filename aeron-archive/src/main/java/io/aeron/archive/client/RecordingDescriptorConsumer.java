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
package io.aeron.archive.client;

import io.aeron.Image;
import io.aeron.Subscription;

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
     * @param startTimestamp    of the recording.
     * @param stopTimestamp     of the recording.
     * @param startPosition     of the recording against the recorded publication, the {@link Image#joinPosition()}.
     * @param stopPosition      reached for the recording, final position for {@link Image#position()}.
     * @param initialTermId     of the recorded stream, {@link Image#initialTermId()}.
     * @param segmentFileLength of the recording which is a multiple of termBufferLength.
     * @param termBufferLength  of the recorded stream, {@link Image#termBufferLength()}.
     * @param mtuLength         of the recorded stream, {@link Image#mtuLength()}.
     * @param sessionId         of the recorded stream, this will be the most recent session id for extended recordings.
     * @param streamId          of the recorded stream, {@link Subscription#streamId()}.
     * @param strippedChannel   of the recorded stream which is used for the recording subscription in the archive.
     * @param originalChannel   of the recorded stream provided to the start recording request, {@link Subscription#channel()}.
     * @param sourceIdentity    of the recorded stream, the {@link Image#sourceIdentity()}.
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
