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
package io.aeron.archiver;

import io.aeron.archiver.client.ResponseListener;
import io.aeron.archiver.codecs.ControlResponseCode;

import static org.junit.Assert.fail;

public class FailResponseListener implements ResponseListener
{
    public void onResponse(final ControlResponseCode code, final String errorMessage, final long correlationId)
    {
        fail();
    }

    public void onReplayStarted(final long replayId, final long correlationId)
    {
        fail();
    }

    public void onReplayAborted(final long lastPosition, final long correlationId)
    {
        fail();
    }

    public void onRecordingDescriptor(
        final long correlationId,
        final long recordingId,
        final int segmentFileLength,
        final int termBufferLength,
        final long startTime,
        final long joiningPosition,
        final long endTime,
        final long lastPosition,
        final int sessionId,
        final int streamId,
        final String channel,
        final String sourceIdentity)
    {
        fail();
    }

    public void onRecordingNotFound(final long recordingId, final long maxRecordingId, final long correlationId)
    {
        fail();
    }
}
