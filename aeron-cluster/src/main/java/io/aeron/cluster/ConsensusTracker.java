/*
 * Copyright 2016 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.ConsensusPos;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;

public class ConsensusTracker implements AutoCloseable
{
    private final long recordingId;
    private final ReadableCounter recordingPosition;
    private final Counter consensusPosition;

    public ConsensusTracker(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final long logPosition,
        final long messageIndex,
        final int sessionId,
        final IdleStrategy idleStrategy)
    {
        final CountersReader countersReader = aeron.countersReader();

        idleStrategy.reset();
        int recordingCounterId = RecordingPos.findActiveCounterIdBySession(countersReader, sessionId);
        while (RecordingPos.NULL_COUNTER_ID == recordingCounterId)
        {
            checkInterruptedStatus();
            idleStrategy.idle();

            recordingCounterId = RecordingPos.findActiveCounterIdBySession(countersReader, sessionId);
        }

        recordingPosition = new ReadableCounter(countersReader, recordingCounterId);

        recordingId = RecordingPos.getRecordingId(countersReader, recordingCounterId);
        consensusPosition = ConsensusPos.allocate(aeron, tempBuffer, recordingId, logPosition, messageIndex, sessionId);
    }

    public void close()
    {
        CloseHelper.close(consensusPosition);
    }

    public void updatePosition()
    {
        final long position = recordingPosition.get();
        if (position > consensusPosition.getWeak())
        {
            consensusPosition.setOrdered(position);
        }
    }

    public long recordingId()
    {
        return recordingId;
    }

    private void checkInterruptedStatus()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new RuntimeException("Unexpected interrupt");
        }
    }
}
