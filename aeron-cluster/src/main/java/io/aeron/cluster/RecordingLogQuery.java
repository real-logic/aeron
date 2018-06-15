/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.cluster.codecs.RecordingLogDecoder;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class RecordingLogQuery
{
    enum State
    {
        AWAIT_RECORDING_LOG,
        DONE
    }

    private final MemberStatusPublisher memberStatusPublisher;
    private final ClusterMember[] clusterMembers;
    private final ConsensusModule.Context context;
    private final long leadershipTermId;
    private final int leaderMemberId;
    private final int memberId;

    private State state = State.AWAIT_RECORDING_LOG;
    private long activeCorrelationId = Aeron.NULL_VALUE;
    private long targetPosition = NULL_POSITION;
    private long leaderRecordingId = Aeron.NULL_VALUE;

    RecordingLogQuery(
        final MemberStatusPublisher memberStatusPublisher,
        final ClusterMember[] clusterMembers,
        final int leaderMemberId,
        final int memberId,
        final long leadershipTermId,
        final ConsensusModule.Context context)
    {
        this.memberStatusPublisher = memberStatusPublisher;
        this.clusterMembers = clusterMembers;
        this.context = context;
        this.leadershipTermId = leadershipTermId;
        this.leaderMemberId = leaderMemberId;
        this.memberId = memberId;
    }

    int doWork()
    {
        int workCount = 0;

        if (State.AWAIT_RECORDING_LOG == state)
        {
            workCount += awaitRecordingLog();
        }

        return workCount;
    }

    boolean isDone()
    {
        return State.DONE == state;
    }

    long activeCorrelationId()
    {
        return activeCorrelationId;
    }

    long targetPosition()
    {
        return targetPosition;
    }

    long leaderRecordingId()
    {
        return leaderRecordingId;
    }

    void onLeaderRecordingLog(final RecordingLogDecoder decoder)
    {
        if (State.AWAIT_RECORDING_LOG == state &&
            decoder.correlationId() == activeCorrelationId &&
            decoder.requestMemberId() == memberId &&
            decoder.leaderMemberId() == leaderMemberId)
        {
            final RecordingLogDecoder.EntriesDecoder entries = decoder.entries();

            if (!entries.hasNext())
            {
                throw new IllegalStateException("no recording log for leadershipTermId=" + leadershipTermId);
            }

            final RecordingLogDecoder.EntriesDecoder logEntry = entries.next();

            leaderRecordingId = logEntry.recordingId();
            targetPosition = logEntry.termBaseLogPosition();

            state(State.DONE);
        }
    }

    private int awaitRecordingLog()
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = context.aeron().nextCorrelationId();

            if (memberStatusPublisher.recordingLogQuery(
                clusterMembers[leaderMemberId].publication(),
                correlationId,
                leaderMemberId,
                memberId,
                leadershipTermId,
                1,
                false))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }

        return workCount;
    }

    private void state(final State state)
    {
        //System.out.println(this.state + " -> " + state);
        this.state = state;
    }
}
