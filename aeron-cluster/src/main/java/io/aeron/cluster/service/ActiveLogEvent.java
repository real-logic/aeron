/*
 * Copyright 2018 Real Logic Ltd.
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

/**
 * Event to signal a change of active log to follow.
 */
class ActiveLogEvent
{
    final long leadershipTermId;
    final int commitPositionId;
    final int sessionId;
    final int streamId;
    final String channel;

    ActiveLogEvent(
        final long leadershipTermId,
        final int commitPositionId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        this.leadershipTermId = leadershipTermId;
        this.commitPositionId = commitPositionId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.channel = channel;
    }

    public String toString()
    {
        return "ActiveLogEvent{" +
            "leadershipTermId=" + leadershipTermId +
            ", commitPositionId=" + commitPositionId +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", channel='" + channel + '\'' +
            '}';
    }
}
