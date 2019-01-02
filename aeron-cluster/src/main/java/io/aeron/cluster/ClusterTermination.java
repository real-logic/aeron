/*
 * Copyright 2014-2019 Real Logic Ltd.
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

class ClusterTermination
{
    private final MemberStatusPublisher memberStatusPublisher;
    private long deadlineMs;
    private boolean hasServiceTerminated = false;

    ClusterTermination(final MemberStatusPublisher memberStatusPublisher, final long deadlineMs)
    {
        this.deadlineMs = deadlineMs;
        this.memberStatusPublisher = memberStatusPublisher;
    }

    void deadlineMs(final long deadlineMs)
    {
        this.deadlineMs = deadlineMs;
    }

    boolean canTerminate(final ClusterMember[] members, final long terminationPosition, final long nowMs)
    {
        if (!hasServiceTerminated)
        {
            return false;
        }

        return haveFollowersTerminated(members, terminationPosition) || nowMs >= deadlineMs;
    }

    void hasServiceTerminated(final boolean hasServiceTerminated)
    {
        this.hasServiceTerminated = hasServiceTerminated;
    }

    void terminationPosition(final ClusterMember[] members, final ClusterMember thisMember, final long position)
    {
        for (final ClusterMember member : members)
        {
            member.hasSentTerminationAck(false);

            if (member != thisMember)
            {
                memberStatusPublisher.terminationPosition(member.publication(), position);
            }
        }
    }

    private static boolean haveFollowersTerminated(final ClusterMember[] members, final long terminationPosition)
    {
        boolean result = true;

        for (int i = 0, length = members.length; i < length; i++)
        {
            final ClusterMember member = members[i];

            if (!member.hasSentTerminationAck() && member.logPosition() < terminationPosition)
            {
                result = false;
                break;
            }
        }

        return result;
    }
}
