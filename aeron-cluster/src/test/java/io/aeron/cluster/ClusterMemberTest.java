/*
 * Copyright 2014-2023 Real Logic Limited.
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

public class ClusterMemberTest
{
    private final ClusterMember[] members = ClusterMember.parse(
        "0,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
        "1,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|" +
        "2,ingressEndpoint,consensusEndpoint,logEndpoint,catchupEndpoint,archiveEndpoint|");

    private final long[] rankedPositions = new long[quorumThreshold(members.length)];

    @Test
    public void shouldDetermineQuorumSize()
    {
        final int[] clusterSizes = new int[]{ 1, 2, 3, 4, 5, 6, 7 };
        final int[] quorumValues = new int[]{ 1, 2, 2, 3, 3, 4, 4 };

        for (int i = 0, length = clusterSizes.length; i < length; i++)
        {
            final int quorumThreshold = quorumThreshold(clusterSizes[i]);
            assertThat("Cluster size: " + clusterSizes[i], quorumThreshold, is(quorumValues[i]));
        }
    }

    @Test
    public void shouldRankClusterStart()
    {
        assertThat(quorumPosition(members, rankedPositions), is(0L));
    }

    @Test
    public void shouldDetermineQuorumPosition()
    {
        final long[][] positions = new long[][]
        {
            { 0, 0, 0 },
            { 123, 0, 0 },
            { 123, 123, 0 },
            { 123, 123, 123 },
            { 0, 123, 123 },
            { 0, 0, 123 },
            { 0, 123, 200 },
        };

        final long[] quorumPositions = new long[]{ 0, 0, 123, 123, 123, 0, 123 };

        for (int i = 0, length = positions.length; i < length; i++)
        {
            final long[] memberPositions = positions[i];
            for (int j = 0; j < memberPositions.length; j++)
            {
                members[j].logPosition(memberPositions[j]);
            }

            final long quorumPosition = quorumPosition(members, rankedPositions);
            assertThat("Test: " + i, quorumPosition, is(quorumPositions[i]));
        }
    }

    @Test
    void isUnanimousCandidateReturnFalseIfThereIsAMemberWithoutLogPosition()
    {
        final ClusterMember candidate = newMember(4, 100, 1000);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1, 2, 100),
            newMember(2, 8, NULL_POSITION),
            newMember(3, 1, 1)
        };

        assertFalse(isUnanimousCandidate(members, candidate));
    }

    @Test
    void isUnanimousCandidateReturnFalseIfThereIsAMemberWithMoreUpToDateLog()
    {
        final ClusterMember candidate = newMember(4, 10, 800);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1, 2, 100),
            newMember(2, 8, 6),
            newMember(3, 11, 1000)
        };

        assertFalse(isUnanimousCandidate(members, candidate));
    }

    @Test
    void isUnanimousCandidateReturnTrueIfTheCandidateHasTheMostUpToDateLog()
    {
        final ClusterMember candidate = newMember(2, 10, 800);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100),
            newMember(20, 8, 6),
            newMember(30, 10, 800)
        };

        assertTrue(isUnanimousCandidate(members, candidate));
    }

    @Test
    void isQuorumCandidateReturnFalseWhenQuorumIsNotReached()
    {
        final ClusterMember candidate = newMember(2, 10, 800);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100),
            newMember(20, 18, 6),
            newMember(30, 10, 800),
            newMember(40, 19, 800),
            newMember(50, 10, 1000),
        };

        assertFalse(isQuorumCandidate(members, candidate));
    }

    @Test
    void isQuorumCandidateReturnTrueWhenQuorumIsReached()
    {
        final ClusterMember candidate = newMember(2, 10, 800);
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(10, 2, 100),
            newMember(20, 18, 6),
            newMember(30, 10, 800),
            newMember(40, 9, 800),
            newMember(50, 10, 700)
        };

        assertTrue(isQuorumCandidate(members, candidate));
    }

    @Test
    void hasQuorumVotesReturnsTrueWhenQuorumIsReached()
    {
        final int candidateTermId = -5;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId * 2).vote(Boolean.FALSE),
            newMember(3).candidateTermId(candidateTermId).vote(null),
            newMember(4).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(5).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertTrue(hasQuorumVote(members, candidateTermId));
    }

    @Test
    void hasQuorumVotesReturnsFalseIfAtLeastOneNegativeVoteIsDetected()
    {
        final int candidateTermId = 8;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.FALSE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(hasQuorumVote(members, candidateTermId));
    }

    @Test
    void hasQuorumVotesReturnsFalseWhenQuorumIsNotReached()
    {
        final int candidateTermId = 2;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(null),
            newMember(3).candidateTermId(candidateTermId + 5).vote(Boolean.TRUE)
        };

        assertFalse(hasQuorumVote(members, candidateTermId));
    }

    @Test
    void hasUnanimousVotesReturnsFalseIfThereIsAtLeastOneNegativeVoteForAGivenCandidateTerm()
    {
        final int candidateTermId = 42;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.FALSE)
        };

        assertFalse(hasUnanimousVote(members, candidateTermId));
    }

    @Test
    void hasUnanimousVotesReturnsFalseIfNotAllNodesVotedPositively()
    {
        final int candidateTermId = 2;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(null),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(hasUnanimousVote(members, candidateTermId));
    }

    @Test
    void hasUnanimousVotesReturnsFalseIfNotAllNodesHadTheExpectedCandidateTermId()
    {
        final int candidateTermId = 2;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId + 1).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertFalse(hasUnanimousVote(members, candidateTermId));
    }

    @Test
    void hasUnanimousVotesReturnsTrueIfAllNodesVotedWithTrue()
    {
        final int candidateTermId = 42;
        final ClusterMember[] members = new ClusterMember[]
        {
            newMember(1).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(2).candidateTermId(candidateTermId).vote(Boolean.TRUE),
            newMember(3).candidateTermId(candidateTermId).vote(Boolean.TRUE)
        };

        assertTrue(hasUnanimousVote(members, candidateTermId));
    }

    @ParameterizedTest
    @CsvSource({
        "5,1000,3,999999,1",
        "-100,99999,4,0,-1",
        "42,371239192371239,42,1001,1",
        "3,-777,3,273291846723894,-1",
        "1,1024,1,1024,0",
    })
    void compareLogReturnsResult(
        final long lhsLogLeadershipTermId,
        final long lhsLogPosition,
        final long rhsLogLeadershipTermId,
        final long rhsLogPosition,
        final int expectedResult)
    {
        assertEquals(
            expectedResult,
            compareLog(lhsLogLeadershipTermId, lhsLogPosition, rhsLogLeadershipTermId, rhsLogPosition));
        assertEquals(expectedResult, compareLog(
            newMember(5, lhsLogLeadershipTermId, lhsLogPosition),
            newMember(100, rhsLogLeadershipTermId, rhsLogPosition)));
    }

    private static ClusterMember newMember(final int id, final long leadershipTermId, final long logPosition)
    {
        final ClusterMember clusterMember = newMember(id);
        return clusterMember.leadershipTermId(leadershipTermId).logPosition(logPosition);
    }

    private static ClusterMember newMember(final int id)
    {
        return new ClusterMember(id, null, null, null, null, null, null);
    }
}
