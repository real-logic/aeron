/*
 * Copyright 2014-2022 Real Logic Limited.
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

import static io.aeron.cluster.ClusterMember.quorumPosition;
import static io.aeron.cluster.ClusterMember.quorumThreshold;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    void parseShouldIgnoreEscapedPipe()
    {
        final ClusterMember[] parsedMembers = ClusterMember.parse(
            "0,ingressEndpoint\\|interface=ingressEndpoint-interface," +
                "consensusEndpoint\\|interface=consensusEndpoint-interface\\|mtu=2048," +
                "logEndpoint\\|interface=logEndpoint-interface,catchupEndpoint\\|interface=catchupEndpoint-interface," +
                "archiveEndpoint\\|interface=archiveEndpoint-interface|" +
                "1,ingressEndpoint1,consensusEndpoint1,logEndpoint1\\|interface=logEndpoint1-interface," +
                "catchupEndpoint1,archiveEndpoint1\\|interface=archiveEndpoint1-interface|");

        final ClusterMember member0 = parsedMembers[0];
        assertEquals(0, member0.id());
        assertEquals("ingressEndpoint|interface=ingressEndpoint-interface", member0.ingressEndpoint());
        assertEquals("consensusEndpoint|interface=consensusEndpoint-interface|mtu=2048", member0.consensusEndpoint());
        assertEquals("logEndpoint|interface=logEndpoint-interface", member0.logEndpoint());
        assertEquals("catchupEndpoint|interface=catchupEndpoint-interface", member0.catchupEndpoint());
        assertEquals("archiveEndpoint|interface=archiveEndpoint-interface", member0.archiveEndpoint());

        final ClusterMember member1 = parsedMembers[1];
        assertEquals(1, member1.id());
        assertEquals("ingressEndpoint1", member1.ingressEndpoint());
        assertEquals("consensusEndpoint1", member1.consensusEndpoint());
        assertEquals("logEndpoint1|interface=logEndpoint1-interface", member1.logEndpoint());
        assertEquals("catchupEndpoint1", member1.catchupEndpoint());
        assertEquals("archiveEndpoint1|interface=archiveEndpoint1-interface", member1.archiveEndpoint());
    }
}
