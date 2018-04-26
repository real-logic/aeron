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

import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Publication;
import io.aeron.cluster.service.ClusteredService;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@Ignore
public class MultiNodeTest
{
    private static final String THREE_NODE_MEMBERS =
        "0,localhost:9010,localhost:9020,localhost:9030,localhost:8010|" +
        "1,localhost:9011,localhost:9021,localhost:9031,localhost:8011|" +
        "2,localhost:9012,localhost:9022,localhost:9032,localhost:8012";

    private final MemberStatusListener[] mockMemberStatusListeners = new MemberStatusListener[3];

    @Before
    public void before()
    {
        for (int i = 0; i < mockMemberStatusListeners.length; i++)
        {
            mockMemberStatusListeners[i] = mock(MemberStatusListener.class);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeLeaderStaticThreeNodeConfigWithElection()
    {
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .appointedLeaderId(0);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, true, true, false))
        {
            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onRequestVote(0, 0, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(0, 0, 0);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(1),
                0,
                0,
                1,
                true);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(2),
                0,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onNewLeadershipTerm(eq(0L), eq(0L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onNewLeadershipTerm(eq(0L), eq(0L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 0, 0, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 0, 0, 2);

            harness.awaitServiceOnStart();
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeFollowerStaticThreeNodeConfigWithElection()
    {
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .appointedLeaderId(1);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, true, true, false))
        {
            harness.memberStatusPublisher().requestVote(
                harness.memberStatusPublication(1), 0, 0, 1);

            do
            {
                harness.awaitMemberStatusMessage(1);
            }
            while (harness.memberStatusCounters(1).onVoteCounter == 0);

            verify(mockMemberStatusListeners[1]).onVote(0, 1, 0, true);

            final int logSessionId = 123456;
            final ChannelUri channelUri = ChannelUri.parse(context.logChannel());
            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
            final Publication logPublication =
                harness.aeron().addExclusivePublication(channelUri.toString(), context.logStreamId());

            final ChannelUriStringBuilder destinationUri = new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(harness.member(0).logEndpoint());

            logPublication.addDestination(destinationUri.build());

            harness.memberStatusPublisher().newLeadershipTerm(
                harness.memberStatusPublication(1), 0, 0, 1, logSessionId);

            harness.awaitMemberStatusMessage(1);

            verify(mockMemberStatusListeners[1]).onAppendedPosition(0, 0, 0);

            harness.awaitServiceOnStart();
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeLeaderStaticThreeNodeConfigWithElectionFromPreviousLog()
    {
        final long position = ConsensusModuleHarness.makeRecordingLog(
            10, 100, null, null, new ConsensusModule.Context());
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .appointedLeaderId(0);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, false, true, false))
        {
            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onRequestVote(position, 1, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(position, 1, 0);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(1),
                1,
                0,
                1,
                true);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(2),
                1,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1);
            harness.awaitMemberStatusMessage(2);

            verify(mockMemberStatusListeners[1]).onNewLeadershipTerm(
                eq(position), eq(1L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onNewLeadershipTerm(
                eq(position), eq(1L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 0, 1, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 0, 1, 2);

            harness.awaitServiceOnStart();
            harness.awaitServiceOnMessageCounter(10);

            verify(mockService, times(10))
                .onSessionMessage(anyLong(), anyLong(), anyLong(), any(), anyInt(), eq(100), any());
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeFollowerStaticThreeNodeConfigWithElectionFromPreviousLog()
    {
        final long position = ConsensusModuleHarness.makeRecordingLog(
            10, 100, null, null, new ConsensusModule.Context());
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .appointedLeaderId(1);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, false, true, false))
        {
            harness.memberStatusPublisher().requestVote(
                harness.memberStatusPublication(1), position, 1, 1);

            harness.awaitMemberStatusMessage(1);

            verify(mockMemberStatusListeners[1]).onVote(1, 1, 0, true);

            final int logSessionId = 123456;
            final ChannelUri channelUri = ChannelUri.parse(context.logChannel());
            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
            final Publication logPublication =
                harness.aeron().addExclusivePublication(channelUri.toString(), context.logStreamId());

            final ChannelUriStringBuilder destinationUri = new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(harness.member(0).logEndpoint());

            logPublication.addDestination(destinationUri.build());

            harness.memberStatusPublisher().newLeadershipTerm(
                harness.memberStatusPublication(1), position, 1L, 1, logSessionId);

            harness.awaitMemberStatusMessage(1);

            verify(mockMemberStatusListeners[1]).onAppendedPosition(position, 1, 0);

            harness.awaitServiceOnStart();
            harness.awaitServiceOnMessageCounter(10);

            verify(mockService, times(10))
                .onSessionMessage(anyLong(), anyLong(), anyLong(), any(), anyInt(), eq(100), any());
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBecomeFollowerStaticThreeNodeConfigWithElectionFromPreviousLogWithCatchUp()
    {
        final ConsensusModule.Context followerContext = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .clusterMemberId(1)
            .appointedLeaderId(0);

        final ConsensusModule.Context leaderContext = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS)
            .clusterMemberId(0)
            .appointedLeaderId(0);

        final File followerHarnessDir = ConsensusModuleHarness.harnessDirectory(1);
        final File leaderHarnessDir = ConsensusModuleHarness.harnessDirectory(0);
        final Long2LongHashMap positionMap = new Long2LongHashMap(-1);

        IoUtil.delete(followerHarnessDir, true);
        IoUtil.delete(leaderHarnessDir, true);

        final long position = ConsensusModuleHarness.makeRecordingLog(
            10, 100, null, positionMap, new ConsensusModule.Context());

        IoUtil.delete(new File(leaderHarnessDir, ConsensusModuleHarness.DRIVER_DIRECTORY), true);
        ConsensusModuleHarness.copyDirectory(leaderHarnessDir, followerHarnessDir);

        final long truncatePosition = positionMap.get(5);

        ConsensusModuleHarness.truncateRecordingLog(followerHarnessDir, 0, truncatePosition);

        final ClusteredService mockFollowerService = mock(ClusteredService.class);
        final ClusteredService mockLeaderService = mock(ClusteredService.class);

        final MemberStatusListener[] mockFollowerStatusListeners = new MemberStatusListener[3];
        final MemberStatusListener[] mockLeaderStatusListeners = new MemberStatusListener[3];

        mockLeaderStatusListeners[2] = mock(MemberStatusListener.class);

        try (ConsensusModuleHarness leaderHarness = new ConsensusModuleHarness(
            leaderContext, mockLeaderService, mockLeaderStatusListeners, false, true, false);
            ConsensusModuleHarness followerHarness = new ConsensusModuleHarness(
                followerContext, mockFollowerService, mockFollowerStatusListeners, false, true, false))
        {
            leaderHarness.awaitMemberStatusMessage(2);

            verify(mockLeaderStatusListeners[2]).onRequestVote(position, 1, 0);

            leaderHarness.memberStatusPublisher().placeVote(
                leaderHarness.memberStatusPublication(2),
                1,
                0,
                2,
                true);

            leaderHarness.awaitMemberStatusMessage(2);

            verify(mockLeaderStatusListeners[2]).onNewLeadershipTerm(
                eq(position), eq(1L), eq(0), anyInt());

            leaderHarness.memberStatusPublisher().appendedPosition(
                leaderHarness.memberStatusPublication(2), position, 1, 2);

            leaderHarness.awaitServiceOnStart();
            followerHarness.awaitServiceOnStart();
            followerHarness.awaitServiceOnMessageCounter(10);

            verify(mockFollowerService, times(10))
                .onSessionMessage(anyLong(), anyLong(), anyLong(), any(), anyInt(), eq(100), any());

            // wait until Leader sends commitPosition after election. This will only work while Leader waits for
            // all followers.
            do
            {
                leaderHarness.awaitMemberStatusMessage(2);
            }
            while (leaderHarness.memberStatusCounters(2).onCommitPositionCounter == 0);
        }
    }
}
