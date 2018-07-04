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
import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
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
        "0,localhost:9010,localhost:9020,localhost:9030,localhost:9040,localhost:8010|" +
        "1,localhost:9011,localhost:9021,localhost:9031,localhost:9041,localhost:8011|" +
        "2,localhost:9012,localhost:9022,localhost:9032,localhost:9042,localhost:8012";

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
            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(1),
                -1L,
                0L,
                1);

            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(2),
                -1L,
                0L,
                2);

            harness.awaitMemberStatusMessage(1, harness.onRequestVoteCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onRequestVoteCounter(2));

            verify(mockMemberStatusListeners[1]).onRequestVote(-1L, 0L, 0L, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(-1L, 0L, 0L, 0);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(1),
                0L,
                -1L,
                0,
                0,
                1,
                true);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(2),
                0L,
                -1L,
                0,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1, harness.onNewLeadershipTermCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onNewLeadershipTermCounter(2));

            verify(mockMemberStatusListeners[1]).onNewLeadershipTerm(eq(-1L), eq(0L), eq(0L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onNewLeadershipTerm(eq(-1L), eq(0L), eq(0L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 0L, 0L, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 0L, 0L, 2);

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
            harness.awaitMemberStatusMessage(1, harness.onCanvassPosition(1));

            verify(mockMemberStatusListeners[1], atLeastOnce()).onCanvassPosition(-1L, 0L, 0);

            harness.memberStatusPublisher().requestVote(
                harness.memberStatusPublication(1), -1L, 0L, 0L, 1);

            harness.awaitMemberStatusMessage(1, harness.onVoteCounter(1));

            verify(mockMemberStatusListeners[1]).onVote(0L, -1L, 0L, 1, 0, true);

            final Publication publication = harness.createLogPublication(
                ChannelUri.parse(context.logChannel()), null, 0L, true);

            harness.memberStatusPublisher().newLeadershipTerm(
                harness.memberStatusPublication(1), -1L, 0L, 0L, 1, publication.sessionId());

            harness.awaitMemberStatusMessage(1, harness.onAppendedPositionCounter(1));

            verify(mockMemberStatusListeners[1]).onAppendedPosition(0L, 0L, 0);

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
            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(1),
                0L,
                position,
                1);

            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(2),
                0L,
                position,
                2);

            harness.awaitMemberStatusMessage(1, harness.onRequestVoteCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onRequestVoteCounter(2));

            verify(mockMemberStatusListeners[1]).onRequestVote(0L, position, 1L, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(0L, position, 1L, 0);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(1),
                1L,
                0L,
                position,
                0,
                1,
                true);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(2),
                1L,
                0L,
                position,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1, harness.onNewLeadershipTermCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onNewLeadershipTermCounter(2));

            verify(mockMemberStatusListeners[1]).onNewLeadershipTerm(
                eq(0L), eq(position), eq(1L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onNewLeadershipTerm(
                eq(0L), eq(position), eq(1L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 1L, position, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 1L, position, 2);

            harness.awaitServiceOnStart();
            harness.awaitServiceOnMessageCounter(10);

            verify(mockService, times(10))
                .onSessionMessage(any(ClientSession.class), anyLong(), anyLong(), any(), anyInt(), eq(100), any());

            harness.awaitMemberStatusMessage(1, harness.onCommitPosition(1));
            harness.awaitMemberStatusMessage(2, harness.onCommitPosition(2));

            verify(mockMemberStatusListeners[1]).onCommitPosition(1, position, 0);
            verify(mockMemberStatusListeners[2]).onCommitPosition(1, position, 0);
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
            final RecordingExtent recordingExtent = new RecordingExtent();
            harness.recordingExtent(0, recordingExtent);

            harness.awaitMemberStatusMessage(1, harness.onCanvassPosition(1));

            verify(mockMemberStatusListeners[1], atLeastOnce()).onCanvassPosition(0, position, 0);

            harness.memberStatusPublisher().requestVote(
                harness.memberStatusPublication(1), 0L, position, 1L, 1);

            harness.awaitMemberStatusMessage(1, harness.onVoteCounter(1));

            verify(mockMemberStatusListeners[1]).onVote(1L, 0L, position, 1, 0, true);

            final Publication publication = harness.createLogPublication(
                ChannelUri.parse(context.logChannel()), recordingExtent, position, false);

            harness.memberStatusPublisher().newLeadershipTerm(
                harness.memberStatusPublication(1), 0L, position, 1L, 1, publication.sessionId());

            harness.awaitMemberStatusMessage(1, harness.onAppendedPositionCounter(1));

            verify(mockMemberStatusListeners[1], atLeastOnce()).onAppendedPosition(1L, position, 0);

            harness.awaitServiceOnStart();
            harness.awaitServiceOnMessageCounter(10);

            verify(mockService, times(10))
                .onSessionMessage(any(ClientSession.class), anyLong(), anyLong(), any(), anyInt(), eq(100), any());
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

        // 96 for NewLeadershipTermEvent at start of the log
        final long truncatePosition = positionMap.get(5) + 96;

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
            leaderHarness.memberStatusPublisher().canvassPosition(
                leaderHarness.memberStatusPublication(2),
                0L,
                position,
                2);

            leaderHarness.awaitMemberStatusMessage(2, leaderHarness.onRequestVoteCounter(2));

            verify(mockLeaderStatusListeners[2]).onRequestVote(0L, position, 1L, 0);

            leaderHarness.memberStatusPublisher().placeVote(
                leaderHarness.memberStatusPublication(2),
                1L,
                0L,
                position,
                0,
                2,
                true);

            leaderHarness.awaitMemberStatusMessage(2, leaderHarness.onNewLeadershipTermCounter(2));

            verify(mockLeaderStatusListeners[2], atLeastOnce()).onNewLeadershipTerm(
                eq(0L), eq(position), eq(1L), eq(0), anyInt());

            leaderHarness.memberStatusPublisher().appendedPosition(
                leaderHarness.memberStatusPublication(2), 1L, position, 2);

            leaderHarness.awaitServiceOnStart();
            followerHarness.awaitServiceOnStart();
            followerHarness.awaitServiceOnMessageCounter(10);

            verify(mockFollowerService, times(10))
                .onSessionMessage(any(ClientSession.class), anyLong(), anyLong(), any(), anyInt(), eq(100), any());

            // wait until Leader sends commitPosition after election. This will only work while Leader waits for
            // all followers.
            leaderHarness.awaitMemberStatusMessage(2, leaderHarness.onCommitPosition(2));

            verify(mockLeaderStatusListeners[2]).onCommitPosition(1, position, 0);
        }
    }

    @Ignore
    @Test(timeout = 10_000L)
    public void shouldBecomeLeaderStaticNonAppointedThreeNodeConfigWithElection()
    {
        final ClusteredService mockService = mock(ClusteredService.class);

        final ConsensusModule.Context context = new ConsensusModule.Context()
            .clusterMembers(THREE_NODE_MEMBERS);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context, mockService, mockMemberStatusListeners, true, true, false))
        {
            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(1),
                -1L,
                0L,
                1);

            harness.memberStatusPublisher().canvassPosition(
                harness.memberStatusPublication(2),
                -1L,
                0L,
                2);

            harness.awaitMemberStatusMessage(1, harness.onCanvassPosition(1));
            harness.awaitMemberStatusMessage(2, harness.onCanvassPosition(2));

            verify(mockMemberStatusListeners[1], atLeastOnce()).onCanvassPosition(-1L, 0L, 0);
            verify(mockMemberStatusListeners[2], atLeastOnce()).onCanvassPosition(-1L, 0L, 0);

            harness.awaitMemberStatusMessage(1, harness.onRequestVoteCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onRequestVoteCounter(2));

            verify(mockMemberStatusListeners[1]).onRequestVote(-1L, 0L, 0L, 0);
            verify(mockMemberStatusListeners[2]).onRequestVote(-1L, 0L, 0L, 0);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(1),
                0L,
                -1L,
                0,
                0,
                1,
                true);

            harness.memberStatusPublisher().placeVote(
                harness.memberStatusPublication(2),
                0L,
                -1L,
                0,
                0,
                2,
                true);

            harness.awaitMemberStatusMessage(1, harness.onNewLeadershipTermCounter(1));
            harness.awaitMemberStatusMessage(2, harness.onNewLeadershipTermCounter(2));

            verify(mockMemberStatusListeners[1]).onNewLeadershipTerm(eq(-1L), eq(0L), eq(0L), eq(0), anyInt());
            verify(mockMemberStatusListeners[2]).onNewLeadershipTerm(eq(-1L), eq(0L), eq(0L), eq(0), anyInt());

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(1), 0L, 0L, 1);

            harness.memberStatusPublisher().appendedPosition(
                harness.memberStatusPublication(2), 0L, 0L, 2);

            harness.awaitServiceOnStart();
        }
    }
}
