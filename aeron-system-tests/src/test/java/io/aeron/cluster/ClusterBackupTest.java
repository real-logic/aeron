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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.samples.archive.SampleAuthenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.CredentialsSupplier;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestBackupNode;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith(InterruptingTestCallback.class)
class ClusterBackupTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private final CredentialsSupplier simpleCredentialsSupplier = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:admin".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return ArrayUtil.EMPTY_BYTE_ARRAY;
        }
    };

    private final CredentialsSupplier challengeResponseCredentialsSupplier = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:adminC".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return "admin:CSadmin".getBytes(StandardCharsets.US_ASCII);
        }
    };

    private final CredentialsSupplier invalidSimpleCredentialsSupplier = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:invalid".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return ArrayUtil.EMPTY_BYTE_ARRAY;
        }
    };

    private final CredentialsSupplier invalidChallengeResponseCredentialsSupplier = new CredentialsSupplier()
    {
        public byte[] encodedCredentials()
        {
            return "admin:adminC".getBytes(StandardCharsets.US_ASCII);
        }

        public byte[] onChallenge(final byte[] encodedChallenge)
        {
            return "admin:invalid".getBytes(StandardCharsets.US_ASCII);
        }
    };

    @BeforeEach
    void setUp()
    {
        systemTestWatcher.ignoreErrorsMatching(
            (s) -> s.contains("ats_gcm_decrypt final_ex: error:00000000:lib(0):func(0):reason(0)"));
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterNoSnapshotsAndEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(cluster.findLeader().service().cluster().logPosition());
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(0, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterNoSnapshotsAndNonEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterNoSnapshotsAndThenSendMessages()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();
        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterWithSnapshot()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount, node.service().messageCount());
        assertTrue(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterAfterCleanShutdown()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.node(0).isTerminationExpected(true);
        cluster.node(1).isTerminationExpected(true);
        cluster.node(2).isTerminationExpected(true);

        cluster.shutdownCluster(leader);
        cluster.awaitNodeTerminations();

        assertTrue(cluster.node(0).service().wasSnapshotTaken());
        assertTrue(cluster.node(1).service().wasSnapshotTaken());
        assertTrue(cluster.node(2).service().wasSnapshotTaken());

        cluster.stopAllNodes();
        cluster.restartAllNodes(false);
        final TestNode newLeader = cluster.awaitLeader();
        final long logPosition = newLeader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterWithSnapshotAndNonEmptyLog()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);
        cluster.awaitServicesMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(leader, totalMessageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();
        cluster.awaitServiceMessageCount(node, totalMessageCount);

        assertEquals(totalMessageCount, node.service().messageCount());
        assertTrue(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterWithSnapshotAndNonEmptyLogWithSimpleAuthentication()
    {
        final AuthenticatorSupplier authenticatorSupplier = SampleAuthenticator::new;

        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAuthenticationSupplier(authenticatorSupplier)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;

        cluster.connectClient(simpleCredentialsSupplier);
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);
        cluster.awaitServicesMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(leader, totalMessageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true, simpleCredentialsSupplier);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();
        cluster.awaitServiceMessageCount(node, totalMessageCount);

        assertEquals(totalMessageCount, node.service().messageCount());
        assertTrue(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterWithSnapshotAndNonEmptyLogWithChallengeResponseAuthentication()
    {
        final AuthenticatorSupplier authenticatorSupplier = SampleAuthenticator::new;

        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAuthenticationSupplier(authenticatorSupplier)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;

        cluster.connectClient(challengeResponseCredentialsSupplier);
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);
        cluster.awaitServicesMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(leader, totalMessageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.startClusterBackupNode(true, challengeResponseCredentialsSupplier);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();
        cluster.awaitServiceMessageCount(node, totalMessageCount);

        assertEquals(totalMessageCount, node.service().messageCount());
        assertTrue(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldLogErrorWithInvalidCredentials()
    {
        final AuthenticatorSupplier authenticatorSupplier = SampleAuthenticator::new;
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAuthenticationSupplier(authenticatorSupplier)
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("AUTHENTICATION_REJECTED"));

        cluster.awaitLeader();
        final TestBackupNode testBackupNode = cluster.startClusterBackupNode(true, invalidSimpleCredentialsSupplier);

        cluster.awaitBackupState(ClusterBackup.State.RESET_BACKUP);

        awaitErrorLogged(testBackupNode, "AUTHENTICATION_REJECTED");
    }

    @Test
    @InterruptAfter(30)
    void shouldLogErrorWithInvalidChallengeResponse()
    {
        final AuthenticatorSupplier authenticatorSupplier = SampleAuthenticator::new;
        final TestCluster cluster = aCluster()
            .withStaticNodes(3)
            .withAuthenticationSupplier(authenticatorSupplier)
            .start();
        systemTestWatcher.cluster(cluster);
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("AUTHENTICATION_REJECTED"));

        cluster.awaitLeader();
        final TestBackupNode testBackupNode = cluster.startClusterBackupNode(
            true, invalidChallengeResponseCredentialsSupplier);

        cluster.awaitBackupState(ClusterBackup.State.RESET_BACKUP);

        awaitErrorLogged(testBackupNode, "AUTHENTICATION_REJECTED");
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterWithSnapshotThenSend()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int preSnapshotMessageCount = 10;
        final int postSnapshotMessageCount = 7;
        final int totalMessageCount = preSnapshotMessageCount + postSnapshotMessageCount;
        cluster.connectClient();
        cluster.sendMessages(preSnapshotMessageCount);
        cluster.awaitResponseMessageCount(preSnapshotMessageCount);
        cluster.awaitServicesMessageCount(preSnapshotMessageCount);

        cluster.takeSnapshot(leader);
        cluster.awaitSnapshotCount(1);

        cluster.startClusterBackupNode(true);

        cluster.sendMessages(postSnapshotMessageCount);
        cluster.awaitResponseMessageCount(totalMessageCount);
        cluster.awaitServiceMessageCount(leader, totalMessageCount);

        final long logPosition = leader.service().cluster().logPosition();

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();
        cluster.awaitServiceMessageCount(node, totalMessageCount);

        assertEquals(totalMessageCount, node.service().messageCount());
        assertTrue(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(30)
    void shouldBeAbleToGetTimeOfNextBackupQuery()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        cluster.awaitLeader();
        final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);

        final long nowMs = backupNode.epochClock().time();
        assertThat(backupNode.nextBackupQueryDeadlineMs(), greaterThan(nowMs));
    }

    @Test
    @InterruptAfter(30)
    void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithReQuery()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leader = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        final long logPosition = leader.service().cluster().logPosition();
        final TestBackupNode backupNode = cluster.startClusterBackupNode(true);

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);

        assertTrue(backupNode.nextBackupQueryDeadlineMs(0));

        cluster.sendMessages(5);
        cluster.awaitResponseMessageCount(messageCount + 5);
        cluster.awaitServiceMessageCount(leader, messageCount + 5);

        final long nextLogPosition = leader.service().cluster().logPosition();
        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(nextLogPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount + 5, node.service().messageCount());
    }

    @Test
    @InterruptAfter(40)
    void shouldBackupClusterNoSnapshotsAndNonEmptyLogAfterFailure()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderOne = cluster.awaitLeader();

        final int messageCount = 10;
        cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        cluster.stopNode(leaderOne);

        final TestNode leaderTwo = cluster.awaitLeader();
        final long logPosition = leaderTwo.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);
        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    @Test
    @InterruptAfter(60)
    void shouldBackupClusterNoSnapshotsAndNonEmptyLogWithFailure()
    {
        final TestCluster cluster = aCluster().withStaticNodes(3).start();
        systemTestWatcher.cluster(cluster);

        final TestNode leaderOne = cluster.awaitLeader();

        final int messageCount = 10;
        final AeronCluster aeronCluster = cluster.connectClient();
        cluster.sendMessages(messageCount);
        cluster.awaitResponseMessageCount(messageCount);
        cluster.awaitServicesMessageCount(messageCount);

        final long logPosition = leaderOne.service().cluster().logPosition();

        cluster.startClusterBackupNode(true);

        aeronCluster.sendKeepAlive();
        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        aeronCluster.sendKeepAlive();
        cluster.awaitBackupLiveLogPosition(logPosition);
        cluster.stopNode(leaderOne);

        final TestNode leaderTwo = cluster.awaitLeader();
        cluster.awaitNewLeadershipEvent(1);

        cluster.sendMessages(5);
        cluster.awaitResponseMessageCount(messageCount + 5);

        final long nextLogPosition = leaderTwo.service().cluster().logPosition();

        cluster.awaitBackupState(ClusterBackup.State.BACKING_UP);
        cluster.awaitBackupLiveLogPosition(nextLogPosition);
        cluster.stopAllNodes();

        final TestNode node = cluster.startStaticNodeFromBackup();
        cluster.awaitLeader();

        assertEquals(messageCount + 5, node.service().messageCount());
        assertFalse(node.service().wasSnapshotLoaded());
    }

    private static void awaitErrorLogged(final TestBackupNode testBackupNode, final String expectedErrorMessage)
    {
        final AtomicBuffer atomicBuffer = testBackupNode.clusterBackupErrorLog();
        final MutableBoolean foundError = new MutableBoolean();

        Tests.await(() ->
        {
            ErrorLogReader.read(
                atomicBuffer,
                (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                {
                    if (encodedException.contains(expectedErrorMessage))
                    {
                        foundError.set(true);
                    }
                });
            return foundError.get();
        });
    }
}
