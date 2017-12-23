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
package io.aeron.cluster;

import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.CredentialProvider;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.NoOpLock;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

public class AuthenticationTest
{
    private static final String CREDENTIAL_STRING = "username=\"admin\"|password=\"secret\"";
    private static final String CHALLENGE_STRING = "I challenge you!";

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private AeronCluster aeronCluster;
    private SessionDecorator sessionDecorator;
    private Publication publication;

    private final byte[] credentialData = CREDENTIAL_STRING.getBytes();
    private final byte[] challengeData = CHALLENGE_STRING.getBytes();

    @After
    public void after()
    {
        CloseHelper.close(container);
        CloseHelper.close(clusteredMediaDriver);

        container.context().deleteDirectory();
        clusteredMediaDriver.consensusModule().context().deleteDirectory();
        clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldAuthenticateOnConnectRequestWithEmptyCredentialData()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0L);
        final MutableLong serviceSessionId = new MutableLong(-1L);
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialProvider credentialProvider =
            spy(new CredentialProvider()
            {
                public byte[] connectRequestCredentialData()
                {
                    return CredentialProvider.NULL_CREDENTIALS;
                }

                public byte[] onChallenge(final byte[] challengeData)
                {
                    fail();
                    return null;
                }
            });

        final Authenticator authenticator =
            spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertThat(credentialData.length, is(0));
                }

                public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    fail();
                }

                public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                    sessionProxy.authenticate();
                }

                public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

        launchClusteredMediaDriver((ctx) -> authenticator);
        launchService(serviceSessionId, serviceMsgCounter);

        connectClient(credentialProvider);
        sendCountedMessageIntoCluster(0);
        while (serviceMsgCounter.get() == 0)
        {
            Thread.yield();
        }

        assertThat(authenticatorSessionId.value, is(aeronCluster.sessionId()));
        assertThat(serviceSessionId.value, is(aeronCluster.sessionId()));

        aeronCluster.close();
    }

    @Test(timeout = 10_000)
    public void shouldAuthenticateOnConnectRequestWithCredentialData()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0L);
        final MutableLong serviceSessionId = new MutableLong(-1L);
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialProvider credentialProvider =
            spy(new CredentialProvider()
            {
                public byte[] connectRequestCredentialData()
                {
                    return credentialData;
                }

                public byte[] onChallenge(final byte[] challengeData)
                {
                    fail();
                    return null;
                }
            });

        final Authenticator authenticator =
            spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertThat(new String(credentialData), is(CREDENTIAL_STRING));
                }

                public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    fail();
                }

                public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                    sessionProxy.authenticate();
                }

                public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

        launchClusteredMediaDriver((ctx) -> authenticator);
        launchService(serviceSessionId, serviceMsgCounter);

        connectClient(credentialProvider);
        sendCountedMessageIntoCluster(0);
        while (serviceMsgCounter.get() == 0)
        {
            Thread.yield();
        }

        assertThat(authenticatorSessionId.value, is(aeronCluster.sessionId()));
        assertThat(serviceSessionId.value, is(aeronCluster.sessionId()));

        aeronCluster.close();
    }

    @Test(timeout = 10_000)
    public void shouldAuthenticateOnChallengeResponse()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0L);
        final MutableLong serviceSessionId = new MutableLong(-1L);
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialProvider credentialProvider =
            spy(new CredentialProvider()
            {
                public byte[] connectRequestCredentialData()
                {
                    return CredentialProvider.NULL_CREDENTIALS;
                }

                public byte[] onChallenge(final byte[] challengeData)
                {
                    assertThat(new String(challengeData), is(CHALLENGE_STRING));
                    return credentialData;
                }
            });

        final Authenticator authenticator =
            spy(new Authenticator()
            {
                boolean challengeSuccessful = false;

                public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertThat(credentialData.length, is(0));
                }

                public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionId));
                    assertThat(new String(credentialData), is(CREDENTIAL_STRING));
                    challengeSuccessful = true;
                }

                public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                    sessionProxy.challenge(challengeData);
                }

                public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    if (challengeSuccessful)
                    {
                        assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                        sessionProxy.authenticate();
                    }
                }
            });

        launchClusteredMediaDriver((ctx) -> authenticator);
        launchService(serviceSessionId, serviceMsgCounter);

        connectClient(credentialProvider);
        sendCountedMessageIntoCluster(0);
        while (serviceMsgCounter.get() == 0)
        {
            Thread.yield();
        }

        assertThat(authenticatorSessionId.value, is(aeronCluster.sessionId()));
        assertThat(serviceSessionId.value, is(aeronCluster.sessionId()));

        aeronCluster.close();
    }

    @Test(timeout = 10_000)
    public void shouldRejectOnConnectRequest()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0L);
        final MutableLong serviceSessionId = new MutableLong(-1L);
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialProvider credentialProvider =
            spy(new CredentialProvider()
            {
                public byte[] connectRequestCredentialData()
                {
                    return CredentialProvider.NULL_CREDENTIALS;
                }

                public byte[] onChallenge(final byte[] challengeData)
                {
                    assertThat(new String(challengeData), is(CHALLENGE_STRING));
                    return credentialData;
                }
            });

        final Authenticator authenticator =
            spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertThat(credentialData.length, is(0));
                }

                public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    fail();
                }

                public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                    sessionProxy.reject();
                }

                public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

        launchClusteredMediaDriver((ctx) -> authenticator);
        launchService(serviceSessionId, serviceMsgCounter);

        try
        {
            connectClient(credentialProvider);
        }
        catch (final AuthenticationException ex)
        {
            assertThat(serviceSessionId.value, is(-1L));

            CloseHelper.close(aeronCluster);
            return;
        }

        fail("should have seen exception");
    }

    @Test(timeout = 10_000)
    public void shouldRejectOnChallengeResponse()
    {
        final AtomicLong serviceMsgCounter = new AtomicLong(0L);
        final MutableLong serviceSessionId = new MutableLong(-1L);
        final MutableLong authenticatorSessionId = new MutableLong(-1L);

        final CredentialProvider credentialProvider =
            spy(new CredentialProvider()
            {
                public byte[] connectRequestCredentialData()
                {
                    return CredentialProvider.NULL_CREDENTIALS;
                }

                public byte[] onChallenge(final byte[] challengeData)
                {
                    assertThat(new String(challengeData), is(CHALLENGE_STRING));
                    return credentialData;
                }
            });

        final Authenticator authenticator =
            spy(new Authenticator()
            {
                boolean challengeRespondedTo = false;

                public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertThat(credentialData.length, is(0));
                }

                public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionId));
                    assertThat(new String(credentialData), is(CREDENTIAL_STRING));
                    challengeRespondedTo = true;
                }

                public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                    sessionProxy.challenge(challengeData);
                }

                public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    if (challengeRespondedTo)
                    {
                        assertThat(authenticatorSessionId.value, is(sessionProxy.sessionId()));
                        sessionProxy.reject();
                    }
                }
            });

        launchClusteredMediaDriver((ctx) -> authenticator);
        launchService(serviceSessionId, serviceMsgCounter);

        try
        {
            connectClient(credentialProvider);
        }
        catch (final AuthenticationException ex)
        {
            assertThat(serviceSessionId.value, is(-1L));

            CloseHelper.close(aeronCluster);
            return;
        }

        fail("should have seen exception");
    }

    private void sendCountedMessageIntoCluster(final int value)
    {
        final long msgCorrelationId = aeronCluster.context().aeron().nextCorrelationId();

        msgBuffer.putInt(0, value);

        while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, SIZE_OF_INT) < 0)
        {
            Thread.yield();
        }
    }

    private void launchService(final MutableLong sessionId, final AtomicLong msgCounter)
    {
        final ClusteredService service =
            new StubClusteredService()
            {
                private int counterValue = 0;

                public void onSessionOpen(final ClientSession session, final long timestampMs)
                {
                    sessionId.value = session.id();
                }

                public void onSessionMessage(
                    final long clusterSessionId,
                    final long correlationId,
                    final long timestampMs,
                    final DirectBuffer buffer,
                    final int offset,
                    final int length,
                    final Header header)
                {
                    assertThat(buffer.getInt(offset), is(counterValue));
                    msgCounter.getAndIncrement();
                    counterValue++;
                }
            };

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(true));
    }

    private AeronCluster connectToCluster(final CredentialProvider credentialProvider)
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .credentialProvider(credentialProvider)
                .lock(new NoOpLock()));
    }

    private void connectClient(final CredentialProvider credentialProvider)
    {
        aeronCluster = connectToCluster(credentialProvider);
        sessionDecorator = new SessionDecorator(aeronCluster.sessionId());
        publication = aeronCluster.ingressPublication();
    }

    private void launchClusteredMediaDriver(final AuthenticatorSupplier authenticatorSupplier)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(true)
                .threadingMode(ThreadingMode.SHARED)
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .authenticatorSupplier(authenticatorSupplier));
    }
}
