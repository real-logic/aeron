/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.security.*;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.security.NullCredentialsSupplier.NULL_CREDENTIAL;
import static java.time.Duration.ofSeconds;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

public class AuthenticationTest
{
    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final String CREDENTIALS_STRING = "username=\"admin\"|password=\"secret\"";
    private static final String CHALLENGE_STRING = "I challenge you!";
    private static final String PRINCIPAL_STRING = "I am THE Principal!";

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    private final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    private AeronCluster aeronCluster;

    private final byte[] encodedCredentials = CREDENTIALS_STRING.getBytes();
    private final byte[] encodedChallenge = CHALLENGE_STRING.getBytes();

    @AfterEach
    public void after()
    {
        CloseHelper.close(aeronCluster);
        CloseHelper.close(container);
        CloseHelper.close(clusteredMediaDriver);

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        }
    }

    @Test
    public void shouldAuthenticateOnConnectRequestWithEmptyCredentials()
    {
        assertTimeout(ofSeconds(10), () ->
        {
            final AtomicLong serviceMsgCounter = new AtomicLong(0L);
            final MutableLong serviceSessionId = new MutableLong(-1L);
            final MutableLong authenticatorSessionId = new MutableLong(-1L);
            final MutableReference<byte[]> encodedPrincipal = new MutableReference<>();

            final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
            {
                public byte[] encodedCredentials()
                {
                    return NULL_CREDENTIAL;
                }

                public byte[] onChallenge(final byte[] encodedChallenge)
                {
                    fail();
                    return null;
                }
            });

            final Authenticator authenticator = spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertEquals(0, encodedCredentials.length);
                }

                public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    fail();
                }

                public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.authenticate(null);
                }

                public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

            launchClusteredMediaDriver(() -> authenticator);
            launchService(serviceSessionId, encodedPrincipal, serviceMsgCounter);

            connectClient(credentialsSupplier);
            sendCountedMessageIntoCluster(0);
            while (serviceMsgCounter.get() == 0)
            {
                Thread.yield();
                TestUtil.checkInterruptedStatus();
            }

            assertEquals(aeronCluster.clusterSessionId(), authenticatorSessionId.value);
            assertEquals(aeronCluster.clusterSessionId(), serviceSessionId.value);
            assertEquals(0, encodedPrincipal.get().length);
        });
    }

    @Test
    public void shouldAuthenticateOnConnectRequestWithCredentials()
    {
        assertTimeout(ofSeconds(10), () ->
        {
            final AtomicLong serviceMsgCounter = new AtomicLong(0L);
            final MutableLong serviceSessionId = new MutableLong(-1L);
            final MutableLong authenticatorSessionId = new MutableLong(-1L);
            final MutableReference<byte[]> encodedPrincipal = new MutableReference<>();

            final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
            {
                public byte[] encodedCredentials()
                {
                    return encodedCredentials;
                }

                public byte[] onChallenge(final byte[] encodedChallenge)
                {
                    fail();
                    return null;
                }
            });

            final Authenticator authenticator = spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
                }

                public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    fail();
                }

                public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.authenticate(PRINCIPAL_STRING.getBytes());
                }

                public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

            launchClusteredMediaDriver(() -> authenticator);
            launchService(serviceSessionId, encodedPrincipal, serviceMsgCounter);

            connectClient(credentialsSupplier);
            sendCountedMessageIntoCluster(0);
            while (serviceMsgCounter.get() == 0)
            {
                Thread.yield();
                TestUtil.checkInterruptedStatus();
            }

            assertEquals(aeronCluster.clusterSessionId(), authenticatorSessionId.value);
            assertEquals(aeronCluster.clusterSessionId(), serviceSessionId.value);
            assertEquals(PRINCIPAL_STRING, new String(encodedPrincipal.get()));
        });
    }

    @Test
    public void shouldAuthenticateOnChallengeResponse()
    {
        assertTimeout(ofSeconds(10), () ->
        {
            final AtomicLong serviceMsgCounter = new AtomicLong(0L);
            final MutableLong serviceSessionId = new MutableLong(-1L);
            final MutableLong authenticatorSessionId = new MutableLong(-1L);
            final MutableReference<byte[]> encodedPrincipal = new MutableReference<>();

            final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
            {
                public byte[] encodedCredentials()
                {
                    return NULL_CREDENTIAL;
                }

                public byte[] onChallenge(final byte[] encodedChallenge)
                {
                    assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                    return encodedCredentials;
                }
            });

            final Authenticator authenticator = spy(new Authenticator()
            {
                boolean challengeSuccessful = false;

                public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertEquals(0, encodedCredentials.length);
                }

                public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    assertEquals(sessionId, authenticatorSessionId.value);
                    assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
                    challengeSuccessful = true;
                }

                public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.challenge(encodedChallenge);
                }

                public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    if (challengeSuccessful)
                    {
                        assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                        sessionProxy.authenticate(PRINCIPAL_STRING.getBytes());
                    }
                }
            });

            launchClusteredMediaDriver(() -> authenticator);
            launchService(serviceSessionId, encodedPrincipal, serviceMsgCounter);

            connectClient(credentialsSupplier);
            sendCountedMessageIntoCluster(0);
            while (serviceMsgCounter.get() == 0)
            {
                Thread.yield();
                TestUtil.checkInterruptedStatus();
            }

            assertEquals(aeronCluster.clusterSessionId(), authenticatorSessionId.value);
            assertEquals(aeronCluster.clusterSessionId(), serviceSessionId.value);
            assertEquals(PRINCIPAL_STRING, new String(encodedPrincipal.get()));
        });
    }

    @Test
    public void shouldRejectOnConnectRequest()
    {
        assertTimeout(ofSeconds(10), () ->
        {
            final AtomicLong serviceMsgCounter = new AtomicLong(0L);
            final MutableLong serviceSessionId = new MutableLong(-1L);
            final MutableLong authenticatorSessionId = new MutableLong(-1L);
            final MutableReference<byte[]> encodedPrincipal = new MutableReference<>();

            final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
            {
                public byte[] encodedCredentials()
                {
                    return NULL_CREDENTIAL;
                }

                public byte[] onChallenge(final byte[] encodedChallenge)
                {
                    assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                    return encodedCredentials;
                }
            });

            final Authenticator authenticator = spy(new Authenticator()
            {
                public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertEquals(0, encodedCredentials.length);
                }

                public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    fail();
                }

                public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.reject();
                }

                public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    fail();
                }
            });

            launchClusteredMediaDriver(() -> authenticator);
            launchService(serviceSessionId, encodedPrincipal, serviceMsgCounter);

            try
            {
                connectClient(credentialsSupplier);
            }
            catch (final AuthenticationException ex)
            {
                assertEquals(-1L, serviceSessionId.value);
                return;
            }

            fail("should have seen exception");
        });
    }

    @Test
    public void shouldRejectOnChallengeResponse()
    {
        assertTimeout(ofSeconds(10), () ->
        {
            final AtomicLong serviceMsgCounter = new AtomicLong(0L);
            final MutableLong serviceSessionId = new MutableLong(-1L);
            final MutableLong authenticatorSessionId = new MutableLong(-1L);
            final MutableReference<byte[]> encodedPrincipal = new MutableReference<>();

            final CredentialsSupplier credentialsSupplier = spy(new CredentialsSupplier()
            {
                public byte[] encodedCredentials()
                {
                    return NULL_CREDENTIAL;
                }

                public byte[] onChallenge(final byte[] encodedChallenge)
                {
                    assertEquals(CHALLENGE_STRING, new String(encodedChallenge));
                    return encodedCredentials;
                }
            });

            final Authenticator authenticator = spy(new Authenticator()
            {
                boolean challengeRespondedTo = false;

                public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    authenticatorSessionId.value = sessionId;
                    assertEquals(0, encodedCredentials.length);
                }

                public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
                {
                    assertEquals(sessionId, authenticatorSessionId.value);
                    assertEquals(CREDENTIALS_STRING, new String(encodedCredentials));
                    challengeRespondedTo = true;
                }

                public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                    sessionProxy.challenge(encodedChallenge);
                }

                public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
                {
                    if (challengeRespondedTo)
                    {
                        assertEquals(sessionProxy.sessionId(), authenticatorSessionId.value);
                        sessionProxy.reject();
                    }
                }
            });

            launchClusteredMediaDriver(() -> authenticator);
            launchService(serviceSessionId, encodedPrincipal, serviceMsgCounter);

            try
            {
                connectClient(credentialsSupplier);
            }
            catch (final AuthenticationException ex)
            {
                assertEquals(-1L, serviceSessionId.value);
                return;
            }

            fail("should have seen exception");
        });
    }

    private void sendCountedMessageIntoCluster(final int value)
    {
        msgBuffer.putInt(0, value);

        while (aeronCluster.offer(msgBuffer, 0, SIZE_OF_INT) < 0)
        {
            Thread.yield();
            TestUtil.checkInterruptedStatus();
        }
    }

    private void launchService(
        final MutableLong sessionId, final MutableReference<byte[]> encodedPrincipal, final AtomicLong msgCounter)
    {
        final ClusteredService service = new StubClusteredService()
        {
            private int counterValue = 0;

            public void onSessionOpen(final ClientSession session, final long timestamp)
            {
                sessionId.value = session.id();
                encodedPrincipal.set(session.encodedPrincipal());
            }

            public void onSessionMessage(
                final ClientSession session,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                assertEquals(counterValue, buffer.getInt(offset));
                msgCounter.getAndIncrement();
                counterValue++;
            }
        };

        container = null;

        container = ClusteredServiceContainer.launch(
            new ClusteredServiceContainer.Context()
                .clusteredService(service)
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .errorHandler(TestUtil.errorHandler(0)));
    }

    private AeronCluster connectToCluster(final CredentialsSupplier credentialsSupplier)
    {
        return AeronCluster.connect(
            new AeronCluster.Context()
                .credentialsSupplier(credentialsSupplier));
    }

    private void connectClient(final CredentialsSupplier credentialsSupplier)
    {
        aeronCluster = null;
        aeronCluster = connectToCluster(credentialsSupplier);
    }

    private void launchClusteredMediaDriver(final AuthenticatorSupplier authenticatorSupplier)
    {
        clusteredMediaDriver = null;

        clusteredMediaDriver = ClusteredMediaDriver.launch(
            new MediaDriver.Context()
                .warnIfDirectoryExists(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(TestUtil.errorHandler(0))
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .recordingEventsEnabled(false)
                .deleteArchiveOnStart(true),
            new ConsensusModule.Context()
                .errorHandler(Throwable::printStackTrace)
                .authenticatorSupplier(authenticatorSupplier)
                .terminationHook(TestUtil.TERMINATION_HOOK)
                .deleteDirOnStart(true));
    }
}
