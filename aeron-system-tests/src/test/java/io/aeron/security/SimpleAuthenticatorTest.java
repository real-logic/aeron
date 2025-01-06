/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SimpleAuthenticatorTest
{
    @Test
    void shouldAuthenticate()
    {
        final SessionProxy mockSessionProxy = mock(SessionProxy.class);
        final long nowMs = 1_000_000L;
        final long sessionId = 982374;

        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final byte[] encodedCredentials = "user:pass".getBytes(US_ASCII);

        when(mockSessionProxy.sessionId()).thenReturn(sessionId);

        final SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator.Builder()
            .principal(encodedPrincipal, encodedCredentials)
            .newInstance();

        simpleAuthenticator.onConnectRequest(sessionId, encodedCredentials, nowMs);
        simpleAuthenticator.onConnectedSession(mockSessionProxy, nowMs);

        verify(mockSessionProxy).authenticate(encodedPrincipal);
    }

    @ParameterizedTest
    @ValueSource(strings = {"user:wrong", "wrong:pass"})
    void shouldReject(final String incorrectCredentials)
    {
        final SessionProxy mockSessionProxy = mock(SessionProxy.class);
        final long nowMs = 1_000_000L;
        final long sessionId = 982374;

        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final byte[] encodedCredentials = "user:pass".getBytes(US_ASCII);

        when(mockSessionProxy.sessionId()).thenReturn(sessionId);

        final SimpleAuthenticator simpleAuthenticator = new SimpleAuthenticator.Builder()
            .principal(encodedPrincipal, encodedCredentials)
            .newInstance();

        simpleAuthenticator.onConnectRequest(sessionId, incorrectCredentials.getBytes(US_ASCII), nowMs);
        simpleAuthenticator.onConnectedSession(mockSessionProxy, nowMs);
        verify(mockSessionProxy).reject();
    }

    @Test
    void shouldHandleMultipleConcurrentAuthenticationRequests()
    {
        final long nowMs = 9283479L;
        final String[][] users = {
            { "user1", "user1:pass1" },
            { "user2", "user2:pass2" },
            { "user3", "user3:pass3" },
            { "user4", "user4:pass4" },
            { "user5", "user5:pass5" },
        };
        final SessionProxy mockSessionProxy = mock(SessionProxy.class);

        final SimpleAuthenticator.Builder builder = new SimpleAuthenticator.Builder();

        for (final String[] user : users)
        {
            builder.principal(user[0].getBytes(US_ASCII), user[1].getBytes(US_ASCII));
        }

        final SimpleAuthenticator simpleAuthenticator = builder.newInstance();

        for (int i = 0; i < users.length; i++)
        {
            simpleAuthenticator.onConnectRequest(i + 1000, users[i][1].getBytes(US_ASCII), nowMs);
        }

        for (int i = 0; i < users.length; i++)
        {
            when(mockSessionProxy.sessionId()).thenReturn(i + 1000L);
            simpleAuthenticator.onConnectedSession(mockSessionProxy, nowMs);
            verify(mockSessionProxy).authenticate(users[i][0].getBytes(US_ASCII));
        }
    }
}