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

/**
 * Representation of a session during the authentication process from the perspective of an {@link Authenticator}.
 *
 * @see Authenticator
 */
public interface SessionProxy
{
    /**
     * The identity of the potential session assigned by the system.
     *
     * @return identity for the potential session.
     */
    long sessionId();

    /**
     * Inform the system that the session requires a challenge by sending the provided encoded challenge.
     *
     * @param encodedChallenge to be sent to the client.
     * @return true if challenge was accepted to be sent at present time or false if it will be retried later.
     */
    boolean challenge(byte[] encodedChallenge);

    /**
     * Inform the system that the session has met authentication requirements.
     *
     * @param encodedPrincipal that has passed authentication.
     * @return true if authentication was accepted at present time or false if it will be retried later.
     */
    boolean authenticate(byte[] encodedPrincipal);

    /**
     * Inform the system that the session should be rejected.
     */
    void reject();
}
