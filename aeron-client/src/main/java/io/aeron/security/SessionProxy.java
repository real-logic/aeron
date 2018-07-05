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
package io.aeron.security;

/**
 * Representation for a session which is going through the authentication process.
 */
public interface SessionProxy
{
    /**
     * The session Id of the potential session assigned by the system.
     *
     * @return session id for the potential session
     */
    long sessionId();

    /**
     * Inform the system that the session requires a challenge and to send the provided encoded challenge.
     *
     * @param encodedChallenge to send to the client.
     * @return true if challenge was sent or false if challenge could not be sent.
     */
    boolean challenge(byte[] encodedChallenge);

    /**
     * Inform the system that the session has met authentication requirements.
     *
     * @param encodedPrincipal that has passed authentication.
     * @return true if success event was sent or false if success event could not be sent.
     */
    boolean authenticate(byte[] encodedPrincipal);

    /**
     * Inform the system that the session has NOT met authentication requirements and should be rejected.
     */
    void reject();
}
