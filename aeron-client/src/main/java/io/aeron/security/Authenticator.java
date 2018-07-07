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
 * Interface for Authenticator to handle authentication of clients to a system.
 * <p>
 * The session-id refers to the authentication session and not the Aeron transport session assigned to a publication.
 */
public interface Authenticator
{
    /**
     * Called upon reception of a Connect Request.
     *
     * @param sessionId          to identify the client session connecting.
     * @param encodedCredentials from the Connect Request. Will not be null, but may be 0 length.
     * @param nowMs              current epoch time in milliseconds.
     */
    void onConnectRequest(long sessionId, byte[] encodedCredentials, long nowMs);

    /**
     * Called upon reception of a Challenge Response from an unauthenticated client.
     *
     * @param sessionId          to identify the client session connecting.
     * @param encodedCredentials from the Challenge Response. Will not be null, but may be 0 length.
     * @param nowMs              current epoch time in milliseconds.
     */
    void onChallengeResponse(long sessionId, byte[] encodedCredentials, long nowMs);

    /**
     * Called when a client's response channel has been connected. This method may be called multiple times until the
     * session is timeouts, is challenged, authenticated, or rejected.
     *
     * @param sessionProxy to use to inform client of status.
     * @param nowMs        current epoch time in milliseconds.
     * @see SessionProxy
     */
    void onConnectedSession(SessionProxy sessionProxy, long nowMs);

    /**
     * Called when a challenged client should be able to accept a response from the authenticator.
     * <p>
     * When this is called, there is no assumption that a Challenge Response has been received, plus this method
     * may be called multiple times.
     * <p>
     * It is up to the concrete class to provide any timeout management.
     *
     * @param sessionProxy to use to inform client of status.
     * @param nowMs        current epoch time in milliseconds.
     * @see SessionProxy
     */
    void onChallengedSession(SessionProxy sessionProxy, long nowMs);
}
