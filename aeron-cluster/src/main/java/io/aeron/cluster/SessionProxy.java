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

import io.aeron.cluster.codecs.EventCode;

import static io.aeron.cluster.ClusterSession.State.CHALLENGED;
import static io.aeron.cluster.ClusterSession.State.REJECTED;

/**
 * Proxy for a session for authentication purposes. Used to inform system of client authentication status.
 * <p>
 * <b>Note:</b> The object is not threadsafe.
 */
public class SessionProxy
{
    private final EgressPublisher egressPublisher;
    private ClusterSession clusterSession;
    private String memberEndpointsDetail;

    public SessionProxy(final EgressPublisher egressPublisher)
    {
        this.egressPublisher = egressPublisher;
    }

    public final SessionProxy session(final ClusterSession clusterSession)
    {
        this.clusterSession = clusterSession;
        return this;
    }

    public final SessionProxy memberEndpointsDetail(final String memberEndpointsDetail)
    {
        this.memberEndpointsDetail = memberEndpointsDetail;
        return this;
    }

    /**
     * The session Id of the potential session assigned by the consensus module.
     *
     * @return session id for the potential session
     */
    public final long sessionId()
    {
        return clusterSession.id();
    }

    /**
     * Inform the system that the session requires a challenge and to send the provided encoded challenge.
     *
     * @param encodedChallenge to send to the client.
     * @return true if challenge was sent or false if challenge could not be sent.
     */
    public final boolean challenge(final byte[] encodedChallenge)
    {
        if (egressPublisher.sendChallenge(clusterSession, encodedChallenge))
        {
            clusterSession.state(CHALLENGED);
            return true;
        }

        return false;
    }

    /**
     * Inform the system that the session is met authentication requirements and can continue as a full access client.
     *
     * @param encodedPrincipal to pass to the on session open cluster event.
     * @return true if success event was sent or false if success event could not be sent.
     */
    public final boolean authenticate(final byte[] encodedPrincipal)
    {
        return authenticate(encodedPrincipal, ClusterSession.Capability.CLIENT_AND_MEMBER);
    }

    /**
     * Inform the system that the session is met authentication requirements and can continue with the given capability.
     *
     * @param encodedPrincipal to pass to the on session open cluster event.
     * @param capability       for the client.
     * @return true if success event was sent or false if success event could not be sent.
     * @see ClusterSession.Capability
     */
    public final boolean authenticate(final byte[] encodedPrincipal, final ClusterSession.Capability capability)
    {
        ClusterSession.checkEncodedPrincipalLength(encodedPrincipal);

        if (egressPublisher.sendEvent(clusterSession, EventCode.OK, memberEndpointsDetail))
        {
            clusterSession.authenticate(encodedPrincipal, capability);
            return true;
        }

        return false;
    }

    /**
     * Inform the system that the session has NOT met authentication requirements and should be rejected.
     */
    public final void reject()
    {
        clusterSession.state(REJECTED);
    }
}
