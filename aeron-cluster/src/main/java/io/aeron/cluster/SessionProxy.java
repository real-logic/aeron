/*
 * Copyright 2014-2017 Real Logic Ltd.
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

import static io.aeron.cluster.ClusterSession.State.*;

/**
 * Proxy for a session for authenication purposes. Used to inform system of client authentication status.
 */
public class SessionProxy
{
    private final EgressPublisher egressPublisher;
    private ClusterSession clusterSession;

    public SessionProxy(final EgressPublisher egressPublisher)
    {
        this.egressPublisher = egressPublisher;
    }

    public final void clusterSession(final ClusterSession clusterSession)
    {
        if (this.clusterSession != clusterSession)
        {
            this.clusterSession = clusterSession;
        }
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
     * Inform the system that the session requires a challenge and to send the provided data in the challenge.
     *
     * @param challengeData to send in the challenge to the client.
     * @return true if challenge was sent or false if challenge could not be sent.
     */
    public final boolean challenge(final byte[] challengeData)
    {
        if (egressPublisher.sendChallenge(
            clusterSession, clusterSession.lastCorrelationId(), clusterSession.id(), challengeData))
        {
            clusterSession.state(CHALLENGED);
            return true;
        }

        return false;
    }

    /**
     * Inform the system that the session is met authentication requirements and can continue.
     *
     * @return true if success event was sent or false if success event could not be sent.
     */
    public final boolean authenticate()
    {
        if (egressPublisher.sendEvent(clusterSession, EventCode.OK, ""))
        {
            clusterSession.state(AUTHENTICATED);
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
