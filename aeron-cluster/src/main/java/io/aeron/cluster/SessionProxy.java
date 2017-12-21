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

public class SessionProxy
{
    private EgressPublisher egressPublisher;
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

    public final long sessionId()
    {
        return clusterSession.id();
    }

    // send challenge
    public final void challenge(final byte[] challengeData)
    {
        if (egressPublisher.sendChallenge(
            clusterSession, clusterSession.lastCorrelationId(), clusterSession.id(), challengeData))
        {
            clusterSession.state(CHALLENGED);
        }
    }

    // authenticate session
    public final void authenticate()
    {
        if (egressPublisher.sendEvent(clusterSession, EventCode.OK, ""))
        {
            clusterSession.state(AUTHENTICATED);
        }
    }

    // reject session
    public final void reject()
    {
        clusterSession.state(REJECTED);
    }
}
