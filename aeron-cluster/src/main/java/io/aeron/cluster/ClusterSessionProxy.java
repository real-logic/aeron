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
import io.aeron.security.SessionProxy;

import static io.aeron.cluster.ClusterSession.State.CHALLENGED;
import static io.aeron.cluster.ClusterSession.State.REJECTED;

/**
 * Proxy for a session being authenticated by an {@link io.aeron.security.Authenticator}.
 */
class ClusterSessionProxy implements SessionProxy
{
    private static final String EMPTY_DETAIL = "";
    private final EgressPublisher egressPublisher;
    private ClusterSession clusterSession;
    private int leaderMemberId;

    ClusterSessionProxy(final EgressPublisher egressPublisher)
    {
        this.egressPublisher = egressPublisher;
    }

    final SessionProxy session(final ClusterSession clusterSession)
    {
        this.clusterSession = clusterSession;
        return this;
    }

    final ClusterSessionProxy leaderMemberId(final int leaderMemberId)
    {
        this.leaderMemberId = leaderMemberId;
        return this;
    }

    public final long sessionId()
    {
        return clusterSession.id();
    }

    public final boolean challenge(final byte[] encodedChallenge)
    {
        if (egressPublisher.sendChallenge(clusterSession, encodedChallenge))
        {
            clusterSession.state(CHALLENGED);
            return true;
        }

        return false;
    }

    public final boolean authenticate(final byte[] encodedPrincipal)
    {
        ClusterSession.checkEncodedPrincipalLength(encodedPrincipal);

        if (egressPublisher.sendEvent(clusterSession, leaderMemberId, EventCode.OK, EMPTY_DETAIL))
        {
            clusterSession.authenticate(encodedPrincipal);
            return true;
        }

        return false;
    }

    public final void reject()
    {
        clusterSession.state(REJECTED);
    }
}
