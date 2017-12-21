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

/**
 * Default Authenticator that authenticates all connection requests immediately.
 */
public class DefaultAuthenticatorSupplier implements AuthenticatorSupplier
{
    public Authenticator newAuthenticator(final ConsensusModule.Context context)
    {
        return new Authenticator()
        {
            public void onConnectRequest(final long sessionId, final byte[] credentialData, final long nowMs)
            {
            }

            public void onChallengeResponse(final long sessionId, final byte[] credentialData, final long nowMs)
            {
            }

            public void onProcessConnectedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                sessionProxy.authenticate();
            }

            public void onProcessChallengedSession(final SessionProxy sessionProxy, final long nowMs)
            {
                sessionProxy.authenticate();
            }
        };
    }
}
