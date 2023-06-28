/*
 * Copyright 2014-2023 Real Logic Limited.
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

public class SimpleAuthenticator implements Authenticator
{
    public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {

    }

    public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
    {

    }

    public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
    {

    }

    public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
    {

    }

    public static class Builder
    {
        public Builder principal(final byte[] principal, final byte[] credentials)
        {
            return this;
        }

        public SimpleAuthenticator newInstance()
        {
            return new SimpleAuthenticator();
        }
    }
}
