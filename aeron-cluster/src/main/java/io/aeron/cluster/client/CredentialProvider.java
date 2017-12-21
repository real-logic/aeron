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
package io.aeron.cluster.client;

/**
 * Provider of credentials for authentication with a cluster leader.
 *
 * This is a base class that provides no credentials. Extend this class to provide credentials for clients.
 */
public class CredentialProvider
{
    public static final byte[] NULL_CREDENTIALS = new byte[0];

    /**
     * Provide credentials to be included in Session Connect message to the cluster.
     *
     * @return credentials in binary form to be included in the Session Connect message to the cluster.
     */
    public byte[] connectRequestCredentialData()
    {
        return NULL_CREDENTIALS;
    }

    /**
     * Given some challenge data, provide the credentials to be included in a Challenge Response as part of
     * authentication with a cluster.
     *
     * @param challengeData from the cluster to use in providing credentials.
     * @return credentials in binary form to be included in the Challenge Response to the cluster.
     */
    public byte[] onChallenge(final byte[] challengeData)
    {
        return NULL_CREDENTIALS;
    }
}
