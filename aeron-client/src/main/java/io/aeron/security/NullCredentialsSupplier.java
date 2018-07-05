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

import org.agrona.collections.ArrayUtil;

/**
 * Null provider of credentials when no authentication is required.
 */
public class NullCredentialsSupplier implements CredentialsSupplier
{
    public static final byte[] NULL_CREDENTIAL = ArrayUtil.EMPTY_BYTE_ARRAY;

    public byte[] encodedCredentials()
    {
        return NULL_CREDENTIAL;
    }

    public byte[] onChallenge(final byte[] encodedChallenge)
    {
        return NULL_CREDENTIAL;
    }
}
