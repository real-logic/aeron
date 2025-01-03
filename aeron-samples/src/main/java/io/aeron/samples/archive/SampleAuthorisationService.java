/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.archive;

import io.aeron.security.AuthorisationService;
import org.agrona.collections.ObjectHashSet;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * Trivial authorisation service that allows a set of US-ASCII encoded principals access to all services.
 */
public class SampleAuthorisationService implements AuthorisationService
{
    private final ObjectHashSet<String> allowedPrincipals = new ObjectHashSet<>();

    /**
     * Create with a collection of principals that are allowed access to resources governed by this service.
     *
     * @param allowedPrincipals the collection of principals allow to access servivce
     */
    public SampleAuthorisationService(final Collection<String> allowedPrincipals)
    {
        this.allowedPrincipals.addAll(allowedPrincipals);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isAuthorised(
        final int protocolId,
        final int actionId,
        final Object type,
        final byte[] encodedPrincipal)
    {
        final String principal = new String(encodedPrincipal, StandardCharsets.US_ASCII);
        return allowedPrincipals.contains(principal);
    }
}
