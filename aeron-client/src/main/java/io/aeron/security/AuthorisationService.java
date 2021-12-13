/*
 * Copyright 2014-2021 Real Logic Limited.
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

/**
 * Interface for an authorisation service to handle authorisation checks of clients in a system.
 *
 * @see AuthorisationServiceSupplier
 */
@FunctionalInterface
public interface AuthorisationService
{
    /**
     * An {@link AuthorisationService} instance that allows every command.
     */
    AuthorisationService ALLOW_ALL = (templateId, type, encodedCredentials) -> true;

    /**
     * An {@link AuthorisationService} instance that forbids all commands.
     */
    AuthorisationService DENY_ALL = (templateId, type, encodedCredentials) -> false;

    /**
     * Checks if the client with the specified credentials is allowed to perform an operation indicated by the
     * given {@code templateId}.
     *
     * @param templateId         of the command being checked, i.e. an SBE message id.
     * @param type               optional type for the command being checked, may be {@code null}. For example for
     *                           an admin request in the cluster it will contain {@code AdminRequestType} value which
     *                           denotes the exact kind of the request.
     * @param encodedCredentials from the Challenge Response. Will not be {@code null}, but may be 0 length.
     * @return {@code true} if the client is authorised to execute the command or {@code false} otherwise.
     */
    boolean isAuthorised(int templateId, Object type, byte[] encodedCredentials);
}
