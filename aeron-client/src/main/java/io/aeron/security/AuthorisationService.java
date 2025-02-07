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
package io.aeron.security;

/**
 * Interface for an authorisation service to handle authorisation checks on clients performing actions to a system.
 *
 * @see AuthorisationServiceSupplier
 */
@FunctionalInterface
public interface AuthorisationService
{
    /**
     * An {@link AuthorisationService} instance that allows every action.
     */
    AuthorisationService ALLOW_ALL = (protocolId, actionId, type, encodedPrincipal) -> true;

    /**
     * An {@link AuthorisationService} instance that forbids all actions.
     */
    AuthorisationService DENY_ALL = (protocolId, actionId, type, encodedPrincipal) -> false;

    /**
     * Checks if the client with authenticated credentials is allowed to perform an action indicated by the
     * given {@code actionId}.
     *
     * @param protocolId       of the protocol to which the action belongs, e.g. a SBE schema id.
     * @param actionId         of the command being checked, e.g. a SBE message template id.
     * @param type             optional type for the action being checked, may be {@code null}. For example for
     *                         an admin request in the cluster it will contain {@code AdminRequestType} value which
     *                         denotes the exact kind of the request.
     * @param encodedPrincipal that has been authenticated.
     * @return {@code true} if the client is authorised to execute the action or {@code false} otherwise.
     */
    boolean isAuthorised(int protocolId, int actionId, Object type, byte[] encodedPrincipal);
}
