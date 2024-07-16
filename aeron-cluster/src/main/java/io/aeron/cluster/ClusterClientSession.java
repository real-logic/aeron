/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Publication;

/**
 * Representation of a client session to an Aeron Cluster for use in an {@link ConsensusModuleExtension}.
 * @see ConsensusModuleControl#getClientSession(long)
 * @see ConsensusModuleControl#closeClusterSession(long)
 */
public interface ClusterClientSession
{
    /**
     * Cluster session identifier.
     *
     * @return cluster session identifier.
     */
    long id();

    /**
     * Determine if a cluster client session is open, so it is active for operations.
     *
     * @return true of the session is open otherwise false.
     */
    boolean isOpen();

    /**
     * Authenticated principal for a session encoded in byte form.
     *
     * @return authenticated principal for a session encoded in byte form.
     */
    byte[] encodedPrincipal();

    /**
     * Response {@link Publication} to be used for sending responses privately to a client.
     *
     * @return response {@link Publication} to be used for sending responses privately to a client.
     */
    Publication responsePublication();

    /**
     * The time last activity has been recorded for a session to determine if it is active.
     *
     * @return time last activity has been recorded for a session to determine if it is active.
     */
    long timeOfLastActivityNs();

    /**
     * The time last activity recorded for a session to determine if it is active. This should be updated on valid
     * ingress.
     *
     * @param timeNs of last activity recorded for a session to determine if it is active.
     */
    void timeOfLastActivityNs(long timeNs);
}
