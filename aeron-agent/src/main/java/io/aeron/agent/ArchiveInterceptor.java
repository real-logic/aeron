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
package io.aeron.agent;

import net.bytebuddy.asm.Advice;

import static io.aeron.agent.ArchiveEventCode.CONTROL_SESSION_STATE_CHANGE;
import static io.aeron.agent.ArchiveEventCode.REPLICATION_SESSION_STATE_CHANGE;
import static io.aeron.agent.ArchiveEventLogger.LOGGER;

/**
 * Intercepts calls in the archive which relate to state changes.
 */
class ArchiveInterceptor
{
    static class ReplicationSessionStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(final E oldState, final E newState, final long replicationId)
        {
            LOGGER.logSessionStateChange(REPLICATION_SESSION_STATE_CHANGE, oldState, newState, replicationId);
        }
    }

    static class ControlSessionStateChange
    {
        @Advice.OnMethodEnter
        static <E extends Enum<E>> void stateChange(final E oldState, final E newState, final long controlSessionId)
        {
            LOGGER.logSessionStateChange(CONTROL_SESSION_STATE_CHANGE, oldState, newState, controlSessionId);
        }
    }

    static class ReplaySession
    {
        @Advice.OnMethodEnter
        static void onPendingError(final long sessionId, final long recordingId, final String errorMessage)
        {
            LOGGER.logReplaySessionError(sessionId, recordingId, errorMessage);
        }
    }

    static class Catalog
    {
        @Advice.OnMethodEnter
        static void catalogResized(final long catalogLength, final long newCatalogLength)
        {
            LOGGER.logCatalogResize(catalogLength, newCatalogLength);
        }
    }
}
