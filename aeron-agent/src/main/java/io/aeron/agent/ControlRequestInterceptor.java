/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.agent;

import io.aeron.logbuffer.Header;
import net.bytebuddy.asm.Advice;
import org.agrona.DirectBuffer;

import static io.aeron.agent.ArchiveEventLogger.LOGGER;

/**
 * Intercepts requests to the archive.
 */
final class ControlRequestInterceptor
{
    static class ControlRequest
    {
        @Advice.OnMethodEnter
        static void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            LOGGER.logControlRequest(buffer, offset, length);
        }
    }
}