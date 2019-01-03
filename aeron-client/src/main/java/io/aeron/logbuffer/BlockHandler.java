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
package io.aeron.logbuffer;

import org.agrona.DirectBuffer;

/**
 * Function for handling a block of message fragments scanned from the log.
 */
@FunctionalInterface
public interface BlockHandler
{
    /**
     * Callback for handling a block of messages being read from a log.
     *
     * @param buffer    containing the block of message fragments.
     * @param offset    at which the block begins, including any frame headers.
     * @param length    of the block in bytes, including any frame headers that is aligned up to
     *                  {@link io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT}.
     * @param sessionId of the stream containing this block of message fragments.
     * @param termId    of the stream containing this block of message fragments.
     */
    void onBlock(DirectBuffer buffer, int offset, int length, int sessionId, int termId);
}
