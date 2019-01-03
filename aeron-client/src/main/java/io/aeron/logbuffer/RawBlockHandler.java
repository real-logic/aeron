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

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.channels.FileChannel;

/**
 * Function for handling a raw block of fragments from the log that are contained in the underlying file.
 */
@FunctionalInterface
public interface RawBlockHandler
{
    /**
     * Notification of an available block of fragments.
     *
     * @param fileChannel containing the block of fragments.
     * @param fileOffset  at which the block begins, including any frame headers.
     * @param termBuffer  mapped over the block of fragments.
     * @param termOffset  in the termBuffer at which block begins, including any frame headers.
     * @param length      of the block in bytes, including any frame headers that is aligned up to
     *                    {@link io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT}.
     * @param sessionId   of the stream of fragments.
     * @param termId      of the stream of fragments.
     */
    void onBlock(
        FileChannel fileChannel,
        long fileOffset,
        UnsafeBuffer termBuffer,
        int termOffset,
        int length,
        int sessionId,
        int termId);
}
