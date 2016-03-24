/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

import java.nio.channels.FileChannel;

/**
 * Function for handling a block of fragments from the log that are contained in the underlying file.
 */
@FunctionalInterface
public interface FileBlockHandler
{
    /**
     * Notification of an available block of fragments.
     *
     * @param fileChannel containing the block of fragments.
     * @param offset      at which the block begins, including any frame headers.
     * @param length      of the block in bytes, including any frame headers that is aligned up to
     *                    {@link io.aeron.logbuffer.FrameDescriptor#FRAME_ALIGNMENT}.
     * @param sessionId   of the stream of fragments.
     * @param termId      of the stream of fragments.
     */
    void onBlock(FileChannel fileChannel, long offset, int length, int sessionId, int termId);
}
