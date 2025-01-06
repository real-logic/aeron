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
package io.aeron.logbuffer;

import org.agrona.DirectBuffer;

/**
 * Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
 * or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
 */
@FunctionalInterface
public interface ControlledFragmentHandler
{
    /**
     * Action to be taken on return from {@link #onFragment(DirectBuffer, int, int, Header)}.
     */
    enum Action
    {
        /**
         * Abort the current polling operation and do not advance the position for this fragment.
         */
        ABORT,

        /**
         * Break from the current polling operation and commit the position as of the end of the current fragment
         * being handled.
         */
        BREAK,

        /**
         * Continue processing but commit the position as of the end of the current fragment so that
         * flow control is applied to this point.
         */
        COMMIT,

        /**
         * Continue processing until fragment limit or no fragments with position commit at end of poll as the in
         * {@link #onFragment(DirectBuffer, int, int, Header)}.
         */
        CONTINUE,
    }

    /**
     * Callback for handling fragments of data being read from a log.
     * <p>
     * Within this callback reentrant calls to the {@link io.aeron.Aeron} client are not permitted and
     * will result in undefined behaviour.
     *
     * @param buffer containing the data.
     * @param offset at which the data begins.
     * @param length of the data in bytes.
     * @param header representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    Action onFragment(DirectBuffer buffer, int offset, int length, Header header);
}
