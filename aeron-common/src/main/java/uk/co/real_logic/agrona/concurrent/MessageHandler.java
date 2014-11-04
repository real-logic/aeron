/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.agrona.concurrent;

import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Callback interface for processing of messages that are read from a buffer.
 */
public interface MessageHandler
{
    /**
     * Called for the processing of each message read from a buffer in turn.
     *
     * @param msgTypeId type of the encoded message.
     * @param buffer containing the encoded message.
     * @param index at which the encoded message begins.
     * @param length in bytes of the encoded message.
     */
    void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length);
}
