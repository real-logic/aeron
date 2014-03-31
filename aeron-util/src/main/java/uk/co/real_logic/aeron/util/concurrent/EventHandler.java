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
package uk.co.real_logic.aeron.util.concurrent;

/**
 * Callback interface for processing of events that are read from a buffer.
 */
public interface EventHandler
{
    /**
     * Called for the processing of each event read from a buffer in turn.
     *
     * @param eventTypeId type of the encoded event.
     * @param buffer containing the encoded event.
     * @param index at which the encoded event begins.
     * @param length in bytes of the encoded event.
     */
    void onEvent(final int eventTypeId, final AtomicBuffer buffer, final int index, final int length);
}
