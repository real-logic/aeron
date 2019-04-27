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

/**
 * Identifies an event that can be enabled for logging.
 */
public interface EventCode
{
    /**
     * Returns the unique event identifier withing an {@link EventCodeType}.
     *
     * @return the unique event identifier withing an {@link EventCodeType}.
     */
    int id();

    /**
     * Get the event code's tag bit. Each tag bit is a unique identifier for the event code used
     * when checking that the event code is enabled or not. Each EventCode has a unique tag bit within the long word.
     *
     * @return the value with one bit set for the tag.
     */
    long tagBit();

    /**
     * Get the type of event code for classification.
     *
     * @return the type of event code for classification.
     */
    EventCodeType eventCodeType();
}
