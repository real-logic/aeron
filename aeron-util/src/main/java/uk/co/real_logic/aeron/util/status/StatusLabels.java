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
package uk.co.real_logic.aeron.util.status;

/**
 * A enumeration of common labels to be used by status counters.
 */
public enum  StatusLabels
{
    PRODUCER_TO_SOURCE_POSITION("Producer to Source Position"),
    RECEIVER_TO_CONSUMER_POSITION("Producer to Source Position")
    ;

    // TODO: get label from

    private final String label;

    private StatusLabels(final String label)
    {
        this.label = label;
    }

}
