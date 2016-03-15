/*
 * Copyright 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.stats;

import uk.co.real_logic.agrona.concurrent.CountersManager;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.concurrent.status.Position;
import uk.co.real_logic.agrona.concurrent.status.UnsafeBufferPosition;

public class PublisherLimit
{
    /**
     * Type id of a publisher limit counter.
     */
    public static final int PUBLISHER_LIMIT_TYPE_ID = 1;

    public static Position allocate(
        final CountersManager countersManager,
        final long registrationId,
        final int sessionId,
        final int streamId,
        final String channel)
    {
        final String label = "publisher limit: " + registrationId + ' ' + sessionId + ' ' + streamId + ' ' + channel;

        final int counterId = countersManager.allocate(
            label,
            PUBLISHER_LIMIT_TYPE_ID,
            (buffer) -> buffer.putLong(0, registrationId)
        );

        return new UnsafeBufferPosition((UnsafeBuffer)countersManager.valuesBuffer(), counterId, countersManager);
    }
}
