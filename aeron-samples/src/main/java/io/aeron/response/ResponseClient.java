/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.response;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;

class ResponseClient
{
    private final Aeron aeron;
    private Publication publication;
    private Subscription subscription;

    ResponseClient(final Aeron aeron)
    {
        this.aeron = aeron;
    }

    int poll()
    {
        final String endpoint = "192.168.0.1:10000";

        int workCount = 0;

        if (null == subscription)
        {
            subscription = aeron.addSubscription("aeron:udp?is-response-channel=true", 10001);
        }

        if (null == publication)
        {
            publication = aeron.addPublication(
                "aeron:udp?endpoint=" + endpoint + "|" +
                "response-subscription=" + subscription.registrationId(),
                10001);

            workCount++;
        }

        return workCount;
    }
}
