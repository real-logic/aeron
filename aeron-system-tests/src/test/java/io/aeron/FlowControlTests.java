/*
 * Copyright 2014-2020 Real Logic Ltd.
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
package io.aeron;

import io.aeron.test.Tests;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;

public class FlowControlTests
{
    public static void awaitConnectionAndStatusMessages(
        final CountersReader countersReader,
        final Subscription subscription,
        final Subscription... subscriptions)
    {
        while (!subscription.isConnected())
        {
            Tests.sleep(1);
        }

        for (final Subscription sub : subscriptions)
        {
            while (!sub.isConnected())
            {
                Tests.sleep(1);
            }
        }

        final long delta = 1 + subscriptions.length;
        Tests.awaitCounterDelta(countersReader, STATUS_MESSAGES_RECEIVED.id(), delta);
    }
}
