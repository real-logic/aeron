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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.test.Tests.awaitConnected;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RegistrationAndOwnerTest
{
    private static final int STREAM_ID = 1001;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @ParameterizedTest
    @ValueSource(strings = { "aeron:udp?endpoint=localhost:24325", "aeron:ipc" })
    void shouldHaveCorrectOwnershipOnEntities(final String channel)
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true);

        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(ctx, testWatcher))
        {
            testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

            try (
                Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
                Subscription subscription = aeron.addSubscription(channel, STREAM_ID);
                Publication publication = aeron.addPublication(channel, STREAM_ID);
                Counter userCounter = aeron.addCounter(1002, "Test Counter"))
            {
                testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

                awaitConnected(subscription);
                awaitConnected(publication);

                final CountersReader countersReader = aeron.countersReader();
                final int subscriberPositionId = subscription.imageAtIndex(0).subscriberPositionId();

                assertEquals(aeron.clientId(), countersReader.getCounterOwnerId(subscriberPositionId));
                assertEquals(aeron.clientId(), countersReader.getCounterOwnerId(publication.positionLimitId()));
                assertEquals(aeron.clientId(), countersReader.getCounterOwnerId(userCounter.id()));

                assertEquals(
                    subscription.registrationId(), countersReader.getCounterRegistrationId(subscriberPositionId));

                assertEquals(
                    publication.registrationId(),
                    countersReader.getCounterRegistrationId(publication.positionLimitId()));

                assertEquals(userCounter.registrationId(),
                    countersReader.getCounterRegistrationId(userCounter.id()));
            }
        }
    }
}
