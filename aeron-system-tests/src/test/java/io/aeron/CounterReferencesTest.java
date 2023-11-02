/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.driver.status.PublisherPos.PUBLISHER_POS_TYPE_ID;
import static io.aeron.driver.status.ReceiverHwm.RECEIVER_HWM_TYPE_ID;
import static io.aeron.driver.status.ReceiverPos.RECEIVER_POS_TYPE_ID;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith(InterruptingTestCallback.class)
class CounterReferencesTest
{
    private static final String UDP_CHANNEL = "aeron:udp?endpoint=localhost:24325";
    private static final int STREAM_ID = 7000;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;
    private CountersReader countersReader;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_MIN_LENGTH)
            .ipcTermBufferLength(TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Tests::onError);

        driver = TestMediaDriver.launch(driverCtx, testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context());
        countersReader = aeron.countersReader();
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldLinkUdpSubPosToTheUnderlyingImage()
    {
        try (
            Subscription subscription = aeron.addSubscription(UDP_CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(UDP_CHANNEL, STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            final int subPosId = subscription.imageAtIndex(0).subscriberPositionId();
            final long referenceId = countersReader.getCounterReferenceId(subPosId);

            final int rcvHwmId = countersReader.findByTypeIdAndRegistrationId(RECEIVER_HWM_TYPE_ID, referenceId);
            assertNotEquals(NULL_COUNTER_ID, rcvHwmId);

            final int rcvPosId = countersReader.findByTypeIdAndRegistrationId(RECEIVER_POS_TYPE_ID, referenceId);
            assertNotEquals(NULL_COUNTER_ID, rcvPosId);

            try (Subscription anotherSubscription = aeron.addSubscription(UDP_CHANNEL, STREAM_ID))
            {
                Tests.awaitConnected(anotherSubscription);

                final int anotherSubPosId = anotherSubscription.imageAtIndex(0).subscriberPositionId();
                assertEquals(referenceId, countersReader.getCounterReferenceId(anotherSubPosId));
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldLinkIpcSubPosToTheUnderlyingPublication()
    {
        try (
            Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(IPC_CHANNEL, STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            final int subPosId = subscription.imageAtIndex(0).subscriberPositionId();
            final long referenceId = countersReader.getCounterReferenceId(subPosId);

            assertEquals(publication.registrationId(), referenceId);

            final int pubPosId = countersReader.findByTypeIdAndRegistrationId(PUBLISHER_POS_TYPE_ID, referenceId);
            assertNotEquals(NULL_COUNTER_ID, pubPosId);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldLinkSpySubPosToTheUnderlyingPublication()
    {
        try (
            Subscription subscription = aeron.addSubscription(SPY_PREFIX + UDP_CHANNEL, STREAM_ID);
            Publication publication = aeron.addPublication(UDP_CHANNEL, STREAM_ID))
        {
            Tests.awaitConnected(subscription);

            final int subPosId = subscription.imageAtIndex(0).subscriberPositionId();
            final long referenceId = countersReader.getCounterReferenceId(subPosId);

            assertEquals(publication.registrationId(), referenceId);

            final int pubPosId = countersReader.findByTypeIdAndRegistrationId(PUBLISHER_POS_TYPE_ID, referenceId);
            assertNotEquals(NULL_COUNTER_ID, pubPosId);
        }
    }
}
