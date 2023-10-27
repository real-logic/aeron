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
import io.aeron.exceptions.RegistrationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(InterruptingTestCallback.class)
public class WildcardPortManagerSystemTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldAllocatePortsInTheGivenRanges()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(64 * 1024)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .receiverWildcardPortRange("20700 20701")
            .senderWildcardPortRange("20702 20702");

        final Aeron.Context aeronCtx = new Aeron.Context()
            .aeronDirectoryName(driverCtx.aeronDirectoryName());

        driver = TestMediaDriver.launch(driverCtx, testWatcher);
        aeron = Aeron.connect(aeronCtx);

        final String subscriptionChannel = "aeron:udp?endpoint=127.0.0.1:0";

        final Subscription subscription1 = aeron.addSubscription(subscriptionChannel, 1);
        final String endpoint1 = awaitResolvedEndpoint(subscription1);
        assertEquals("127.0.0.1:20700", endpoint1);

        final Subscription subscription2 = aeron.addSubscription(subscriptionChannel, 2);
        final String endpoint2 = awaitResolvedEndpoint(subscription2);
        assertEquals("127.0.0.1:20701", endpoint2);

        final RegistrationException registrationException1 = assertThrows(
            RegistrationException.class,
            () -> aeron.addSubscription(subscriptionChannel, 3));
        assertThat(registrationException1.getMessage(), containsString("no available ports in range 20700 20701"));

        subscription2.close();

        final Subscription subscription3 = await(0, () -> aeron.addSubscription(subscriptionChannel, 4));
        final String endpoint3 = awaitResolvedEndpoint(subscription3);
        assertEquals("127.0.0.1:20701", endpoint3);

        final String publicationChannel = "aeron:udp?control=127.0.0.1:0|control-mode=dynamic|linger=0";

        final Publication publication1 = aeron.addPublication(publicationChannel, 5);
        final String controlEndpoint1 = awaitResolvedEndpoint(publication1);
        assertEquals("127.0.0.1:20702", controlEndpoint1);

        final RegistrationException registrationException2 = assertThrows(
            RegistrationException.class,
            () -> aeron.addPublication(publicationChannel, 6));
        assertThat(registrationException2.getMessage(), containsString("no available ports in range 20702 20702"));

        publication1.close();

        final Publication publication2 = await(200, () -> aeron.addPublication(publicationChannel, 7));
        final String controlEndpoint2 = awaitResolvedEndpoint(publication2);
        assertEquals("127.0.0.1:20702", controlEndpoint2);
    }

    private <T> T await(final long initialDelayMs, final Supplier<T> supplier)
    {
        if (initialDelayMs > 0)
        {
            Tests.sleep(initialDelayMs);
        }

        final List<Exception> exceptions = new ArrayList<>();

        while (true)
        {
            try
            {
                return supplier.get();
            }
            catch (final Exception e)
            {
                exceptions.add(e);
            }

            try
            {
                Thread.sleep(100);
            }
            catch (final InterruptedException e)
            {
                Thread.currentThread().interrupt();

                final TimeoutException timeoutException = new TimeoutException("Timed out awaiting result");
                exceptions.forEach(timeoutException::addSuppressed);
                throw timeoutException;
            }
        }
    }

    private String awaitResolvedEndpoint(final Subscription subscription)
    {
        String endpoint;

        while ((endpoint = subscription.resolvedEndpoint()) == null)
        {
            if (subscription.channelStatus() == ChannelEndpointStatus.ERRORED)
            {
                throw new AssertionError("Channel endpoint error");
            }

            Tests.yieldingIdle("Could not resolve endpoint");
        }

        return endpoint;
    }

    private String awaitResolvedEndpoint(final Publication publication)
    {
        List<String> localSocketAddresses;

        while ((localSocketAddresses = publication.localSocketAddresses()).isEmpty())
        {
            Tests.yieldingIdle("Could not resolve endpoint");
        }

        return localSocketAddresses.get(0);
    }
}
