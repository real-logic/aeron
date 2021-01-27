/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.*;
import io.aeron.test.driver.DistinctErrorLogTestWatcher;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_PARAM_NAME;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.*;

public class NameReResolutionTest
{
    private static final String ENDPOINT_NAME = "ReResTestEndpoint";
    private static final String ENDPOINT_WITH_ERROR_NAME = "ReResWithErrEndpoint";
    private static final String PUBLICATION_MANUAL_MDC_URI = "aeron:udp?control=localhost:24327|control-mode=manual";
    private static final String PUBLICATION_URI = "aeron:udp?endpoint=" + ENDPOINT_NAME;
    private static final String PUBLICATION_WITH_ERROR_URI = "aeron:udp?endpoint=" + ENDPOINT_WITH_ERROR_NAME;
    private static final String FIRST_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String SECOND_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24326";
    private static final String BAD_ADDRESS = "bad.invalid:24326";

    private static final String CONTROL_NAME = "ReResTestControl";
    private static final String FIRST_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=localhost:24327|control-mode=dynamic|linger=0";
    private static final String SECOND_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=localhost:24328|control-mode=dynamic";
    private static final String SUBSCRIPTION_DYNAMIC_MDC_URI =
        "aeron:udp?control=" + CONTROL_NAME + "|control-mode=dynamic";
    private static final String SUBSCRIPTION_MDS_URI = "aeron:udp?control-mode=manual";

    private static final String STUB_LOOKUP_CONFIGURATION =
        ENDPOINT_NAME + "," + ENDPOINT_PARAM_NAME + "," + "localhost:24326,localhost:24325|" +
        CONTROL_NAME + "," + MDC_CONTROL_PARAM_NAME + "," + "localhost:24328,localhost:24327|" +
        ENDPOINT_WITH_ERROR_NAME + "," + ENDPOINT_PARAM_NAME + "," + BAD_ADDRESS + ",localhost:24325|";

    private static final int STREAM_ID = 1001;

    private Aeron client;
    private TestMediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @RegisterExtension
    public final DistinctErrorLogTestWatcher logWatcher = new DistinctErrorLogTestWatcher();

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final FragmentHandler handler = mock(FragmentHandler.class);
    private CountersReader countersReader;

    @BeforeEach
    public void before()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);

        TestMediaDriver.enableCsvNameLookupConfiguration(context, STUB_LOOKUP_CONFIGURATION);

        driver = TestMediaDriver.launch(context, testWatcher);

        client = Aeron.connect();
        countersReader = client.countersReader();
    }

    @AfterEach
    public void after()
    {
        logWatcher.captureErrors(client.context().aeronDirectoryName());
        CloseHelper.closeAll(client, driver);
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldReResolveEndpointOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldReResolveMdcManualEndpointOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_MANUAL_MDC_URI, STREAM_ID);
        publication.addDestination(PUBLICATION_URI);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldReResolveMdcDynamicControlOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());
        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_DYNAMIC_MDC_URI, STREAM_ID);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on first subscription");
        }

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Tests.sleep(10);
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldReResolveMdcDynamicControlOnManualDestinationSubscriptionOnNotConnected()
    {
        final long initialResolutionChanges = countersReader.getCounterValue(RESOLUTION_CHANGES.id());

        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_MDS_URI, STREAM_ID);
        subscription.addDestination(SUBSCRIPTION_DYNAMIC_MDC_URI);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on first subscription");
        }

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Tests.sleep(10);
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connection to second subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to second subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on second subscription");
        }

        Tests.awaitCounterDelta(countersReader, RESOLUTION_CHANGES.id(), initialResolutionChanges, 1);

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @Timeout(20)
    public void shouldReportErrorOnReResolveFailure() throws IOException
    {
        buffer.putInt(0, 1);

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_WITH_ERROR_URI, STREAM_ID);
        final long initialErrorCount = client.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        while (!subscription.isConnected())
        {
            Tests.yieldingWait("No connect to first subscription");
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Tests.yieldingWait("No message offer to first subscription");
        }

        while (subscription.poll(handler, 1) <= 0)
        {
            Tests.yieldingWait("No message received on first subscription");
        }

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Tests.sleep(10);
        }

        Tests.awaitCounterDelta(
            client.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher = allOf(
            containsString("endpoint=" + ENDPOINT_WITH_ERROR_NAME),
            containsString("name-and-port=" + BAD_ADDRESS));

        SystemTests.waitForErrorToOccur(
            client.context().aeronDirectoryName(),
            exceptionMessageMatcher,
            new SleepingMillisIdleStrategy(100));
    }
}
