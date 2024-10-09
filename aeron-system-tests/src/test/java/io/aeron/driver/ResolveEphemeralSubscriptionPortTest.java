/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class ResolveEphemeralSubscriptionPortTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.toAbsolutePath().toString())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.DEDICATED);
        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = { "|alias=test", "|session-id=99" })
    @InterruptAfter(10)
    void test(final String additionalUriParameters)
    {
        final int streamId = -1142;
        final String tags = "tags=" + aeron.nextCorrelationId() + "," + aeron.nextCorrelationId();

        final Subscription wildcardSubscription = aeron.addSubscription(
            "aeron:udp?endpoint=localhost:0|" + tags, streamId);
        Tests.await(() -> null != wildcardSubscription.resolvedEndpoint());
        final String resolvedEndpoint = wildcardSubscription.resolvedEndpoint();

        final long publicationId = aeron.asyncAddExclusivePublication(
            "aeron:udp?term-length=64k|endpoint=" + resolvedEndpoint + additionalUriParameters, streamId);

        final long subscriptionId = aeron.asyncAddSubscription("aeron:udp?" + tags + additionalUriParameters, streamId);
        wildcardSubscription.close();

        ExclusivePublication publication;
        while (null == (publication = aeron.getExclusivePublication(publicationId)))
        {
            Tests.yield();
        }

        Subscription subscription;
        while (null == (subscription = aeron.getSubscription(subscriptionId)))
        {
            Tests.yield();
        }

        Tests.awaitConnected(publication);
        Tests.awaitConnected(subscription);
    }
}
