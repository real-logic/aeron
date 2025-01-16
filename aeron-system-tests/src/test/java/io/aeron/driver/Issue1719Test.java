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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.test.EventLogExtension;
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
import org.junit.jupiter.params.provider.FieldSource;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class Issue1719Test
{
    private static List<String> channels = List.of(
        "aeron:udp?endpoint=localhost:5555",
        "aeron:udp?control=localhost:7777|control-mode=dynamic"
    );

    private TestMediaDriver driver;
    private Aeron aeron;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @FieldSource("channels")
    void shouldAddWildcardSpyBeforePublication(final String channel)
    {
        final String subUri = CommonContext.SPY_PREFIX + channel;
        final String pubUri = channel + "|ssc=true";
        final int streamId1 = 333;
        final int streamId2 = 777;

        final Subscription spyStream1 = aeron.addSubscription(subUri, streamId1);
        assertEquals(streamId1, spyStream1.streamId());
        final Subscription spyStream2 = aeron.addSubscription(subUri, streamId2);
        assertEquals(streamId2, spyStream2.streamId());

        final ExclusivePublication pubStream1 = aeron.addExclusivePublication(pubUri, streamId1);
        final ExclusivePublication pubStream2 = aeron.addExclusivePublication(pubUri, streamId2);
        assertNotEquals(pubStream1.sessionId(), pubStream2.sessionId());

        Tests.awaitConnected(pubStream1);
        Tests.awaitConnected(pubStream2);
        Tests.awaitConnected(spyStream1);
        Tests.awaitConnected(spyStream2);

        assertEquals(1, spyStream1.imageCount());
        assertEquals(pubStream1.sessionId(), spyStream1.imageAtIndex(0).sessionId());

        assertEquals(1, spyStream2.imageCount());
        assertEquals(pubStream2.sessionId(), spyStream2.imageAtIndex(0).sessionId());
    }

    @ParameterizedTest
    @FieldSource("channels")
    void shouldAddWildcardSpyAfterPublication(final String channel)
    {
        final String subUri = CommonContext.SPY_PREFIX + channel;
        final String pubUri = channel + "|ssc=true";
        final int streamId1 = 333;
        final int streamId2 = 777;

        final ExclusivePublication pubStream1 = aeron.addExclusivePublication(pubUri, streamId1);
        final ExclusivePublication pubStream2 = aeron.addExclusivePublication(pubUri, streamId2);
        assertNotEquals(pubStream1.sessionId(), pubStream2.sessionId());

        final Subscription spyStream1 = aeron.addSubscription(subUri, streamId1);
        assertEquals(streamId1, spyStream1.streamId());
        final Subscription spyStream2 = aeron.addSubscription(subUri, streamId2);
        assertEquals(streamId2, spyStream2.streamId());

        Tests.awaitConnected(pubStream1);
        Tests.awaitConnected(pubStream2);
        Tests.awaitConnected(spyStream1);
        Tests.awaitConnected(spyStream2);

        assertEquals(1, spyStream1.imageCount());
        assertEquals(pubStream1.sessionId(), spyStream1.imageAtIndex(0).sessionId());

        assertEquals(1, spyStream2.imageCount());
        assertEquals(pubStream2.sessionId(), spyStream2.imageAtIndex(0).sessionId());
    }
}
