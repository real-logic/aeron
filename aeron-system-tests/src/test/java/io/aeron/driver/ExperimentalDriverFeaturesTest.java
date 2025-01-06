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
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExperimentalDriverFeaturesTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
            .threadingMode(ThreadingMode.SHARED);
        context.enableExperimentalFeatures(false);

        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
        watcher.ignoreErrorsMatching(s -> true);
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietClose(driver);
    }

    @Test
    void shouldFailToCreateResponsePublicationIfExperimentalFeaturesAreDisabled()
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Exception ex1 = assertThrows(Exception.class, () -> aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=10", 10001));
            assertThat(ex1.getMessage(), containsString("experimental feature"));
            final Exception ex2 = assertThrows(Exception.class, () -> aeron.addPublication(
                "aeron:udp?endpoint=localhost:10000|control-mode=response", 10001));
            assertThat(ex2.getMessage(), containsString("experimental feature"));
        }
    }

    @Test
    void shouldFailToCreateResponseSubscriptionIfExperimentalFeaturesAreDisabled()
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Exception ex1 = assertThrows(Exception.class, () -> aeron.addSubscription(
                "aeron:udp?endpoint=localhost:10000|response-correlation-id=10", 10001));
            assertThat(ex1.getMessage(), containsString("experimental feature"));
            final Exception ex2 = assertThrows(Exception.class, () -> aeron.addSubscription(
                "aeron:udp?endpoint=localhost:10000|control-mode=response", 10001));
            assertThat(ex2.getMessage(), containsString("experimental feature"));
        }
    }
}
