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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class LifecycleTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    void shouldStartAndStopInstantly()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .errorHandler(Tests::onError);

        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(driverCtx, testWatcher))
        {
            testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

            final Aeron.Context clientCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            final Aeron aeron = Aeron.connect(clientCtx);
            aeron.close();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldNotifyOfClientTimestampCounter()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .errorHandler(Tests::onError);

        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(driverCtx, testWatcher))
        {
            testWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());

            final Aeron.Context clientCtxOne = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            final Aeron.Context clientCtxTwo = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            try (Aeron aeron = Aeron.connect(clientCtxOne))
            {
                final AvailableCounterHandler availableHandler = mock(AvailableCounterHandler.class);
                aeron.addAvailableCounterHandler(availableHandler);
                final UnavailableCounterHandler unavailableHandler = mock(UnavailableCounterHandler.class);
                aeron.addUnavailableCounterHandler(unavailableHandler);

                try (Aeron aeronTwo = Aeron.connect(clientCtxTwo))
                {
                    aeronTwo.addSubscription("aeron:ipc", 1001);
                    verify(availableHandler, timeout(5000))
                        .onAvailableCounter(any(), eq(clientCtxTwo.clientId()), anyInt());
                }

                verify(unavailableHandler, timeout(5000))
                    .onUnavailableCounter(any(), eq(clientCtxTwo.clientId()), anyInt());
            }
        }
    }
}
