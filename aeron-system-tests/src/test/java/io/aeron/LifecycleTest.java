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
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.mockito.Mockito.*;

@Timeout(10)
public class LifecycleTest
{
    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @Test
    public void shouldStartAndStopInstantly()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .errorHandler(Tests::onError);

        try (TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher))
        {
            final Aeron.Context clientCtx = new Aeron.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName());

            final Aeron aeron = Aeron.connect(clientCtx);
            aeron.close();
        }
        finally
        {
            driverCtx.deleteDirectory();
        }
    }

    @Test
    public void shouldNotifyOfClientTimestampCounter()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .errorHandler(Tests::onError);

        try (TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher))
        {
            final Aeron.Context clientCtxOne = new Aeron.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName());

            final Aeron.Context clientCtxTwo = new Aeron.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName());

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
        finally
        {
            driverCtx.deleteDirectory();
        }
    }
}
