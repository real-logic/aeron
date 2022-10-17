/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import org.agrona.concurrent.NoOpLock;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientContextTest
{
    @Test
    @SuppressWarnings("try")
    void shouldPreventCreatingMultipleClientsWithTheSameContext()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            final Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            try (Aeron ignore = Aeron.connect(ctx))
            {
                assertThrows(ConcurrentConcludeException.class, () -> Aeron.connect(ctx));
            }
        }
    }

    @Test
    @SuppressWarnings("try")
    void shouldRequireInvokerModeIfClientLockIsSet()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            final Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clientLock(NoOpLock.INSTANCE);

            assertThrows(AeronException.class, () -> Aeron.connect(ctx));
        }
    }

    @Test
    @SuppressWarnings("try")
    void shouldAllowCustomLockInAgentRunnerModeIfNotInstanceOfNoOpLock()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            final Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .clientLock(new ReentrantLock());

            try (Aeron aeron = Aeron.connect(ctx))
            {
                aeron.clientId();
            }
        }
    }
}
