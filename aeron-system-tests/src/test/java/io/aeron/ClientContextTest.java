/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.driver.status.ClientHeartbeatTimestamp;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
@InterruptAfter(10)
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

    @Test
    void shouldHaveUniqueCorrelationIdsAcrossMultipleClientsToTheSameDriver()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx))
        {
            final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());

            try (Aeron aeron0 = Aeron.connect(ctx.clone());
                Aeron aeron1 = Aeron.connect(ctx.clone()))
            {
                assertNotEquals(aeron0.nextCorrelationId(), aeron1.nextCorrelationId());
            }
        }
    }

    @Test
    void shouldAddClientVersionInfoToTheHeartbeatTimestampCounter()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);

        try (MediaDriver mediaDriver = MediaDriver.launch(driverCtx);
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))))
        {
            // trigger creation of the timestamp counter on the driver side
            assertNotNull(aeron.addCounter(1000, "test"));

            final long clientId = aeron.clientId();
            int counterId = Aeron.NULL_VALUE;
            final CountersReader countersReader = aeron.countersReader();
            final String expectedLabel = ClientHeartbeatTimestamp.NAME + ": " + clientId + " " +
                AeronCounters.formatVersionInfo(AeronVersion.VERSION, AeronVersion.GIT_SHA);
            while (true)
            {
                if (Aeron.NULL_VALUE == counterId)
                {
                    counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                        countersReader, HeartbeatTimestamp.HEARTBEAT_TYPE_ID, clientId);
                }
                else
                {
                    final int labelLength =
                        countersReader.metaDataBuffer().getInt(
                        CountersReader.metaDataOffset(counterId) + CountersReader.LABEL_OFFSET);
                    if (expectedLabel.length() == labelLength &&
                        expectedLabel.equals(countersReader.getCounterLabel(counterId)))
                    {
                        break;
                    }
                }
                Thread.yield();
            }
        }
    }
}
