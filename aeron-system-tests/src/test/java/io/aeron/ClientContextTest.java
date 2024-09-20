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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.ClientHeartbeatTimestamp;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static io.aeron.driver.status.SystemCounterDescriptor.BYTES_CURRENTLY_MAPPED;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_META_DATA_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class ClientContextTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestMediaDriver mediaDriver;

    @BeforeEach
    void before()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);

        mediaDriver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(mediaDriver);
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPreventCreatingMultipleClientsWithTheSameContext()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());

        try (Aeron ignore = Aeron.connect(ctx))
        {
            assertThrows(ConcurrentConcludeException.class, () -> Aeron.connect(ctx));
        }
    }

    @Test
    @SuppressWarnings("try")
    void shouldRequireInvokerModeIfClientLockIsSet()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientLock(NoOpLock.INSTANCE);

        assertThrows(AeronException.class, () -> Aeron.connect(ctx));
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldAllowCustomLockInAgentRunnerModeIfNotInstanceOfNoOpLock()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientLock(new ReentrantLock());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            aeron.clientId();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldHaveUniqueCorrelationIdsAcrossMultipleClientsToTheSameDriver()
    {
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());

        try (Aeron aeron0 = Aeron.connect(ctx.clone());
            Aeron aeron1 = Aeron.connect(ctx.clone()))
        {
            assertNotEquals(aeron0.nextCorrelationId(), aeron1.nextCorrelationId());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "my-test-client" })
    @InterruptAfter(10)
    void shouldAddClientInfoToTheHeartbeatTimestampCounter(final String clientName)
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientName(clientName)
            .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))))
        {
            // trigger creation of the timestamp counter on the driver side
            assertNotNull(aeron.addCounter(1000, "test"));

            final long clientId = aeron.clientId();
            int counterId = Aeron.NULL_VALUE;
            final CountersReader countersReader = aeron.countersReader();
            final String baseLabel = ClientHeartbeatTimestamp.NAME + ": id=" + clientId;
            final String expandedLabel = baseLabel + " name=" + clientName + " " +
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
                    if (labelLength > baseLabel.length())
                    {
                        assertEquals(expandedLabel, countersReader.getCounterLabel(counterId));
                        break;
                    }
                }
                Tests.yield();
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAddClientInfoToTheHeartbeatTimestampCounterUpToMaxLabelLength()
    {
        final String clientName = Tests.generateStringWithSuffix("", "X", 100);
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientName(clientName)
            .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))))
        {
            // trigger creation of the timestamp counter on the driver side
            assertNotNull(aeron.addCounter(1000, "test"));

            final long clientId = aeron.clientId();
            int counterId = Aeron.NULL_VALUE;
            final CountersReader countersReader = aeron.countersReader();
            final String baseLabel = ClientHeartbeatTimestamp.NAME + ": id=" + clientId;
            final String expandedLabel =
                baseLabel + " name=" + clientName.substring(0, 100) +
                " version=" + AeronVersion.VERSION + " commit=" + AeronVersion.GIT_SHA;
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
                    if (labelLength > baseLabel.length())
                    {
                        assertEquals(expandedLabel, countersReader.getCounterLabel(counterId));
                        break;
                    }
                }
                Tests.yield();
            }
        }
    }

    @Test
    void shouldRejectClientNameThatIsTooLong()
    {
        final String name =
            "this is a very long value that we are hoping with be reject when the value gets " +
            "set on the the context without causing issues will labels";

        final AeronException aeronException = assertThrows(
            AeronException.class, () -> new Aeron.Context().clientName(name).conclude());
        assertEquals("ERROR - clientName length must <= 100", aeronException.getMessage());
    }

    @Test
    void logBuffersMappedOnTheClientSideShouldBeReportedViaSystemCounter()
    {
        final String channel = "aeron:ipc?term-length=128m";
        final int streamId = 42;
        final Aeron.Context context = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());
        try (Aeron aeron = Aeron.connect(context.clone());
            ExclusivePublication publication = aeron.addExclusivePublication(channel, streamId);
            Subscription subscription = aeron.addSubscription(channel, streamId))
        {
            Tests.awaitConnected(publication);
            Tests.awaitConnected(subscription);

            final long logBufferLength = (long)publication.termBufferLength() * PARTITION_COUNT + LOG_META_DATA_LENGTH;
            final CountersReader countersReader = aeron.countersReader();
            final long originalMappedBytes = countersReader.getCounterValue(BYTES_CURRENTLY_MAPPED.id());
            assertThat(originalMappedBytes, is(greaterThan(logBufferLength * 2)));

            // LogBuffers are shared so now new mappings reported
            for (int i = 0; i < 5; i++)
            {
                final Subscription sub = aeron.addSubscription(channel, streamId);
                Tests.awaitConnected(sub);
            }

            assertThat(
                countersReader.getCounterValue(BYTES_CURRENTLY_MAPPED.id()),
                is(originalMappedBytes));

            try (Aeron client1 = Aeron.connect(context.clone());
                Aeron client2 = Aeron.connect(context.clone()))
            {
                final Subscription sub1 = client1.addSubscription(channel, streamId);
                final Subscription sub2 = client2.addSubscription(channel, streamId);
                final Subscription sub3 = client2.addSubscription(channel, streamId);
                Tests.awaitConnected(sub1);
                Tests.awaitConnected(sub2);
                Tests.awaitConnected(sub3);

                assertThat(countersReader.getCounterValue(BYTES_CURRENTLY_MAPPED.id()), allOf(
                    greaterThan(originalMappedBytes + logBufferLength * 2),
                    lessThan(originalMappedBytes + logBufferLength * 3)));

                assertEquals(3, countConnectedClients(countersReader));
            }

            Tests.await(() -> 1 == countConnectedClients(countersReader));

            assertThat(countersReader.getCounterValue(BYTES_CURRENTLY_MAPPED.id()), is(originalMappedBytes));
        }
    }

    private static int countConnectedClients(final CountersReader countersReader)
    {
        final MutableInteger clientCount = new MutableInteger();
        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (HeartbeatTimestamp.HEARTBEAT_TYPE_ID == typeId)
            {
                clientCount.increment();
            }
        });
        return clientCount.get();
    }
}
