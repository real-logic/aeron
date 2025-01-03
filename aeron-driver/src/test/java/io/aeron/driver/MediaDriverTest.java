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
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class MediaDriverTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    void shouldPrintConfigOnStart()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .printConfigurationOnStart(true);

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream(os);
        final PrintStream out = System.out;
        System.setOut(printStream);

        try (MediaDriver mediaDriver = MediaDriver.launch(context))
        {
            assertTrue(mediaDriver.context().printConfigurationOnStart());
            assertThat(os.toString(), containsString("printConfigurationOnStart=true"));
        }
        finally
        {
            System.setOut(out);
        }
    }

    @Test
    void shouldInitializeDutyCycleTrackersWhenNotSet(final @TempDir Path tempDir) throws IOException
    {
        final Path aeronDir = tempDir.resolve("aeron");
        Files.createDirectories(aeronDir);
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .conductorCycleThresholdNs(123)
            .senderCycleThresholdNs(456)
            .receiverCycleThresholdNs(789)
            .nameResolverThresholdNs(101010);

        assertNull(context.conductorDutyCycleTracker());
        assertNull(context.senderDutyCycleTracker());
        assertNull(context.receiverDutyCycleTracker());
        assertNull(context.nameResolverTimeTracker());

        try
        {
            context.conclude();

            verifyStallTracker(
                context.conductorDutyCycleTracker(),
                CONDUCTOR_MAX_CYCLE_TIME,
                CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
                context.conductorCycleThresholdNs());
            verifyStallTracker(
                context.senderDutyCycleTracker(),
                SENDER_MAX_CYCLE_TIME,
                SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
                context.senderCycleThresholdNs());
            verifyStallTracker(
                context.receiverDutyCycleTracker(),
                RECEIVER_MAX_CYCLE_TIME,
                RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED,
                context.receiverCycleThresholdNs());
            verifyStallTracker(
                context.nameResolverTimeTracker(),
                NAME_RESOLVER_MAX_TIME,
                NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED,
                context.nameResolverThresholdNs());
        }
        finally
        {
            context.close();
        }
    }

    @Test
    void shouldUseProvidedDutyCycleTrackers(final @TempDir Path tempDir) throws IOException
    {
        final Path aeronDir = tempDir.resolve("aeron");
        Files.createDirectories(aeronDir);
        final DutyCycleTracker conductorDutyCycleTracker = new DutyCycleTracker();
        final DutyCycleTracker senderDutyCycleTracker = new DutyCycleTracker();
        final DutyCycleTracker receiverDutyCycleTracker = new DutyCycleTracker();
        final DutyCycleTracker nameResolverTimeTracker = new DutyCycleTracker();
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .conductorDutyCycleTracker(conductorDutyCycleTracker)
            .senderDutyCycleTracker(senderDutyCycleTracker)
            .receiverDutyCycleTracker(receiverDutyCycleTracker)
            .nameResolverTimeTracker(nameResolverTimeTracker);

        assertSame(conductorDutyCycleTracker, context.conductorDutyCycleTracker());
        assertSame(senderDutyCycleTracker, context.senderDutyCycleTracker());
        assertSame(receiverDutyCycleTracker, context.receiverDutyCycleTracker());
        assertSame(nameResolverTimeTracker, context.nameResolverTimeTracker());

        try
        {
            context.conclude();

            assertSame(conductorDutyCycleTracker, context.conductorDutyCycleTracker());
            assertSame(senderDutyCycleTracker, context.senderDutyCycleTracker());
            assertSame(receiverDutyCycleTracker, context.receiverDutyCycleTracker());
            assertSame(nameResolverTimeTracker, context.nameResolverTimeTracker());
        }
        finally
        {
            context.close();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldExecuteAsyncCommandsInOrder(final @TempDir Path tempDir)
    {
        final Path aeronDir = tempDir.resolve("aeron");
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .threadingMode(ThreadingMode.DEDICATED)
            .asyncTaskExecutorThreads(2);
        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(context, systemTestWatcher))
        {
            systemTestWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());
            try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir.toString())))
            {
                final String channel = "aeron:udp?endpoint=localhost:5050";
                final int streamId = 1111;
                final long pubId1 = aeron.asyncAddExclusivePublication(channel, streamId);
                aeron.asyncRemovePublication(pubId1);

                final long pubId2 = aeron.asyncAddExclusivePublication(channel, streamId);

                Tests.await(() -> null != aeron.getExclusivePublication(pubId2));

                assertNull(aeron.getExclusivePublication(pubId1));
                assertNotNull(aeron.getExclusivePublication(pubId2));
            }
        }
    }

    private static void verifyStallTracker(
        final DutyCycleTracker dutyCycleTracker,
        final SystemCounterDescriptor maxCycleTimeCounter,
        final SystemCounterDescriptor cycleTimeThresholdExceededCounter,
        final long cycleTimeThresholdNs)
    {
        final DutyCycleStallTracker stallTracker = assertInstanceOf(DutyCycleStallTracker.class, dutyCycleTracker);
        assertEquals(maxCycleTimeCounter.id(), stallTracker.maxCycleTime().id());
        assertEquals(cycleTimeThresholdExceededCounter.id(), stallTracker.cycleTimeThresholdExceededCount().id());
        assertEquals(cycleTimeThresholdNs, stallTracker.cycleTimeThresholdNs());
    }
}
