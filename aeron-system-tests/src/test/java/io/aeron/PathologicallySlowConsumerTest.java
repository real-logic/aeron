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
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.LangUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class PathologicallySlowConsumerTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED_NETWORK)
            .counterFreeToReuseTimeoutNs(0)
            .flowControlReceiverTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50))
            .imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(375))
            .publicationLingerTimeoutNs(TimeUnit.MILLISECONDS.toNanos(222))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(200))
            .untetheredWindowLimitTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .untetheredRestingTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driver = TestMediaDriver.launch(context, testWatcher);
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        testWatcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:ipc?term-length=64k",
        "aeron:udp?term-length=64k|control-mode=dynamic|control=localhost:8338|fc=max" })
    @InterruptAfter(10)
    void test(final String channel) throws InterruptedException
    {
        final int streamId = 9999;
        final BufferClaim bufferClaim = new BufferClaim();
        final FragmentHandler emptyHandler = (buffer, offset, length, header) -> {};
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final ExclusivePublication publication = aeron.addExclusivePublication(channel + "|alias=publisher", streamId);
        final Subscription subscription = aeron.addSubscription(channel + "|alias=subscriber", streamId);

        final String slowChannel = channel + "|alias=slow-subscriber|tether=false";
        final AtomicInteger imageAvailable = new AtomicInteger(0);
        final AtomicInteger imageUnavailable = new AtomicInteger(0);
        try (Aeron slowAeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .resourceLingerDurationNs(TimeUnit.MILLISECONDS.toNanos(30)));
            Subscription slowSubscription = slowAeron.addSubscription(
                slowChannel,
                streamId,
                image -> imageAvailable.incrementAndGet(),
                image -> imageUnavailable.incrementAndGet()))
        {
            Tests.awaitConnected(publication);
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(slowSubscription);

            for (int i = 0; i < 3; i++)
            {
                final int length = random.nextInt(4, 1000);
                while (publication.tryClaim(length, bufferClaim) < 0)
                {
                    Tests.yield();
                }

                bufferClaim.buffer().putInt(bufferClaim.offset(), length);
                bufferClaim.commit();

                while (0 == subscription.poll(emptyHandler, 1))
                {
                    Tests.yield();
                }
            }

            final Image slowImage = slowSubscription.imageAtIndex(0);

            final AtomicReference<Throwable> error = new AtomicReference<>();
            final Thread thread = new Thread(() ->
            {
                try
                {
                    assertEquals(1, slowSubscription.poll(
                        (buffer, offset, length, header) -> Tests.sleep(2500),
                        Integer.MAX_VALUE));
                }
                catch (final Throwable ex)
                {
                    error.set(ex);
                }
            });
            thread.start();

            while (!slowImage.isClosed())
            {
                while (publication.tryClaim(publication.maxPayloadLength(), bufferClaim) < 0)
                {
                    Tests.yield();
                }
                bufferClaim.buffer().putLong(bufferClaim.offset(), random.nextLong());
                bufferClaim.commit();

                while (0 == subscription.poll(emptyHandler, Integer.MAX_VALUE))
                {
                    Tests.yield();
                }

                Tests.sleep(1);
            }

            thread.join();
            assertEquals(1, imageUnavailable.get());
            assertEquals(2, imageAvailable.get());
            if (null != error.get())
            {
                LangUtil.rethrowUnchecked(error.get());
            }
        }
    }
}
