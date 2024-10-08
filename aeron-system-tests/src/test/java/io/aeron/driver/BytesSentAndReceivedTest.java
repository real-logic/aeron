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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
public class BytesSentAndReceivedTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver mediaDriver;
    private Aeron aeron;

    @BeforeEach
    void before(@TempDir final Path tempDir)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(tempDir.toString())
            .threadingMode(ThreadingMode.SHARED);
        mediaDriver = TestMediaDriver.launch(context, systemTestWatcher);

        systemTestWatcher.dataCollector().add(context.aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, mediaDriver);
    }

    @ParameterizedTest
    @InterruptAfter(20)
    @ValueSource(ints = { 1, 5, 10 })
    void unicast(final int numberOfTransports)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final Random random = new Random(-4732947238473892L);
        random.nextBytes(buffer.byteArray());
        final MutableInteger fragmentLength = new MutableInteger();
        final FragmentHandler fragmentHandler = (buf, offset, length, header) -> fragmentLength.set(length);

        final List<Publication> publications = new ArrayList<>();
        final List<Subscription> subscriptions = new ArrayList<>();

        for (int i = 0, port = 5500; i < numberOfTransports; i++)
        {
            final String channel = "aeron:udp?term-length=64k|endpoint=localhost:" + (++port);
            publications.add(aeron.addPublication(channel, port));
            subscriptions.add(aeron.addSubscription(channel, port));
        }

        long expectedTotalBytes = 0;
        for (int i = 0; i < numberOfTransports; i++)
        {
            final Publication publication = publications.get(i);
            final Subscription subscription = subscriptions.get(i);
            Tests.awaitConnected(publication);
            Tests.awaitConnected(subscription);

            final int length = random.nextInt(100, 1000);
            while (publication.offer(buffer, 0, length) < 0)
            {
                Tests.yield();
            }

            fragmentLength.set(0);
            while (fragmentLength.get() != length)
            {
                if (0 == subscription.poll(fragmentHandler, 1))
                {
                    Tests.yield();
                }
            }

            assertThat(subscription.imageAtIndex(0).position(), equalTo(publication.position()));

            expectedTotalBytes += publication.position() + HEADER_LENGTH /* heartbeat */;
        }

        final CountersReader countersReader = aeron.countersReader();
        assertThat(
            SystemCounterDescriptor.BYTES_SENT.label(),
            countersReader.getCounterValue(SystemCounterDescriptor.BYTES_SENT.id()),
            greaterThanOrEqualTo(expectedTotalBytes));
        assertThat(
            SystemCounterDescriptor.BYTES_RECEIVED.label(),
            countersReader.getCounterValue(SystemCounterDescriptor.BYTES_RECEIVED.id()),
            greaterThanOrEqualTo(expectedTotalBytes));
    }
}
