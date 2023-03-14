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
import io.aeron.test.Tests;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReconnectTest
{

    @Test
    void name()
    {
        final String channel = "aeron:udp?endpoint=localhost:10001";
        final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);
        final int streamId = 10001;

        try (
            MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnShutdown(true));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pub = aeron.addPublication(channel, streamId))
        {
            while (!Thread.currentThread().isInterrupted())
            {
                final AtomicBoolean imageUnavailable = new AtomicBoolean(false);
                try (Subscription sub = aeron.addSubscription(
                    channel, streamId, image -> {}, image -> imageUnavailable.set(true)))
                {
                    Tests.awaitConnected(sub);
                    Tests.awaitConnected(pub);

                    while (0 > pub.offer(message))
                    {
                        Tests.yield();
                    }
                    System.out.println("Sent");

                    while (0 >= sub.poll((buffer, offset, length, header) -> {}, 1))
                    {
                        Tests.yield();
                    }
                    System.out.println("Received");
                }

                while (!imageUnavailable.get())
                {
                    Tests.yield();
                }

                Tests.sleep(1_000);
            }
        }
    }
}
