/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.agent;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

public class DriverLoggingAgentTest
{
    private static final String NETWORK_CHANNEL = "aeron:udp?endpoint=localhost:54325";
    private static final int STREAM_ID = 1777;

    private static final IntHashSet MSG_ID_SET = new IntHashSet();
    private static final CountDownLatch LATCH = new CountDownLatch(1);
    private String testDirName;

    @BeforeEach
    public void before()
    {
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        System.setProperty(EventConfiguration.ENABLED_EVENT_CODES_PROP_NAME, "all");
        Common.beforeAgent();

        testDirName = Paths.get(IoUtil.tmpDirName(), "driver-test").toString();
        final File testDir = new File(testDirName);
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @AfterEach
    public void after()
    {
        Common.afterAgent();

        if (testDirName != null)
        {
            IoUtil.delete(new File(testDirName), false);
        }
    }

    @Test
    public void shouldLogMessages()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String aeronDirectoryName = Paths.get(testDirName, "media").toString();

            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .aeronDirectoryName(aeronDirectoryName);

            try (MediaDriver ignore = MediaDriver.launchEmbedded(driverCtx))
            {
                final Aeron.Context clientCtx = new Aeron.Context()
                    .aeronDirectoryName(driverCtx.aeronDirectoryName());

                try (Aeron aeron = Aeron.connect(clientCtx);
                    Subscription subscription = aeron.addSubscription(NETWORK_CHANNEL, STREAM_ID);
                    Publication publication = aeron.addPublication(NETWORK_CHANNEL, STREAM_ID))
                {
                    final UnsafeBuffer offerBuffer = new UnsafeBuffer(new byte[32]);
                    while (publication.offer(offerBuffer) < 0)
                    {
                        Thread.yield();
                    }

                    final MutableInteger counter = new MutableInteger();
                    final FragmentHandler handler = (buffer, offset, length, header) -> counter.value++;

                    while (0 == subscription.poll(handler, 1))
                    {
                        Thread.yield();
                    }

                    assertEquals(counter.get(), 1);
                }

                LATCH.await();
            }

            assertTrue(MSG_ID_SET.contains(CMD_IN_ADD_PUBLICATION.id()));
            assertTrue(MSG_ID_SET.contains(CMD_IN_ADD_SUBSCRIPTION.id()));
            assertTrue(MSG_ID_SET.contains(FRAME_IN.id()));
            assertTrue(MSG_ID_SET.contains(FRAME_OUT.id()));
            assertTrue(MSG_ID_SET.contains(CMD_OUT_AVAILABLE_IMAGE.id()));
            assertTrue(MSG_ID_SET.contains(CMD_IN_CLIENT_CLOSE.id()));
        });
    }

    static class StubEventLogReaderAgent implements Agent, MessageHandler
    {
        public String roleName()
        {
            return "event-log-reader";
        }

        public int doWork()
        {
            return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
        }

        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
        {
            MSG_ID_SET.add(msgTypeId);

            if (CMD_IN_CLIENT_CLOSE.id() == msgTypeId)
            {
                LATCH.countDown();
            }
        }
    }
}
