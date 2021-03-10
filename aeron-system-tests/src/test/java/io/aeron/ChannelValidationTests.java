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

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ChannelValidationTests
{
    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context();

    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

    private Aeron aeron;
    private TestMediaDriver driver;

    private void launch()
    {
        context
            .errorHandler(ignore -> {})
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));

        driver = TestMediaDriver.launch(context, watcher);
        aeron = Aeron.connect();
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(closeables);
        CloseHelper.closeAll(aeron, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    void shouldCantUseDifferentSoSndbufIfAlreadySetViaUri()
    {
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1002);
    }

    @Test
    void shouldCantUseDifferentSoSndbufIfAlreadySetViaContext()
    {
        context.socketSndbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1002);
    }

    @Test
    void shouldCantUseDifferentSoSndbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));
    }


    @Test
    void shouldCantUseDifferentSoRcvbufIfAlreadySetViaUri()
    {
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1002);
    }

    @Test
    void shouldCantUseDifferentSoRcvbufIfAlreadySetViaContext()
    {
        context.socketRcvbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1002);
    }

    @Test
    void shouldCantUseDifferentSoRcvbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));
    }

    @Test
    void shouldValidateMtuAgainstSoSndbufSetViaUri()
    {
        launch();

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|mtu=1056|so-sndbuf=1024", 1000));
    }

    @Test
    void shouldValidateMtuAgainstSoSndbufSetViaContext()
    {
        context.socketSndbufLength(4096);
        launch();

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|mtu=4128", 1000));
    }

    @Test
    void shouldValidateMtuAgainstSoSndbufSetViaOsDefault() throws IOException
    {
        final int defaultOsSocketSndbufLength;
        try (DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET))
        {
            defaultOsSocketSndbufLength = channel.getOption(StandardSocketOptions.SO_SNDBUF);
        }

        assumeTrue(
            defaultOsSocketSndbufLength < Configuration.MAX_UDP_PAYLOAD_LENGTH,
            "OS buffer sizes to big (use sudo sysctl net.core.wmem_default=8192 to verify)");

        final int desiredMaxMessageLength = 2 * defaultOsSocketSndbufLength;
        assumeTrue(
            desiredMaxMessageLength < FrameDescriptor.MAX_MESSAGE_LENGTH,
            "OS buffer sizes to big (use sudo sysctl net.core.wmem_default=8192 to verify)");

        final int termLength = BitUtil.findNextPositivePowerOfTwo(desiredMaxMessageLength * 8);
        context.publicationTermBufferLength(termLength);

        launch();

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|mtu=" + (defaultOsSocketSndbufLength + 32), 1000));
    }

    private Publication addPublication(final String channel, final int streamId)
    {
        final Publication pub = aeron.addPublication(channel, streamId);
        closeables.add(pub);
        return pub;
    }
}
