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

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
class ChannelValidationTests
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context();
    {
        context
            .errorHandler(ignore -> {})
            .dirDeleteOnStart(true)
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));
    }

    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

    private Aeron aeron;
    private TestMediaDriver driver;

    private void launch()
    {
        driver = TestMediaDriver.launch(context, watcher);
        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(closeables);
        CloseHelper.closeAll(aeron, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    void publicationCantUseDifferentSoSndbufIfAlreadySetViaUri()
    {
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1002);
    }

    @Test
    void publicationCantUseDifferentSoSndbufIfAlreadySetViaContext()
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
    void publicationCantUseDifferentSoSndbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));
    }


    @Test
    void publicationCantUseDifferentSoRcvbufIfAlreadySetViaUri()
    {
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));

        addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1002);
    }

    @Test
    void publicationCantUseDifferentSoRcvbufIfAlreadySetViaContext()
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
    void publicationCantUseDifferentSoRcvbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addPublication("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addPublication("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));
    }

    @Test
    void subscriptionCantUseDifferentSoSndbufIfAlreadySetViaUri()
    {
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));

        addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1002);
    }

    @Test
    void subscriptionCantUseDifferentSoSndbufIfAlreadySetViaContext()
    {
        context.socketSndbufLength(131072);
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));

        addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=131072", 1002);
    }

    @Test
    void subscriptionCantUseDifferentSoSndbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-sndbuf=65536", 1001));
    }


    @Test
    void subscriptionCantUseDifferentSoRcvbufIfAlreadySetViaUri()
    {
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));

        addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1002);
    }

    @Test
    void subscriptionCantUseDifferentSoRcvbufIfAlreadySetViaContext()
    {
        context.socketRcvbufLength(131072);
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));

        addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=131072", 1002);
    }

    @Test
    void subscriptionCantUseDifferentSoRcvbufIfAlreadySetViaDefault()
    {
        context.socketRcvbufLength(131072);
        launch();

        addSubscription("aeron:udp?endpoint=localhost:9999", 1000);

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|so-rcvbuf=65536", 1001));
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
            () ->
            {
                final String uri = "aeron:udp?endpoint=localhost:9999|mtu=" + ((2 * defaultOsSocketSndbufLength) + 32);
                addPublication(uri, 1000);
            });
    }

    @Test
    void shouldValidateReceiverWindowAgainstSoRcvbufSetViaUri()
    {
        launch();

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=1056|so-rcvbuf=1024", 1000));
    }

    @Test
    void shouldValidateReceiverWindowAgainstSoRcvbufSetViaContext()
    {
        context.socketRcvbufLength(4096);
        context.initialWindowLength(4096);
        launch();

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=4128", 1000));
    }

    @Test
    void shouldValidateReceiverAgainstSoRcvbufSetViaOsDefault() throws IOException
    {
        final int defaultOsSocketRcvbufLength;
        try (DatagramChannel channel = DatagramChannel.open(StandardProtocolFamily.INET))
        {
            defaultOsSocketRcvbufLength = channel.getOption(StandardSocketOptions.SO_RCVBUF);
        }

        assumeTrue(
            defaultOsSocketRcvbufLength < Configuration.MAX_UDP_PAYLOAD_LENGTH,
            "OS buffer sizes to big (use sudo sysctl net.core.rmem_default=8192 to verify)");

        final int desiredMaxMessageLength = 2 * defaultOsSocketRcvbufLength;
        assumeTrue(
            desiredMaxMessageLength < FrameDescriptor.MAX_MESSAGE_LENGTH,
            "OS buffer sizes to big (use sudo sysctl net.core.rmem_default=8192 to verify)");

        final int termLength = BitUtil.findNextPositivePowerOfTwo(desiredMaxMessageLength * 8);
        context.publicationTermBufferLength(termLength);
        context.socketRcvbufLength(0);
        context.initialWindowLength(defaultOsSocketRcvbufLength);

        launch();

        assertThrows(
            RegistrationException.class,
            () ->
            {
                final int receiverWindow = (2 * defaultOsSocketRcvbufLength) + 32;
                addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=" + receiverWindow, 1000);
            });
    }

    @Test
    @InterruptAfter(5)
    void shouldValidateSenderMtuAgainstUriReceiverWindow() throws IOException
    {
        context.errorHandler(null);
        launch();

        final long initialErrorCount = aeron.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        addPublication("aeron:udp?endpoint=localhost:9999|mtu=1408", 1000);
        addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=1376", 1000);

        Tests.awaitCounterDelta(aeron.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher = allOf(
            containsString("mtuLength="),
            containsString("> initialWindowLength="));

        SystemTests.waitForErrorToOccur(driver.aeronDirectoryName(), exceptionMessageMatcher, Tests.SLEEP_1_MS);
    }

    @ParameterizedTest
    @ValueSource(strings = { "mtu", "rcv-wnd", "so-rcvbuf", "so-sndbuf" })
    void shouldNotAllowUriParametersForManualMdc(final String parameter)
    {
        launch();

        final Publication publication = addPublication("aeron:udp?control-mode=manual", 1000);

        final RegistrationException registrationException = assertThrows(
            RegistrationException.class,
            () -> publication.addDestination("aeron:udp?endpoint=localhost:9999|" + parameter + "=4096"));

        assertThat(registrationException.getMessage(), containsString(parameter));
    }

    @ParameterizedTest
    @ValueSource(strings = { "mtu", "rcv-wnd", "so-rcvbuf", "so-sndbuf" })
    void shouldNotAllowUriParametersForManualMds(final String parameter)
    {
        launch();

        final Subscription subscription = addSubscription("aeron:udp?control-mode=manual", 1000);

        final RegistrationException registrationException = assertThrows(
            RegistrationException.class,
            () -> subscription.addDestination("aeron:udp?endpoint=localhost:9999|" + parameter + "=4096"));

        assertThat(registrationException.getMessage(), containsString(parameter));
    }

    @Test
    void shouldErrorOnPublicationWithWildcardEndpoint()
    {
        launch();

        assertThrows(RegistrationException.class, () -> addPublication("aeron:udp?endpoint=localhost:0", 10001));
    }

    @Test
    void shouldErrorOnPublicationAddDestinationWithWildcardEndpoint()
    {
        launch();

        final Publication publication = addPublication("aeron:udp?control-mode=manual", 10001);
        assertThrows(RegistrationException.class, () -> publication.addDestination("aeron:udp?endpoint=localhost:0"));
    }

    @Test
    void shouldErrorOnSubscriptionWithWildcardControl()
    {
        launch();

        assertThrows(
            RegistrationException.class,
            () -> addSubscription("aeron:udp?control=localhost:0|endpoint=localhost:20000", 10001));
    }

    private Publication addPublication(final String channel, final int streamId)
    {
        final Publication pub = aeron.addPublication(channel, streamId);
        closeables.add(pub);
        return pub;
    }

    private Subscription addSubscription(final String channel, final int streamId)
    {
        final Subscription sub = aeron.addSubscription(channel, streamId);
        closeables.add(sub);
        return sub;
    }
}
