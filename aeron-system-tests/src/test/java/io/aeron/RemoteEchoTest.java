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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.NetworkUtil;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.echo.ProvisioningServerMain;
import io.aeron.samples.echo.api.EchoMonitorMBean;
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import io.aeron.test.*;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.aeron.samples.echo.api.ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING;
import static java.lang.Math.min;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@BindingsTest
@ExtendWith(InterruptingTestCallback.class)
class RemoteEchoTest
{
    private static final int SOURCE_DATA_LENGTH = 1024 * 1024;
    private static MediaDriver mediaDriver = null;
    private static ProvisioningServerMain provisioningServer = null;
    private static JMXConnector connector = null;
    private static IoSupplier<MBeanServerConnection> mbeanConnectionSupplier = null;
    private static String remoteHost = null;
    private static String localHost = null;
    private static String aeronDir = null;

    private DirectBuffer sourceData;
    private MBeanServerConnection mBeanServerConnection;
    private ProvisioningMBean provisioningMBean;

    private static boolean isEmpty(final String s)
    {
        return null == s || s.trim().isEmpty();
    }

    @RegisterExtension
    final RandomWatcher randomWatcher = new RandomWatcher(18604930465192L);

    @BeforeAll
    static void beforeAll() throws IOException
    {
        remoteHost = System.getProperty("aeron.test.system.binding.remote.host");
        localHost = System.getProperty("aeron.test.system.binding.local.host");
        aeronDir = System.getProperty("aeron.test.system.aeron.dir");

        // If aeron.dir is set assume an external driver, otherwise start embedded.
        if (isEmpty(aeronDir))
        {
            mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context().dirDeleteOnShutdown(true));
            aeronDir = mediaDriver.aeronDirectoryName();
        }

        // If remote host is not set run the provision service locally.
        if (isEmpty(remoteHost))
        {
            // TODO: echo service in basic test, check for remote access property.
            provisioningServer = ProvisioningServerMain.launch(new Aeron.Context());
            remoteHost = "localhost";
            localHost = "localhost";
            mbeanConnectionSupplier = ManagementFactory::getPlatformMBeanServer;
        }
        else
        {
            if (isEmpty(localHost))
            {
                final InetAddress address = InetAddress.getByName(remoteHost);
                localHost = Objects.requireNonNull(NetworkUtil.findFirstMatchingLocalAddress(address)).getHostAddress();
            }

            final String serviceURL = "service:jmx:rmi:///jndi/rmi://" + remoteHost + ":10000/jmxrmi";
            final JMXServiceURL url = new JMXServiceURL(serviceURL);
            connector = JMXConnectorFactory.connect(url);
            mbeanConnectionSupplier = connector::getMBeanServerConnection;

            System.out.println("Using local=" + localHost + " remote=" + remoteHost + " for communication");
        }
    }

    @AfterAll
    static void afterAll()
    {
        try
        {
            final MBeanServerConnection mBeanServerConnection = mbeanConnectionSupplier.get();
            final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
                mBeanServerConnection,
                new ObjectName(IO_AERON_TYPE_PROVISIONING_NAME_TESTING),
                ProvisioningMBean.class);
            provisioningMBean.removeAll();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        CloseHelper.quietCloseAll(connector, provisioningServer, mediaDriver);
    }

    @BeforeEach
    void setUp() throws IOException, MalformedObjectNameException, TimeoutException
    {
        final byte[] bytes = new byte[SOURCE_DATA_LENGTH];
        randomWatcher.random().nextBytes(bytes);
        sourceData = new UnsafeBuffer(bytes);

        connectJmxMBean();
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleSingleUnicastEchoPair() throws MalformedObjectNameException
    {
        final String requestUri = new ChannelUriStringBuilder()
            .media("udp").endpoint(remoteHost + ":24324").rejoin(false).linger(0L).termLength(1 << 16).build();
        final String responseUri = new ChannelUriStringBuilder()
            .media("udp").endpoint(localHost + ":24325").rejoin(false).linger(0L).termLength(1 << 16).build();
        final int requestStreamId = 1001;
        final int responseStreamId = 1002;

        provisioningMBean.createEchoPair(1, requestUri, requestStreamId, responseUri, responseStreamId);

        try (
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            Publication pub = aeron.addPublication(requestUri, requestStreamId);
            Subscription sub = aeron.addSubscription(responseUri, responseStreamId))
        {
            final EchoMonitorMBean echoMonitorMBean = JMX.newMBeanProxy(
                mBeanServerConnection,
                new ObjectName(ProvisioningConstants.echoPairObjectName(1)),
                EchoMonitorMBean.class);

            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            sendAndReceiveRandomData(pub, sub);

            assertTrue(0 < echoMonitorMBean.getFragmentCount());
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldHandleTenUnicastEchoPairs()
    {
        final List<Publication> pubs = new ArrayList<>();
        final List<Subscription> subs = new ArrayList<>();

        final ChannelUriStringBuilder requestUriBuilder = new ChannelUriStringBuilder()
            .media("udp").rejoin(false).linger(0L).termLength(1 << 16);
        final ChannelUriStringBuilder responseUriBuilder = new ChannelUriStringBuilder()
            .media("udp").rejoin(false).linger(0L).termLength(1 << 16);
        final int requestStreamId = 1001;
        final int responseStreamId = 1002;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir)))
        {
            for (int i = 0; i < 10; i++)
            {
                final String requestUri = requestUriBuilder.endpoint(remoteHost + ":" + (24300 + i)).build();
                final String responseUri = responseUriBuilder.endpoint(localHost + ":" + (24400 + i)).build();

                provisioningMBean.createEchoPair(i + 1, requestUri, requestStreamId, responseUri, responseStreamId);

                final ConcurrentPublication pub = aeron.addPublication(requestUri, requestStreamId);
                final Subscription sub = aeron.addSubscription(responseUri, responseStreamId);

                pubs.add(pub);
                subs.add(sub);

                Tests.awaitConnected(pub);
                Tests.awaitConnected(sub);
            }

            sendAndReceiveRandomData(pubs, subs);
        }
        finally
        {
            CloseHelper.quietCloseAll(pubs);
            CloseHelper.quietCloseAll(subs);
        }
    }

    private void sendAndReceiveRandomData(
        final Publication pub,
        final Subscription sub)
    {
        final ExpandableArrayBuffer receivedData = new ExpandableArrayBuffer(SOURCE_DATA_LENGTH);
        final MutableInteger sentBytes = new MutableInteger(0);
        final MutableInteger recvBytes = new MutableInteger(0);

        final FragmentHandler handler = (buffer, offset, length, header) ->
        {
            receivedData.putBytes(recvBytes.get(), buffer, offset, length);
            recvBytes.addAndGet(length);
        };

        while (recvBytes.get() < SOURCE_DATA_LENGTH)
        {
            if (sentBytes.get() < SOURCE_DATA_LENGTH)
            {
                final int randomLength = randomWatcher.random().nextInt(pub.maxMessageLength());
                final int toSend = min(SOURCE_DATA_LENGTH - sentBytes.get(), randomLength);
                if (pub.offer(sourceData, sentBytes.get(), toSend) > 0)
                {
                    sentBytes.addAndGet(toSend);
                }
                Tests.yield();
            }

            if (sub.poll(handler, 10) <= 0)
            {
                Tests.yield();
            }
        }

        assertEquals(0, sourceData.compareTo(receivedData));
    }

    private void sendAndReceiveRandomData(
        final List<Publication> pubs,
        final List<Subscription> subs)
    {
        assertEquals(pubs.size(), subs.size());

        final List<ExpandableArrayBuffer> receivedDataBuffers = new ArrayList<>();
        final List<MutableInteger> sentDataCounts = new ArrayList<>();
        final List<MutableInteger> receivedDataCounts = new ArrayList<>();
        final List<FragmentHandler> handlers = new ArrayList<>();

        for (int i = 0; i < pubs.size(); i++)
        {
            final ExpandableArrayBuffer receivedData = new ExpandableArrayBuffer(SOURCE_DATA_LENGTH);
            final MutableInteger sentBytes = new MutableInteger(0);
            final MutableInteger recvBytes = new MutableInteger(0);
            final FragmentHandler handler = (buffer, offset, length, header) ->
            {
                receivedData.putBytes(recvBytes.get(), buffer, offset, length);
                recvBytes.addAndGet(length);
            };

            receivedDataBuffers.add(receivedData);
            sentDataCounts.add(sentBytes);
            receivedDataCounts.add(recvBytes);
            handlers.add(handler);
        }

        while (dataIsPending(receivedDataCounts, SOURCE_DATA_LENGTH))
        {
            for (int i = 0; i < pubs.size(); i++)
            {
                final Publication pub = pubs.get(i);
                final Subscription sub = subs.get(i);
                final MutableInteger sentBytes = sentDataCounts.get(i);
                final FragmentHandler handler = handlers.get(i);

                if (sentBytes.get() < SOURCE_DATA_LENGTH)
                {
                    final int randomLength = randomWatcher.random().nextInt(pub.maxMessageLength());
                    final int toSend = min(SOURCE_DATA_LENGTH - sentBytes.get(), randomLength);
                    if (pub.offer(sourceData, sentBytes.get(), toSend) > 0)
                    {
                        sentBytes.addAndGet(toSend);
                    }
                    Tests.yield();
                }

                if (sub.poll(handler, 10) <= 0)
                {
                    Tests.yield();
                }
            }
        }

        for (final ExpandableArrayBuffer receivedData : receivedDataBuffers)
        {
            assertEquals(0, sourceData.compareTo(receivedData));
        }

    }

    private boolean dataIsPending(final List<MutableInteger> receivedDataCounts, final int sourceDataLength)
    {
        for (final MutableInteger receivedDataCount : receivedDataCounts)
        {
            if (receivedDataCount.get() < sourceDataLength)
            {
                return true;
            }
        }

        return false;
    }

    private interface IoSupplier<T>
    {
        T get() throws IOException;
    }

    private void connectJmxMBean() throws IOException, MalformedObjectNameException, TimeoutException
    {
        mBeanServerConnection = mbeanConnectionSupplier.get();
        final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
        do
        {
            final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
                mBeanServerConnection,
                new ObjectName(IO_AERON_TYPE_PROVISIONING_NAME_TESTING),
                ProvisioningMBean.class);

            try
            {
                provisioningMBean.removeAll();
                this.provisioningMBean = provisioningMBean;
                break;
            }
            catch (final UndeclaredThrowableException e)
            {
                if (!(e.getCause() instanceof InstanceNotFoundException))
                {
                    throw e;
                }
            }

            if (deadlineMs <= System.currentTimeMillis())
            {
                throw new TimeoutException("Failed to connect to provisioning mbean");
            }
        }
        while (true);
    }
}
