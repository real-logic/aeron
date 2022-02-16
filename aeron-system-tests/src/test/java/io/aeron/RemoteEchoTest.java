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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.NetworkUtil;
import io.aeron.samples.echo.ProvisioningServerMain;
import io.aeron.samples.echo.api.EchoMonitorMBean;
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import io.aeron.test.BindingsTest;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static io.aeron.samples.echo.api.ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING;
import static org.junit.jupiter.api.Assertions.assertEquals;

@BindingsTest
public class RemoteEchoTest
{
    private static MediaDriver mediaDriver = null;
    private static ProvisioningServerMain provisioningServer = null;
    private static JMXConnector connector = null;
    private static String remoteHost = null;
    private static String localHost = null;
    private static String aeronDir = null;

    @BeforeAll
    static void beforeAll() throws IOException
    {
        remoteHost = System.getProperty("aeron.test.system.binding.remote.host");
        localHost = System.getProperty("aeron.test.system.binding.local.host");
        aeronDir = System.getProperty("aeron.dir");

        // If aeron.dir is set assume an external driver, otherwise start embedded.
        if (null == aeronDir)
        {
            mediaDriver = MediaDriver.launchEmbedded();
            aeronDir = mediaDriver.aeronDirectoryName();
        }

        // If remote host is not set run the provision service locally.
        if (null == remoteHost)
        {
            // TODO: echo service in basic test, check for remote access property.
            provisioningServer = ProvisioningServerMain.launch(new Aeron.Context());
            remoteHost = "localhost";
            localHost = "localhost";
        }
        else
        {
            if (null == localHost)
            {
                final InetAddress address = InetAddress.getByName(remoteHost);
                localHost = Objects.requireNonNull(NetworkUtil.findFirstMatchingLocalAddress(address)).getHostAddress();
            }
        }

        final JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + remoteHost + ":10000/jmxrmi");
        connector = JMXConnectorFactory.connect(url);
    }

    @AfterAll
    static void afterAll()
    {
        CloseHelper.quietCloseAll(connector, provisioningServer, mediaDriver);
    }

    @Test
    void testSingleEchoPair() throws IOException, MalformedObjectNameException
    {
        final String requestUri = new ChannelUriStringBuilder()
            .media("udp").endpoint(remoteHost + ":24324").rejoin(false).linger(0L).build();
        final String responseUri = new ChannelUriStringBuilder()
            .media("udp").endpoint(localHost + ":24325").rejoin(false).linger(0L).build();
        final int requestStreamId = 1001;
        final int responseStreamId = 1001;
        final String message = "hello world";

        final MBeanServerConnection mBeanServerConnection = connector.getMBeanServerConnection();
        final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
            mBeanServerConnection,
            new ObjectName(IO_AERON_TYPE_PROVISIONING_NAME_TESTING),
            ProvisioningMBean.class);
        provisioningMBean.removeAll();
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

            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(message.getBytes(StandardCharsets.UTF_8));
            while (pub.offer(unsafeBuffer) < 0)
            {
                Tests.yield();
            }

            while (sub.poll((buffer, offset, length, header) ->
            {
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                final String incoming = new String(bytes);
                assertEquals(message, incoming);
            }, 1) < 1)
            {
                Tests.yield();
            }

            assertEquals(1, echoMonitorMBean.getFragmentCount());
        }
    }
}
