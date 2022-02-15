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
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RemoteEchoTest
{
    private static MediaDriver mediaDriver = null;
    private static ProvisioningServerMain provisioningServer = null;
    private static Thread provisioningServerThread = null;
    private static JMXConnector connector = null;
    private static String remoteHost = null;
    private static String localHost = null;

    @BeforeAll
    static void beforeAll() throws IOException
    {
        remoteHost = System.getProperty("aeron.test.system.binding.remote.host");
        localHost = System.getProperty("aeron.test.system.binding.local.host");
        if (null == remoteHost)
        {
            // TODO: check before launching and may use existing driver.
            mediaDriver = MediaDriver.launch();
            // TODO: only start echo service in basic test, check for remote access property.
            provisioningServer = ProvisioningServerMain.launch(new Aeron.Context());
            provisioningServerThread = AgentRunner.startOnThread(new AgentRunner(
                new BackoffIdleStrategy(), throwable -> {}, null, provisioningServer));
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
        if (null != provisioningServerThread)
        {
            provisioningServerThread.interrupt();
            try
            {
                provisioningServerThread.join();
            }
            catch (final InterruptedException ignore)
            {
            }
        }
        CloseHelper.quietCloseAll(connector, provisioningServer, mediaDriver);
    }

    @Test
    void testSingleEchoPair() throws IOException, MalformedObjectNameException
    {
        final String requestUri = new ChannelUriStringBuilder().media("udp").endpoint(remoteHost + ":24324").build();
        final String responseUri = new ChannelUriStringBuilder().media("udp").endpoint(localHost + ":24325").build();
        final int requestStreamId = 1001;
        final int responseStreamId = 1001;
        final String message = "hello world";

        try (
            MediaDriver driver = MediaDriver.launchEmbedded();
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication pub = aeron.addPublication(requestUri, requestStreamId);
            Subscription sub = aeron.addSubscription(responseUri, responseStreamId))
        {
            final MBeanServerConnection mBeanServerConnection = connector.getMBeanServerConnection();

            final ObjectName objectName = new ObjectName(ProvisioningConstants.IO_AERON_TYPE_PROVISIONING_NAME_TESTING);
            final ProvisioningMBean provisioningMBean = JMX.newMBeanProxy(
                mBeanServerConnection, objectName, ProvisioningMBean.class);

            provisioningMBean.createEchoPair(1, requestUri, requestStreamId, responseUri, responseStreamId);

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
        }
    }
}
