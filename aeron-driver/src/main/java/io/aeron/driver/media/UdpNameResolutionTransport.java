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
package io.aeron.driver.media;

import io.aeron.CommonContext;
import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.MediaDriver;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class UdpNameResolutionTransport extends UdpChannelTransport
{
    public interface UdpFrameHandler
    {
        int onFrame(
            UnsafeBuffer unsafeBuffer,
            int length,
            InetSocketAddress srcAddress,
            long nowMs);
    }

    private final UnsafeBuffer unsafeBuffer;
    private final ByteBuffer byteBuffer;

    public UdpNameResolutionTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress resolverAddr,
        final UnsafeBuffer unsafeBuffer,
        final MediaDriver.Context context)
    {
        super(udpChannel, null, resolverAddr, null, context);

        this.unsafeBuffer = unsafeBuffer;
        this.byteBuffer = unsafeBuffer.byteBuffer();
    }

    public int poll(final UdpFrameHandler handler, final long nowMs)
    {
        int bytesReceived = 0;
        final InetSocketAddress srcAddress = receive(byteBuffer);

        if (null != srcAddress)
        {
            final int length = byteBuffer.position();

            if (isValidFrame(unsafeBuffer, length))
            {
                receiveHook(unsafeBuffer, length, srcAddress);
                bytesReceived = handler.onFrame(unsafeBuffer, length, srcAddress, nowMs);
            }
        }

        return bytesReceived;
    }

    /**
     * Send contents of {@link java.nio.ByteBuffer} to the remote address.
     *
     * @param buffer        to send containing the payload.
     * @param remoteAddress to send to send the payload to.
     * @return number of bytes sent.
     */
    public int sendTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        final int remaining = buffer.remaining();
        int bytesSent = 0;
        try
        {
            if (null != sendDatagramChannel)
            {
                if (sendDatagramChannel.isOpen())
                {
                    sendHook(buffer, remoteAddress);
                    bytesSent = sendDatagramChannel.send(buffer, remoteAddress);
                }
            }
        }
        catch (final IOException ex)
        {
            sendError(remaining, ex, remoteAddress);
        }

        return bytesSent;
    }

    public InetSocketAddress boundAddress()
    {
        try
        {
            return (InetSocketAddress)receiveDatagramChannel.getLocalAddress();
        }
        catch (final IOException ex)
        {
            return null;
        }
    }

    public static InetSocketAddress getInterfaceAddress(final String interfaceString)
    {
        try
        {
            return InterfaceSearchAddress.parse(interfaceString).getAddress();
        }
        catch (final UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }

    public static InetSocketAddress getInetSocketAddress(final String hostAndPort)
    {
        return SocketAddressParser.parse(
            hostAndPort, CommonContext.ENDPOINT_PARAM_NAME, false, DefaultNameResolver.INSTANCE);
    }
}
