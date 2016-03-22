/*
 * Copyright 2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.agent;

import uk.co.real_logic.aeron.driver.event.EventCode;
import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint;
import uk.co.real_logic.aeron.driver.media.SendChannelEndpoint;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class ChannelEndpointInterceptor
{
    public static class SenderProxyInterceptor
    {
        public static void registerSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
        {
            EventLogger.LOGGER.logChannelCreated(
                EventCode.SEND_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
        }

        public static void closeSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
        {
            EventLogger.LOGGER.logChannelCreated(
                EventCode.SEND_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
        }
    }

    public static class ReceiverProxyInterceptor
    {
        public static void registerReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
        {
            EventLogger.LOGGER.logChannelCreated(
                EventCode.RECEIVE_CHANNEL_CREATION, channelEndpoint.udpChannel().description());
        }

        public static void closeReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
        {
            EventLogger.LOGGER.logChannelCreated(
                EventCode.RECEIVE_CHANNEL_CLOSE, channelEndpoint.udpChannel().description());
        }
    }

    public static class SendChannelEndpointInterceptor
    {
        public static void presend(final ByteBuffer buffer, final InetSocketAddress address)
        {
            EventLogger.LOGGER.logFrameOut(buffer, address);
        }

        public static void dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
        {
            EventLogger.LOGGER.logFrameIn(buffer, 0, length, srcAddress);
        }
    }

    public static class ReceiveChannelEndpointInterceptor
    {
        public static void sendTo(
            final ByteBuffer buffer,
            final InetSocketAddress address)
        {
            EventLogger.LOGGER.logFrameOut(buffer, address);
        }

        public static void dispatch(final UnsafeBuffer buffer, final int length, final InetSocketAddress srcAddress)
        {
            EventLogger.LOGGER.logFrameIn(buffer, 0, length, srcAddress);
        }
    }
}
