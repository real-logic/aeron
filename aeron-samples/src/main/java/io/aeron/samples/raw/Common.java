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
package io.aeron.samples.raw;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;

/**
 * Common configuration and functions used across raw samples.
 */
public class Common
{
    /**
     * Number of message to exchange.
     */
    public static final int NUM_MESSAGES = 10_000;

    /**
     * UDP port on which Pong will listen.
     */
    public static final int PONG_PORT = 20123;

    /**
     * UDP port on which Ping will listen.
     */
    public static final int PING_PORT = 20124;

    /**
     * Address to send ping messages to. Pong should listen to this address.
     */
    public static final String PING_DEST = System.getProperty("io.aeron.raw.ping.dest", "localhost");

    /**
     * Address to send pong messages to. Ping should listen to this address
     */
    public static final String PONG_DEST = System.getProperty("io.aeron.raw.pong.dest", "localhost");

    static void init(final DatagramChannel channel) throws IOException
    {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }

    static void init(final DatagramChannel channel, final InetSocketAddress sendAddress) throws IOException
    {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        channel.connect(sendAddress);
    }
}
