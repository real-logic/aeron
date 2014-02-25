/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.iodriver;

import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

/**
 * Transport abstraction for UDP.
 *
 * Holds DatagramChannel, parsers, etc.
 */
public final class UDPTransport implements ReadHandler
{
    public static final int BYTE_BUFFER_SZ = 4096; // TODO: this needs to be configured in some way

    private ByteBuffer readByteBuffer;
    private DirectBuffer readBuffer;
    private HeaderFlyweight header;
    private DatagramChannel channel;

    UDPTransport() throws Exception
    {
        this.readByteBuffer = ByteBuffer.allocateDirect(BYTE_BUFFER_SZ);
        this.readBuffer = new DirectBuffer(this.readByteBuffer);
        this.header = new HeaderFlyweight();
        this.channel = DatagramChannel.open();
    }

    public void bind(SocketAddress local, EventLoop loop) throws Exception
    {
        channel.bind(local);
        loop.registerForRead(channel, this);
        // TODO: save state so we know we are listening.
    }

    public void connect(SocketAddress remote, EventLoop loop) throws Exception
    {
        channel.connect(remote);
        loop.registerForRead(channel, this);
        // TODO: save state so we know we are connected.
    }

    @Override
    public void handleRead() throws Exception
    {
        readByteBuffer.clear();
        final SocketAddress srcAddr = channel.receive(readByteBuffer);

        header.reset(readBuffer);

        // TODO: ver check
        // TODO: session Id check

        // header type dispatch
        switch (header.headerType())
        {
            case HeaderFlyweight.HDR_TYPE_CONN:
                break;
            case HeaderFlyweight.HDR_TYPE_DATA:
                break;
            case HeaderFlyweight.HDR_TYPE_NAK:
                break;
            case HeaderFlyweight.HDR_TYPE_FCR:
                break;
            case HeaderFlyweight.HDR_TYPE_EXT:
                break;
        }
    }
}
