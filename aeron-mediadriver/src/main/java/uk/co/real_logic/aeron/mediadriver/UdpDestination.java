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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.BitUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;

/**
 * Encapsulation of UDP destinations
 * <p>
 * Format of URI:
 * <code>
 * udp://[interface[:port]@]ip:port
 * </code>
 */
public class UdpDestination
{
    private final InetSocketAddress remote;
    private final InetSocketAddress local;
    private final String uriStr;
    private final long consistentHash;

    public static UdpDestination parse(final String destinationUri) throws Exception
    {
        final URI uri = new URI(destinationUri);
        final String userInfo = uri.getUserInfo();

        if (!"udp".equals(uri.getScheme()) || uri.getPort() == -1)
        {
            throw new IllegalArgumentException("malformed destination URI: " + destinationUri);
        }

        final Builder builder = new Builder()
                .uriStr(destinationUri)
                .consistentHash(BitUtil.generateConsistentHash(destinationUri.getBytes()))
                .remotePort(uri.getPort())
                .remoteAddr(InetAddress.getByName(uri.getHost()));

        if (userInfo != null)
        {
            final int colonIndex = userInfo.indexOf(":");

            if (-1 == colonIndex)
            {
                builder.localAddr(InetAddress.getByName(userInfo));
                builder.localPort(0);
            }
            else
            {
                builder.localAddr(InetAddress.getByName(userInfo.substring(0, colonIndex)));
                builder.localPort(Integer.parseInt(userInfo.substring(colonIndex + 1)));
            }
        }

        return new UdpDestination(builder);
    }

    public InetSocketAddress remote()
    {
        return remote;
    }

    public InetSocketAddress local()
    {
        return local;
    }

    public NetworkInterface localInterface() throws Exception
    {
        return NetworkInterface.getByInetAddress(local.getAddress());
    }

    public int localPort()
    {
        return local.getPort();
    }

    public UdpDestination(final Builder builder)
    {
        this.remote = new InetSocketAddress(builder.remoteAddr, builder.remotePort);
        this.local = new InetSocketAddress(builder.localAddr, builder.localPort);
        this.uriStr = builder.uriStr;
        this.consistentHash = builder.consistentHash;
    }

    public long consistentHash()
    {
        return consistentHash;
    }

    public int hashCode()
    {
        return remote.hashCode() + local.hashCode(); // this could cause things to clump slightly
    }

    public boolean equals(Object obj)
    {
        if (null != obj && obj instanceof UdpDestination)
        {
            final UdpDestination rhs = (UdpDestination)obj;

            return rhs.local.equals(this.local) && rhs.remote.equals(this.remote);
        }

        return false;
    }

    public String toString()
    {
        return String.format("udp://%1$s:$2$d@%3$s:%4$d",
                             local.getAddress().getHostAddress(), Integer.valueOf(local.getPort()),
                             remote.getAddress().getHostAddress(), Integer.valueOf(remote.getPort()));
    }

    public static class Builder
    {
        private InetAddress remoteAddr;
        private InetAddress localAddr;
        private int remotePort;
        private int localPort;
        private String uriStr;
        private long consistentHash;

        public Builder()
        {
            this.remoteAddr = null;
            this.localAddr = null;
            this.remotePort = 0;
            this.localPort = 0;
        }

        public Builder uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        public Builder remoteAddr(final InetAddress addr)
        {
            remoteAddr = addr;
            return this;
        }

        public Builder localAddr(final InetAddress addr)
        {
            localAddr = addr;
            return this;
        }

        public Builder remotePort(final int port)
        {
            remotePort = port;
            return this;
        }

        public Builder localPort(final int port)
        {
            localPort = port;
            return this;
        }

        public Builder consistentHash(final long hash)
        {
            consistentHash = hash;
            return this;
        }
    }
}
