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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.security.MessageDigest;

import static uk.co.real_logic.aeron.util.BitUtil.toHex;

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

    public String sha1Hash() throws Exception
    {
        MessageDigest md = MessageDigest.getInstance("SHA-1");

        return toHex(md.digest(uriStr.getBytes()));
    }

    public static class Builder
    {
        private InetAddress remoteAddr;
        private InetAddress localAddr;
        private int remotePort;
        private int localPort;
        private String uriStr;

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
    }
}
