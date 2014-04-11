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

import java.net.*;

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
    private static final int LAST_MULTICAST_DIGIT = 3;

    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;

    private final String uriStr;
    private final long consistentHash;

    public static UdpDestination parse(final String destinationUri) throws Exception
    {
        final URI uri = new URI(destinationUri);
        final String userInfo = uri.getUserInfo();
        final int uriPort = uri.getPort();

        if (!"udp".equals(uri.getScheme()) || uriPort == -1)
        {
            throw new IllegalArgumentException("malformed destination URI: " + destinationUri);
        }

        final Builder builder = new Builder()
                .uriStr(destinationUri)
                .consistentHash(BitUtil.generateConsistentHash(destinationUri.getBytes()));

        final InetAddress hostAddress = InetAddress.getByName(uri.getHost());
        if (hostAddress.isMulticastAddress())
        {
            final byte[] addressAsBytes = hostAddress.getAddress();
            if (BitUtil.isEven(addressAsBytes[LAST_MULTICAST_DIGIT]))
            {
                throw new IllegalArgumentException("Multicast data addresses must be odd");
            }

            addressAsBytes[LAST_MULTICAST_DIGIT]--;
            final InetSocketAddress controlAddress = new InetSocketAddress(InetAddress.getByAddress(addressAsBytes), 0);
            final InetSocketAddress dataAddress = new InetSocketAddress(hostAddress, 0);

            builder.localControlAddress(controlAddress)
                   .remoteControlAddress(controlAddress)
                   .localDataAddress(dataAddress)
                   .remoteDataAddress(dataAddress);
        }
        else
        {
            final InetSocketAddress remoteAddress = new InetSocketAddress(hostAddress, uriPort);
            builder.remoteControlAddress(remoteAddress)
                   .remoteDataAddress(remoteAddress);

            InetSocketAddress localAddress = new InetSocketAddress(0);
            if (userInfo != null)
            {
                final int colonIndex = userInfo.indexOf(":");
                if (colonIndex == -1)
                {
                    localAddress = new InetSocketAddress(InetAddress.getByName(userInfo), 0);
                }
                else
                {
                    final InetAddress specifiedLocalHost = InetAddress.getByName(userInfo.substring(0, colonIndex));
                    final int localPort = Integer.parseInt(userInfo.substring(colonIndex + 1));
                    localAddress = new InetSocketAddress(specifiedLocalHost, localPort);
                }
            }

            builder.localControlAddress(localAddress)
                    .localDataAddress(localAddress);
        }

        return new UdpDestination(builder);
    }

    public InetSocketAddress remoteData()
    {
        return remoteData;
    }

    public InetSocketAddress localData()
    {
        return localData;
    }

    public InetSocketAddress remoteControl()
    {
        return remoteControl;
    }

    public InetSocketAddress localControl()
    {
        return localControl;
    }

    public UdpDestination(final Builder builder)
    {
        this.remoteData = builder.remoteData;
        this.localData = builder.localData;
        this.remoteControl = builder.remoteControl;
        this.localControl = builder.localControl;
        this.uriStr = builder.uriStr;
        this.consistentHash = builder.consistentHash;
    }

    public long consistentHash()
    {
        return consistentHash;
    }

    public int hashCode()
    {
        return remoteData.hashCode() + localData.hashCode(); // this could cause things to clump slightly
    }

    public boolean equals(Object obj)
    {
        if (null != obj && obj instanceof UdpDestination)
        {
            final UdpDestination rhs = (UdpDestination)obj;

            return rhs.localData.equals(this.localData) && rhs.remoteData.equals(this.remoteData);
        }

        return false;
    }

    public String toString()
    {
        return String.format("udp://%1$s:%2$d@%3$s:%4$d",
                localData.getAddress().getHostAddress(), Integer.valueOf(localData.getPort()),
                remoteData.getAddress().getHostAddress(), Integer.valueOf(remoteData.getPort()));
    }

    public boolean isMulticast()
    {
        return remoteData.getAddress().isMulticastAddress();
    }

    public NetworkInterface localDataInterface() throws SocketException
    {
        return NetworkInterface.getByInetAddress(localData.getAddress());
    }

    public static class Builder
    {
        private InetSocketAddress remoteData;
        private InetSocketAddress localData;
        private InetSocketAddress remoteControl;
        private InetSocketAddress localControl;
        private String uriStr;
        private long consistentHash;

        public Builder()
        {

        }

        public Builder uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        public Builder consistentHash(final long hash)
        {
            consistentHash = hash;
            return this;
        }

        public Builder remoteDataAddress(final InetSocketAddress remoteData)
        {
            this.remoteData = remoteData;
            return this;
        }

        public Builder localDataAddress(final InetSocketAddress localData)
        {
            this.localData = localData;
            return this;
        }

        public Builder remoteControlAddress(final InetSocketAddress remoteControl)
        {
            this.remoteControl = remoteControl;
            return this;
        }

        public Builder localControlAddress(final InetSocketAddress localControl)
        {
            this.localControl = localControl;
            return this;
        }
    }
}
