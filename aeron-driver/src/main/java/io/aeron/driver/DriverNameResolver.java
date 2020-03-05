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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.media.UdpNameResolutionTransport;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.DriverNameResolverCache.byteSubsetEquals;
import static io.aeron.protocol.ResolutionEntryFlyweight.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class DriverNameResolver implements AutoCloseable, UdpNameResolutionTransport.UdpFrameHandler, NameResolver
{
    // TODO: make these configurable
    private static final long SELF_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long NEIGHBOR_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long DUTY_CYCLE_INTERVAL_MS = 10;

    public static final int NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 14;
    public static final int NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 15;

    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight(unsafeBuffer);
    private final ResolutionEntryFlyweight resolutionEntryFlyweight = new ResolutionEntryFlyweight();
    private final ArrayList<Neighbor> neighborList = new ArrayList<>();

    private final UdpNameResolutionTransport transport;
    private final DriverNameResolverCache cache;
    private final NameResolver delegateResolver;
    private final AtomicCounter invalidPackets;
    private final AtomicCounter neighborsCounter;
    private final AtomicCounter cacheEntriesCounter;
    private final byte[] nameTempBuffer = new byte[ResolutionEntryFlyweight.MAX_NAME_LENGTH];
    private final byte[] addressTempBuffer = new byte[ResolutionEntryFlyweight.ADDRESS_LENGTH_IP6];

    private final byte[] localName;
    private final String localDriverName;
    private InetSocketAddress localSocketAddress;
    private byte[] localAddress;

    private final String bootstrapNeighbor;
    private InetSocketAddress bootstrapNeighborAddr;
    private long timeOfLastBootstrapNeighborResolveMs;

    private final long neighborTimeoutMs;
    private final long selfResolutionIntervalMs;
    private final long neighborResolutionIntervalMs;
    private final int mtuLength;
    private final boolean preferIPv6 = false;

    private long timeOfLastWorkMs = 0;
    private long deadlineSelfResolutionMs;
    private long deadlineNeighborResolutionMs;

    DriverNameResolver(
        final String name,
        final String resolverInterface,
        final String bootstrapNeighbor,
        final MediaDriver.Context context)
    {
        this.neighborTimeoutMs = TIMEOUT_MS;
        this.selfResolutionIntervalMs = SELF_RESOLUTION_INTERVAL_MS;
        this.neighborResolutionIntervalMs = NEIGHBOR_RESOLUTION_INTERVAL_MS;
        this.mtuLength = context.mtuLength();
        invalidPackets = context.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);
        delegateResolver = context.nameResolver();

        final long nowMs = context.epochClock().time();

        this.bootstrapNeighbor = bootstrapNeighbor;
        bootstrapNeighborAddr = null == bootstrapNeighbor ?
            null : UdpNameResolutionTransport.getInetSocketAddress(bootstrapNeighbor);
        timeOfLastBootstrapNeighborResolveMs = nowMs;

        localSocketAddress = null != resolverInterface ?
            UdpNameResolutionTransport.getInterfaceAddress(resolverInterface) :
            new InetSocketAddress("0.0.0.0", 0);

        final String driverName = null != name ? name : getCanonicalName();

        localDriverName = driverName;
        localName = driverName.getBytes(StandardCharsets.US_ASCII);
        localAddress = localSocketAddress.getAddress().getAddress();

        deadlineSelfResolutionMs = 0;
        deadlineNeighborResolutionMs = nowMs + this.neighborResolutionIntervalMs;

        cache = new DriverNameResolverCache(TIMEOUT_MS);

        final UdpChannel placeholderChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:8050");

        transport = new UdpNameResolutionTransport(placeholderChannel, localSocketAddress, unsafeBuffer, context);

        neighborsCounter = context.countersManager().newCounter(
            "Resolver neighbors", NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID);
        cacheEntriesCounter = context.countersManager().newCounter(
            "Resolver cache entries: name " + localDriverName, NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID);
    }

    public void close()
    {
        CloseHelper.closeAll(transport, cache);
    }

    public void openDatagramChannel()
    {
        transport.openDatagramChannel(null);

        final InetSocketAddress boundAddress = transport.boundAddress();
        if (null != boundAddress)
        {
            localSocketAddress = boundAddress;
            localAddress = boundAddress.getAddress().getAddress();

            final StringBuilder builder = new StringBuilder(": bound ");
            builder.append(transport.bindAddressAndPort());

            if (null != bootstrapNeighborAddr)
            {
                builder
                    .append(" bootstrap ")
                    .append(bootstrapNeighborAddr.getHostString())
                    .append(':')
                    .append(bootstrapNeighborAddr.getPort());
            }

            neighborsCounter.appendToLabel(builder.toString());
        }
    }

    public int doWork(final long nowMs)
    {
        int workCount = 0;

        if ((timeOfLastWorkMs + DUTY_CYCLE_INTERVAL_MS) < nowMs)
        {
            workCount += transport.poll(this, nowMs);
            workCount += cache.timeoutOldEntries(nowMs, cacheEntriesCounter);
            workCount += timeoutNeighbors(nowMs);

            if (nowMs > deadlineSelfResolutionMs)
            {
                sendSelfResolutions(nowMs);
            }

            if (nowMs > deadlineNeighborResolutionMs)
            {
                sendNeighborResolutions(nowMs);
            }

            timeOfLastWorkMs = nowMs;
        }

        return workCount;
    }

    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        DriverNameResolverCache.CacheEntry entry;

        if (preferIPv6)
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP6_MD);
            if (null == entry)
            {
                entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
            }
        }
        else
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
        }

        try
        {
            if (null == entry)
            {
                if (name.equals(localDriverName))
                {
                    return localSocketAddress.getAddress();
                }

                return delegateResolver.resolve(name, uriParamName, isReResolution);
            }

            return InetAddress.getByAddress(entry.address);
        }
        catch (final UnknownHostException ex)
        {
            return null;
        }
    }

    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        // here we would lookup advertised endpoints/control IP:port pairs by name. Currently, we just return delegate.
        return delegateResolver.lookup(name, uriParamName, isReLookup);
    }

    public int timeoutNeighbors(final long nowMs)
    {
        int workCount = 0;

        final ArrayList<Neighbor> neighborList = this.neighborList;
        for (int lastIndex = neighborList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Neighbor neighbor = neighborList.get(i);

            if (nowMs > (neighbor.timeOfLastActivityMs + neighborTimeoutMs))
            {
                ArrayListUtil.fastUnorderedRemove(neighborList, i, lastIndex--);
                workCount++;
            }
        }

        neighborsCounter.setOrdered(neighborList.size());

        return workCount;
    }

    public void sendSelfResolutions(final long nowMs)
    {
        byteBuffer.clear();

        final int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;
        final byte resType = preferIPv6 ? RES_TYPE_NAME_TO_IP6_MD : RES_TYPE_NAME_TO_IP4_MD;

        headerFlyweight
            .headerType(HeaderFlyweight.HDR_TYPE_RES)
            .flags((short)0)
            .version(HeaderFlyweight.CURRENT_VERSION);

        resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
        resolutionEntryFlyweight
            .resType(resType)
            .flags(SELF_FLAG)
            .udpPort((short)localSocketAddress.getPort())
            .ageInMs(0)
            .putAddress(localAddress)
            .putName(localName);

        final int length = resolutionEntryFlyweight.entryLength() + MIN_HEADER_LENGTH;
        headerFlyweight.frameLength(length);

        byteBuffer.limit(length);

        if (neighborList.size() == 0 && null != bootstrapNeighborAddr)
        {
            if (nowMs > (timeOfLastBootstrapNeighborResolveMs + TIMEOUT_MS))
            {
                bootstrapNeighborAddr = UdpNameResolutionTransport.getInetSocketAddress(bootstrapNeighbor);
                timeOfLastBootstrapNeighborResolveMs = nowMs;
            }

            sendResolutionFrameTo(byteBuffer, bootstrapNeighborAddr);
        }
        else
        {
            for (int i = 0, size = neighborList.size(); i < size; i++)
            {
                final Neighbor neighbor = neighborList.get(i);
                sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
            }
        }

        deadlineSelfResolutionMs = nowMs + selfResolutionIntervalMs;
    }

    public void sendNeighborResolutions(final long nowMs)
    {
        final DriverNameResolverCache.Iterator iter = cache.resetIterator();

        while (iter.hasNext())
        {
            byteBuffer.clear();

            int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;

            headerFlyweight
                .headerType(HeaderFlyweight.HDR_TYPE_RES)
                .flags((short)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

            while (iter.hasNext())
            {
                final DriverNameResolverCache.CacheEntry entry = iter.next();

                if (currentOffset + entryLengthRequired(entry.type, entry.name.length) > mtuLength)
                {
                    iter.rewindNext();
                    break;
                }

                resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
                resolutionEntryFlyweight
                    .resType(entry.type)
                    .flags((short)0)
                    .udpPort((short)entry.port)
                    .ageInMs((int)(nowMs - entry.timeOfLastActivityMs))
                    .putAddress(entry.address)
                    .putName(entry.name);

                final int length = resolutionEntryFlyweight.entryLength();
                currentOffset += length;
            }

            headerFlyweight.frameLength(currentOffset);
            byteBuffer.limit(currentOffset);

            for (int i = 0, size = neighborList.size(); i < size; i++)
            {
                final Neighbor neighbor = neighborList.get(i);
                sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
            }
        }

        deadlineNeighborResolutionMs = nowMs + neighborResolutionIntervalMs;
    }

    public int sendResolutionFrameTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        //System.out.println("out " + transport.bindAddressAndPort() + " " + remoteAddress);
        buffer.position(0);
        return transport.sendTo(buffer, remoteAddress);
    }

    public int onFrame(
        final UnsafeBuffer unsafeBuffer,
        final int length,
        final InetSocketAddress srcAddress,
        final long nowMs)
    {
        if (headerFlyweight.headerType() == HDR_TYPE_RES)
        {
            int offset = MIN_HEADER_LENGTH;

            while (length > offset)
            {
                resolutionEntryFlyweight.wrap(unsafeBuffer, offset, length - offset);

                if ((length - offset) < resolutionEntryFlyweight.entryLength())
                {
                    invalidPackets.increment();
                    return 0;
                }

                onResolutionEntry(resolutionEntryFlyweight, srcAddress, nowMs);

                offset += resolutionEntryFlyweight.entryLength();
            }

            return length;
        }

        return 0;
    }

    void onResolutionEntry(
        final ResolutionEntryFlyweight resolutionEntry, final InetSocketAddress srcAddress, final long nowMs)
    {
        final byte resType = resolutionEntry.resType();
        final boolean isSelf = SELF_FLAG == resolutionEntryFlyweight.flags();
        byte[] addr = addressTempBuffer;

        final int addressLength = resolutionEntryFlyweight.getAddress(addressTempBuffer);
        if (isSelf && ResolutionEntryFlyweight.isIp4Wildcard(addressTempBuffer, addressLength))
        {
            addr = srcAddress.getAddress().getAddress();
        }

        final int nameLength = resolutionEntryFlyweight.getName(nameTempBuffer);
        final long timeOfLastActivity = nowMs - resolutionEntryFlyweight.ageInMs();
        final int port = resolutionEntryFlyweight.udpPort();

        // use name and port to indicate it is from this resolver instead of searching interfaces
        if (port == localSocketAddress.getPort() && byteSubsetEquals(nameTempBuffer, localName, nameLength))
        {
            return;
        }

        cache.addOrUpdateEntry(
            nameTempBuffer, nameLength, timeOfLastActivity, resType, addr, port, cacheEntriesCounter);

        final int neighborIndex = findNeighborByAddress(addr, addressLength, port);
        if (-1 == neighborIndex)
        {
            final byte[] neighborAddress = Arrays.copyOf(addr, addressLength);

            try
            {
                neighborList.add(new Neighbor(new InetSocketAddress(
                    InetAddress.getByAddress(neighborAddress), port), timeOfLastActivity));
                neighborsCounter.setOrdered(neighborList.size());
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else if (isSelf)
        {
            neighborList.get(neighborIndex).timeOfLastActivityMs = timeOfLastActivity;
        }
    }

    int findNeighborByAddress(final InetSocketAddress address)
    {
        for (int i = 0, size = neighborList.size(); i < size; i++)
        {
            if (address.equals(neighborList.get(i).socketAddress))
            {
                return i;
            }
        }

        return -1;
    }

    int findNeighborByAddress(final byte[] address, final int addressLength, final int port)
    {
        for (int i = 0, size = neighborList.size(); i < size; i++)
        {
            final InetSocketAddress socketAddress = neighborList.get(i).socketAddress;

            if (byteSubsetEquals(address, socketAddress.getAddress().getAddress(), addressLength) &&
                port == socketAddress.getPort())
            {
                return i;
            }
        }

        return -1;
    }

    public static String getCanonicalName()
    {
        String canonicalName = null;

        try
        {
            canonicalName = InetAddress.getLocalHost().getHostName();
        }
        catch (final UnknownHostException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return canonicalName;
    }

    static class Neighbor
    {
        InetSocketAddress socketAddress;
        long timeOfLastActivityMs;

        Neighbor(final InetSocketAddress socketAddress, final long nowMs)
        {
            this.socketAddress = socketAddress;
            this.timeOfLastActivityMs = nowMs;
        }
    }
}
