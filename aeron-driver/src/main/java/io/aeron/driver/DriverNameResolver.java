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

    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight(unsafeBuffer);
    private final ResolutionEntryFlyweight resolutionEntryFlyweight = new ResolutionEntryFlyweight();
    private final ArrayList<Neighbor> neighborList = new ArrayList<>();

    private final UdpNameResolutionTransport transport;
    private final DriverNameResolverCache cache;
    private final AtomicCounter invalidPackets;
    private final byte[] nameTempBuffer = new byte[ResolutionEntryFlyweight.MAX_NAME_LENGTH];
    private final byte[] addressTempBuffer = new byte[ResolutionEntryFlyweight.ADDRESS_LENGTH_IP6];

    private final InetSocketAddress localSocketAddress;
    private final byte[] localName;
    private final byte[] localAddress;

    private final long neighborTimeoutMs;
    private final long selfResolutionIntervalMs;
    private final long neighborResolutionIntervalMs;
    private final int mtuLength;
    private final boolean preferIPv6 = false;

    private long deadlineSelfResolutionMs;
    private long deadlineNeighborResolutionMs;

    DriverNameResolver(
        final String name,
        final InetSocketAddress resolverAddr,
        final MediaDriver.Context context)
    {
        this.neighborTimeoutMs = TIMEOUT_MS;
        this.selfResolutionIntervalMs = SELF_RESOLUTION_INTERVAL_MS;
        this.neighborResolutionIntervalMs = NEIGHBOR_RESOLUTION_INTERVAL_MS;
        this.mtuLength = context.mtuLength();
        invalidPackets = context.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);

        localSocketAddress = resolverAddr;
        localName = name.getBytes(StandardCharsets.US_ASCII);
        localAddress = resolverAddr.getAddress().getAddress();

        final long nowMs = context.epochClock().time();
        deadlineSelfResolutionMs = nowMs;
        deadlineNeighborResolutionMs = nowMs + this.neighborResolutionIntervalMs;

        cache = new DriverNameResolverCache(TIMEOUT_MS);
        transport = new UdpNameResolutionTransport(resolverAddr, unsafeBuffer, context);
    }

    public void close()
    {
        CloseHelper.closeAll(transport, cache);
    }

    public int doWork(final long nowMs)
    {
        int workCount = 0;

        workCount += transport.poll(this, nowMs);
        workCount += cache.timeoutOldEntries(nowMs);
        workCount += timeoutNeighbors(nowMs);

        if (nowMs > deadlineSelfResolutionMs)
        {
            sendSelfResolutions(nowMs);
        }

        if (nowMs > deadlineNeighborResolutionMs)
        {
            sendNeighborResolutions(nowMs);
        }

        return workCount;
    }

    public InetAddress resolve(final CharSequence name, final String uriParamName, final boolean isReResolution)
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
                return InetAddress.getByName(name.toString());
            }

            return InetAddress.getByAddress(entry.address);
        }
        catch (final UnknownHostException ex)
        {
            return null;
        }
    }

    public CharSequence lookup(final CharSequence name, final String uriParamName, final boolean isReLookup)
    {
        // here we would lookup advertised endpoints/control IP:port pairs by name. Currently, we just return name.
        return name;
    }

    public int timeoutNeighbors(final long nowMs)
    {
        int workCount = 0;

        for (int lastIndex = neighborList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Neighbor neighbor = neighborList.get(i);

            if (nowMs > neighbor.deadlineMs)
            {
                ArrayListUtil.fastUnorderedRemove(neighborList, i, lastIndex--);
                workCount++;
            }
        }

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
        for (final Neighbor neighbor : neighborList)
        {
            sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
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
            final byte resType = preferIPv6 ? RES_TYPE_NAME_TO_IP6_MD : RES_TYPE_NAME_TO_IP4_MD;

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

                final int length = resolutionEntryFlyweight.entryLength() + MIN_HEADER_LENGTH;
                currentOffset += length;
            }

            headerFlyweight.frameLength(currentOffset);
            byteBuffer.limit(currentOffset);

            for (final Neighbor neighbor : neighborList)
            {
                sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
            }
        }

        deadlineNeighborResolutionMs = nowMs + neighborResolutionIntervalMs;
    }

    public int sendResolutionFrameTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
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
        final short port = resolutionEntryFlyweight.udpPort();

        cache.addOrUpdateEntry(nameTempBuffer, nameLength, timeOfLastActivity, resType, addr, port);

        final int neighborIndex = findNeighborByAddress(addr, addressLength, port);
        if (-1 == neighborIndex)
        {
            final byte[] neighborAddress = Arrays.copyOf(addr, addressLength);

            try
            {
                neighborList.add(new Neighbor(new InetSocketAddress(
                    InetAddress.getByAddress(neighborAddress), port), nowMs, nowMs + neighborTimeoutMs));
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else if (isSelf)
        {
            neighborList.get(neighborIndex).timeOfLastActivityMs = nowMs;
            neighborList.get(neighborIndex).deadlineMs = nowMs + neighborTimeoutMs;
        }
    }

    int findNeighborByAddress(final InetSocketAddress address)
    {
        for (int i = 0; i < neighborList.size(); i++)
        {
            if (address.equals(neighborList.get(i).socketAddress))
            {
                return i;
            }
        }

        return -1;
    }

    int findNeighborByAddress(final byte[] address, final int addressLength, final short port)
    {
        for (int i = 0; i < neighborList.size(); i++)
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
        long deadlineMs;

        Neighbor(final InetSocketAddress socketAddress, final long nowMs, final long deadlineMs)
        {
            this.socketAddress = socketAddress;
            this.timeOfLastActivityMs = nowMs;
            this.deadlineMs = nowMs;
        }
    }
}
