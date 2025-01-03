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
package io.aeron.samples.stress;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.Random;

import static io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH;
import static io.aeron.samples.stress.StressUtil.MTU_LENGTHS;
import static io.aeron.samples.stress.StressUtil.UNICAST_STREAM_ID;
import static io.aeron.samples.stress.StressUtil.clientAddress;
import static io.aeron.samples.stress.StressUtil.info;
import static io.aeron.samples.stress.StressUtil.serverAddress;
import static io.aeron.samples.stress.StressUtil.unicastReqChannel;
import static io.aeron.samples.stress.StressUtil.unicastRspChannel;
import static io.aeron.samples.stress.StressUtil.validateMessage;
import static java.lang.Boolean.TRUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Client to stress to aeron driver with varying message sizes and MTU.
 */
public class StressUnicastClient implements Agent
{
    private static final long TIMEOUT_MS = 5_000;
    private final String serverAddress;
    private final String clientAddress;
    private final EpochClock clock;
    private final MutableDirectBuffer msg;
    private final Long2ObjectHashMap<Boolean> inflightMessages = new Long2ObjectHashMap<>();
    private final FragmentAssembler unicastRspAssembler = new FragmentAssembler(this::unicastRspHandler);
    private final int maxInflight;
    private final int totalToSend;
    private final int mtu;
    private final byte[] buffer = new byte[2 * MAX_UDP_PAYLOAD_LENGTH];
    private int messageLength;
    private final CRC64 crc = new CRC64();

    private Aeron aeron;
    private Publication unicastPublication;
    private Subscription unicastSubscription;
    private long correlationId = 0;
    private long lastMessageSent = 0;

    /**
     * Construct Stress Client.
     *
     * @param serverAddress server address to connect to.
     * @param clientAddress local address to get responses.
     * @param clock         for timing.
     * @param maxInflight   maximum number of messages in flight.
     * @param totalToSend   total number of messages to send.
     * @param mtu           the mtu to use.
     */
    public StressUnicastClient(
        final String serverAddress,
        final String clientAddress,
        final EpochClock clock,
        final int maxInflight,
        final int totalToSend,
        final int mtu)
    {
        this.serverAddress = serverAddress;
        this.clientAddress = clientAddress;
        this.clock = clock;
        this.maxInflight = maxInflight;
        this.totalToSend = totalToSend;
        this.mtu = mtu;

        final Random r = new Random(42);
        r.nextBytes(buffer);
        msg = new UnsafeBuffer(buffer);
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        info("server=" + serverAddress + ", client=" + clientAddress);
        aeron = Aeron.connect(new Aeron.Context());
        info("Connected to Aeron dir=" + aeron.context().aeronDirectoryName());

        unicastPublication = aeron.addExclusivePublication(
            unicastReqChannel(serverAddress).mtu(mtu).linger(0L).build(), UNICAST_STREAM_ID);
        unicastSubscription = aeron.addSubscription(unicastRspChannel(clientAddress).build(), UNICAST_STREAM_ID);

        info("publications and subscriptions created");
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        if (0 == messageLength)
        {
            throw new IllegalStateException("messageLength has not been set");
        }

        int sendCount = 0;
        while (inflightMessages.size() < maxInflight && correlationId < totalToSend &&
            0 < unicastPublication.offer(msg, 0, messageLength, this::currentCorrelationId))
        {
            inflightMessages.put(correlationId, TRUE);
            correlationId++;
            sendCount++;
            lastMessageSent = clock.time();
        }

        int recvCount = 0;
        int count;
        while (0 != (count = unicastSubscription.poll(unicastRspAssembler, maxInflight)))
        {
            recvCount += count;
            // Spin
        }

        if (0 < correlationId && 0 == recvCount && !inflightMessages.isEmpty())
        {
            final long timeSinceLastMessageMs = clock.time() - lastMessageSent;
            if (TIMEOUT_MS < timeSinceLastMessageMs)
            {
                throw new RuntimeException("No response received for " + timeSinceLastMessageMs + "ms, client=" + this);
            }
        }

        return sendCount;
    }

    private boolean isComplete()
    {
        return totalToSend <= correlationId && inflightMessages.isEmpty();
    }

    private void unicastRspHandler(
        final DirectBuffer msg,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = header.reservedValue();
        validateMessage(crc, msg, offset, length, correlationId);
        inflightMessages.remove(correlationId);
    }

    private long currentCorrelationId(final DirectBuffer message, final int offset, final int length)
    {
        return correlationId;
    }

    void reset(final int messageLength)
    {
        this.messageLength = messageLength;
        this.correlationId = 0;
        final int payloadLength = messageLength - BitUtil.SIZE_OF_LONG;
        final long crcValue = crc.recalculate(buffer, BitUtil.SIZE_OF_LONG, payloadLength);
        msg.putLong(0, crcValue, LITTLE_ENDIAN);
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        CloseHelper.quietCloseAll(aeron);
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StressClient{" +
            "inflightMessages=" + inflightMessages +
            ", mtu=" + mtu +
            ", messageLength=" + messageLength +
            ", correlationId=" + correlationId +
            ", lastMessageSent=" + lastMessageSent +
            '}';
    }

    /**
     * Entry point.
     *
     * @param args command line arguments.
     */
    public static void main(final String[] args)
    {
        final IdleStrategy idleStrategy = new YieldingIdleStrategy();
        final int maxMessageLength = 2 * MAX_UDP_PAYLOAD_LENGTH;
        final int totalToSend = 100;

        for (final int mtu : MTU_LENGTHS)
        {
            final StressUnicastClient stressClient = new StressUnicastClient(
                serverAddress(), clientAddress(), new SystemEpochClock(), 20, totalToSend, mtu);
            stressClient.onStart();

            try
            {
                for (int messageLength = 32; messageLength <= maxMessageLength; messageLength += 1024)
                {
                    stressClient.reset(messageLength);

                    while (!stressClient.isComplete())
                    {
                        idleStrategy.idle(stressClient.doWork());
                    }

                }

                info("Complete mtu=" + mtu);
            }
            finally
            {
                stressClient.onClose();
            }
        }
    }
}
