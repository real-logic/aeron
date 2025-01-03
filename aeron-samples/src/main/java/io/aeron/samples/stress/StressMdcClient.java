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
import io.aeron.Counter;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongCounterMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.Random;

import static io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH;
import static io.aeron.samples.stress.StressUtil.MDC_STREAM_ID;
import static io.aeron.samples.stress.StressUtil.MTU_LENGTHS;
import static io.aeron.samples.stress.StressUtil.clientAddress;
import static io.aeron.samples.stress.StressUtil.info;
import static io.aeron.samples.stress.StressUtil.mdcReqPubChannel;
import static io.aeron.samples.stress.StressUtil.serverAddress;
import static io.aeron.samples.stress.StressUtil.validateMessage;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Client to stress to aeron driver with varying message sizes and MTU.
 */
public class StressMdcClient implements Agent
{
    private static final long TIMEOUT_MS = 5_000;
    private static final int EXPECTED_RESPONSE_COUNT = 4;
    private final String serverAddress;
    private final String clientAddress;
    private final EpochClock clock;
    private final MutableDirectBuffer msg;
    private final Long2LongCounterMap inflightMessages = new Long2LongCounterMap(-1);
    private final FragmentAssembler mdcRspAssembler1 = new FragmentAssembler(this::mdcRspHandler);
    private final FragmentAssembler mdcRspAssembler2 = new FragmentAssembler(this::mdcRspHandler);
    private final int maxInflight;
    private final int totalToSend;
    private final int mtu;
    private final byte[] buffer = new byte[2 * MAX_UDP_PAYLOAD_LENGTH];
    private final CRC64 crc = new CRC64();

    private Aeron aeron;
    private Publication mdcPublication;
    private Subscription mdcSubscription1;
    private Subscription mdcSubscription2;
    private long correlationId = 0;
    private long lastMessageSent = 0;
    private int messageLength;
    private Counter clientReceiveCount;
    private Counter clientSendCount;

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
    public StressMdcClient(
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
        aeron = Aeron.connect(new Aeron.Context());

        mdcPublication = aeron.addExclusivePublication(
            mdcReqPubChannel(clientAddress).mtu(mtu).linger(0L).build(), MDC_STREAM_ID);

        mdcSubscription1 = aeron.addSubscription(
            StressUtil.mdcRspSubChannel1(serverAddress, clientAddress).build(),
            MDC_STREAM_ID,
            StressUtil::imageAvailable,
            StressUtil::imageUnavailable);
        mdcSubscription2 = aeron.addSubscription(
            StressUtil.mdcRspSubChannel2(serverAddress, clientAddress).build(),
            MDC_STREAM_ID,
            StressUtil::imageAvailable,
            StressUtil::imageUnavailable);

        clientReceiveCount = aeron.addCounter(StressUtil.CLIENT_RECV_COUNT, "Client Receive Count");
        clientSendCount = aeron.addCounter(StressUtil.CLIENT_SEND_COUNT, "Client Send Count");
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        if (!mdcSubscription1.isConnected() || !mdcSubscription2.isConnected())
        {
            return 0;
        }

        if (0 == messageLength)
        {
            throw new IllegalStateException("messageLength has not been set");
        }

        int sendCount = 0;
        while (inflightMessages.size() < maxInflight && correlationId < totalToSend &&
            0 < mdcPublication.offer(msg, 0, messageLength, this::currentCorrelationId))
        {
            inflightMessages.put(correlationId, 0);
            correlationId++;
            sendCount++;
            lastMessageSent = clock.time();

            clientSendCount.increment();
        }

        int recvCount = 0;
        int count;
        while (0 != (count = poll(maxInflight)))
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

    private int poll(final int pollLimit)
    {
        int count = 0;
        count += mdcSubscription1.poll(mdcRspAssembler1, pollLimit);
        count += mdcSubscription2.poll(mdcRspAssembler2, pollLimit);
        return count;
    }

    private boolean isComplete()
    {
        return totalToSend <= correlationId && inflightMessages.isEmpty();
    }

    private void mdcRspHandler(
        final DirectBuffer msg,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = header.reservedValue();

        validateMessage(crc, msg, offset, length, correlationId);

        clientReceiveCount.increment();

        final long responseCount = inflightMessages.incrementAndGet(correlationId);
        if (EXPECTED_RESPONSE_COUNT <= responseCount)
        {
            inflightMessages.remove(correlationId);
        }
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

        final int startMessageLength = 32;
        final int maxMessageLength = 2 * MAX_UDP_PAYLOAD_LENGTH;
        final int totalToSend = 100;

        for (final int mtu : MTU_LENGTHS)
        {
            final StressMdcClient client = new StressMdcClient(
                serverAddress(), clientAddress(), new SystemEpochClock(), 20, totalToSend, mtu);

            try
            {
                client.onStart();

                for (int messageLength = startMessageLength; messageLength <= maxMessageLength; messageLength += 1024)
                {
                    client.reset(messageLength);

                    while (!client.isComplete())
                    {
                        idleStrategy.idle(client.doWork());
                    }
                }

                info("Complete mtu=" + mtu);
            }
            finally
            {
                client.onClose();
            }
        }
    }
}
