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
package io.aeron.samples.cluster.tutorial;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;

import java.util.Objects;

/**
 * Auction service implementing the business logic.
 */
// tag::new_service[]
public class BasicAuctionClusteredService implements ClusteredService
// end::new_service[]
{
    static final int CORRELATION_ID_OFFSET = 0;
    static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int BID_MESSAGE_LENGTH = PRICE_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
    static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + BitUtil.SIZE_OF_BYTE;

    static final int SNAPSHOT_CUSTOMER_ID_OFFSET = 0;
    static final int SNAPSHOT_PRICE_OFFSET = SNAPSHOT_CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    static final int SNAPSHOT_MESSAGE_LENGTH = SNAPSHOT_PRICE_OFFSET + BitUtil.SIZE_OF_LONG;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableArrayBuffer();
    private final MutableDirectBuffer snapshotBuffer = new ExpandableArrayBuffer();

    // tag::state[]
    private final Auction auction = new Auction();
    // end::state[]
    private Cluster cluster;
    private IdleStrategy idleStrategy;

    /**
     * {@inheritDoc}
     */
    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;                      // <1>
        this.idleStrategy = cluster.idleStrategy();  // <2>

        if (null != snapshotImage)                   // <3>
        {
            loadSnapshot(cluster, snapshotImage);
        }
    }
    // end::start[]

    /**
     * {@inheritDoc}
     */
    // tag::message[]
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);                   // <1>
        final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long price = buffer.getLong(offset + PRICE_OFFSET);

        final boolean bidSucceeded = auction.attemptBid(price, customerId);                          // <2>

        if (null != session)                                                                         // <3>
        {
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);                       // <4>
            egressMessageBuffer.putLong(CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId());
            egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice());
            egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, bidSucceeded ? (byte)1 : (byte)0);

            idleStrategy.reset();
            while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0)                 // <5>
            {
                idleStrategy.idle();                                                                 // <6>
            }
        }
    }
    // end::message[]

    /**
     * {@inheritDoc}
     */
    // tag::takeSnapshot[]
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        snapshotBuffer.putLong(SNAPSHOT_CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId());  // <1>
        snapshotBuffer.putLong(SNAPSHOT_PRICE_OFFSET, auction.getBestPrice());

        idleStrategy.reset();
        while (snapshotPublication.offer(snapshotBuffer, 0, SNAPSHOT_MESSAGE_LENGTH) < 0)            // <2>
        {
            idleStrategy.idle();
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        final MutableBoolean isAllDataLoaded = new MutableBoolean(false);
        final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->         // <1>
        {
            assert length >= SNAPSHOT_MESSAGE_LENGTH;                                       // <2>

            final long customerId = buffer.getLong(offset + SNAPSHOT_CUSTOMER_ID_OFFSET);
            final long price = buffer.getLong(offset + SNAPSHOT_PRICE_OFFSET);

            auction.loadInitialState(price, customerId);                                    // <3>

            isAllDataLoaded.set(true);
        };

        while (!snapshotImage.isEndOfStream())                                              // <4>
        {
            final int fragmentsPolled = snapshotImage.poll(fragmentHandler, 1);

            if (isAllDataLoaded.value)                                                      // <5>
            {
                break;
            }

            idleStrategy.idle(fragmentsPolled);                                             // <6>
        }

        assert snapshotImage.isEndOfStream();                                               // <7>
        assert isAllDataLoaded.value;
    }
    // end::loadSnapshot[]

    /**
     * {@inheritDoc}
     */
    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onTerminate(final Cluster cluster)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("onSessionOpen(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("onSessionClose(" + session + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    static class Auction
    {
        private long bestPrice = 0;
        private long currentWinningCustomerId = -1;

        void loadInitialState(final long price, final long customerId)
        {
            bestPrice = price;
            currentWinningCustomerId = customerId;
        }

        boolean attemptBid(final long price, final long customerId)
        {
            System.out.println("attemptBid(this=" + this + ", price=" + price + ",customerId=" + customerId + ")");

            if (price <= bestPrice)
            {
                return false;
            }

            bestPrice = price;
            currentWinningCustomerId = customerId;

            return true;
        }

        long getBestPrice()
        {
            return bestPrice;
        }

        long getCurrentWinningCustomerId()
        {
            return currentWinningCustomerId;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final Auction auction = (Auction)o;

            return bestPrice == auction.bestPrice && currentWinningCustomerId == auction.currentWinningCustomerId;
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            return Objects.hash(bestPrice, currentWinningCustomerId);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Auction{" +
                "bestPrice=" + bestPrice +
                ", currentWinningCustomerId=" + currentWinningCustomerId +
                '}';
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final BasicAuctionClusteredService that = (BasicAuctionClusteredService)o;

        return auction.equals(that.auction);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(auction);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "BasicAuctionClusteredService{" +
            "auction=" + auction +
            '}';
    }
}
