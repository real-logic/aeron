package io.aeron.samples.tutorial.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableBoolean;

import java.util.Objects;

// tag::new_service[]
public class BasicAuctionClusteredService implements ClusteredService
// end::new_service[]
{
    public static final int CORRELATION_ID_OFFSET = 0;
    public static final int CUSTOMER_ID_OFFSET = CORRELATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int PRICE_OFFSET = CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int BID_MESSAGE_LENGTH = PRICE_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int BID_SUCCEEDED_OFFSET = BID_MESSAGE_LENGTH;
    public static final int EGRESS_MESSAGE_LENGTH = BID_SUCCEEDED_OFFSET + BitUtil.SIZE_OF_BYTE;

    public static final int SNAPSHOT_CUSTOMER_ID_OFFSET = 0;
    public static final int SNAPSHOT_PRICE_OFFSET = SNAPSHOT_CUSTOMER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    public static final int SNAPSHOT_MESSAGE_LENGTH = SNAPSHOT_PRICE_OFFSET + BitUtil.SIZE_OF_LONG;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(4);
    private final MutableDirectBuffer snapshotBuffer = new ExpandableDirectByteBuffer(8);

    // tag::state[]
    private final Auction auction = new Auction();
    // end::state[]
    private Cluster cluster;

    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;     // <1>
        if (null != snapshotImage)  // <2>
        {
            loadSnapshot(cluster, snapshotImage);
        }
    }
    // end::start[]

    // tag::message[]
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);     // <1>
        final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long price = buffer.getLong(offset + PRICE_OFFSET);

        final boolean bidSucceeded = auction.attemptBid(price, customerId);            // <2>

        if (null != session)                                                           // <3>
        {
            egressMessageBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);         // <4>
            egressMessageBuffer.putLong(CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId());
            egressMessageBuffer.putLong(PRICE_OFFSET, auction.getBestPrice());
            egressMessageBuffer.putByte(BID_SUCCEEDED_OFFSET, bidSucceeded ? (byte)1 : (byte)0);

            while (session.offer(egressMessageBuffer, 0, EGRESS_MESSAGE_LENGTH) < 0)   // <5>
            {
                cluster.idle();                                                        // <6>
            }
        }
    }
    // end::message[]

    // tag::takeSnapshot[]
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        snapshotBuffer.putLong(CUSTOMER_ID_OFFSET, auction.getCurrentWinningCustomerId()); // <1>
        snapshotBuffer.putLong(PRICE_OFFSET, auction.getBestPrice());

        while (snapshotPublication.offer(snapshotBuffer, 0, SNAPSHOT_MESSAGE_LENGTH) < 0)  // <2>
        {
            cluster.idle();
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        final MutableBoolean allDataLoaded = new MutableBoolean(false);

        while (!snapshotImage.isEndOfStream())                                                 // <1>
        {
            final int fragmentsPolled = snapshotImage.poll((buffer, offset, length, header) -> // <2>
            {
                assert length >= SNAPSHOT_MESSAGE_LENGTH;                                      // <3>

                final long customerId = buffer.getLong(offset + SNAPSHOT_CUSTOMER_ID_OFFSET);
                final long price = buffer.getLong(offset + SNAPSHOT_PRICE_OFFSET);

                auction.loadInitialState(price, customerId);                                   // <4>

                allDataLoaded.set(true);
            }, 1);

            if (allDataLoaded.value)                                                           // <5>
            {
                break;
            }

            cluster.idle(fragmentsPolled);                                                     // <6>
        }

        assert snapshotImage.isEndOfStream();                                                  // <7>
        assert allDataLoaded.value;
    }
    // end::loadSnapshot[]

    public void onRoleChange(final Cluster.Role newRole)
    {

    }

    public void onTerminate(final Cluster cluster)
    {

    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("onSessionOpen(" + session + ")");
    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("onSessionClose(" + session + ")");
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {

    }

    private static class Auction
    {
        private long bestPrice = 0;
        private long currentWinningCustomerId = -1;

        public void loadInitialState(final long price, final long customerId)
        {
            bestPrice = price;
            currentWinningCustomerId = customerId;
        }

        public boolean attemptBid(final long price, final long customerId)
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

        public long getBestPrice()
        {
            return bestPrice;
        }

        public long getCurrentWinningCustomerId()
        {
            return currentWinningCustomerId;
        }

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
            return bestPrice == auction.bestPrice &&
                currentWinningCustomerId == auction.currentWinningCustomerId;
        }

        public int hashCode()
        {
            return Objects.hash(bestPrice, currentWinningCustomerId);
        }

        public String toString()
        {
            return "Auction{" +
                "bestPrice=" + bestPrice +
                ", currentWinningCustomerId=" + currentWinningCustomerId +
                '}';
        }
    }

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

    public int hashCode()
    {
        return Objects.hash(auction);
    }

    public String toString()
    {
        return "BasicAuctionClusteredService{" +
            "auction=" + auction +
            '}';
    }
}
