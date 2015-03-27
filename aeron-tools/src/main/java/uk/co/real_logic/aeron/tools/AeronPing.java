package uk.co.real_logic.aeron.tools;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.*;

public class AeronPing implements PingImpl
{
  private Aeron.Context ctx = null;
  private FragmentAssemblyAdapter dataHandler = null;
  private Aeron aeron = null;
  private Publication pingPub = null;
  private Subscription pongSub = null;
  private CountDownLatch pongConnectionLatch = null;
  private CountDownLatch pongedMessageLatch = null;
  private int pingStreamId = 10;
  private int pongStreamId = 11;
  private String pingChannel = "udp://localhost:44444";
  private String pongChannel = "udp://localhost:55555";
  private long rtt;
  private int fragmentCountLimit;

  public AeronPing()
  {

  }

  public void prepare()
  {
    ctx = new Aeron.Context()
      .newConnectionHandler(this::newPongConnectionHandler);
    dataHandler = new FragmentAssemblyAdapter(this::pongHandler);
    aeron = Aeron.connect(ctx);
    pingPub = aeron.addPublication(pingChannel, pingStreamId);
    pongSub = aeron.addSubscription(pongChannel, pongStreamId, dataHandler);
    pongConnectionLatch = new CountDownLatch(1);
    fragmentCountLimit = 1;
  }

  public void connect()
  {
    try
    {
      pongConnectionLatch.await();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public long sendPingAndReceivePong(int msgLen)
  {
    IdleStrategy idle = new BusySpinIdleStrategy();
    pongedMessageLatch = new CountDownLatch(1);
    UnsafeBuffer atomicBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(msgLen));

    do
    {
      atomicBuffer.putLong(0, System.nanoTime());
    }
    while (!pingPub.offer(atomicBuffer, 0, msgLen));

    while (pongSub.poll(fragmentCountLimit) <= 0)
    {
      idle.idle(0);
    }

    try
    {
      if (pongedMessageLatch.await(10, TimeUnit.SECONDS))
      {
        return rtt >> 1;
      }
      else
      {
        return -1;
      }
    }
    catch (Exception e)
    {
      return -1;
    }
  }

  public void shutdown()
  {

  }

  private void newPongConnectionHandler(String channel, int streamId,
      int sessionId, String sourceInfo)
  {
    if (channel.equals(pongChannel) && pongStreamId == streamId)
    {
      pongConnectionLatch.countDown();
    }
  }

  private void pongHandler(DirectBuffer buffer, int offset, int length,
      Header header)
  {
    long pingTimestamp = buffer.getLong(offset);

    rtt = System.nanoTime() - pingTimestamp;

    pongedMessageLatch.countDown();
  }
}
