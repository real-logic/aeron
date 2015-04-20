package uk.co.real_logic.aeron.tools;

public class PublisherStats extends TransportStats
{
  private long limit;

  public PublisherStats(final String channel)
  {
    parseChannel(channel);
    active = true;
  }

  public void setLimit(final long limit)
  {
    if (limit != this.limit)
    {
      this.limit = limit;
      active = true;
    }
  }

  @Override
public String toString()
  {
    final String s = String.format("%1$5s %2$8d %3$8d %4$10s:%5$5d %6$s%7$s %8$8s\n",
        proto, pos, limit, host, port, "0x", sessionId, active ? "ACTIVE" : "INACTIVE");
    active = false;

    return s;
  }
}
