package uk.co.real_logic.aeron.tools;

public class SubscriberStats extends TransportStats
{
  private long hwm;

  public SubscriberStats(String channel)
  {
    parseChannel(channel);
    active = true;
  }

  public void setHWM(long hwm)
  {
    if (hwm != this.hwm)
    {
      this.hwm = hwm;
      active = true;
    }
  }

  public String toString()
  {
    String s = String.format("%1$5s %2$8d %3$8d %4$10s:%5$5d %6$s%7$s %8$8s\n",
        proto, pos, hwm, host, port, "0x", sessionId, active ? "ACTIVE" : "INACTIVE");

    active = false;
    return s;
  }
}
