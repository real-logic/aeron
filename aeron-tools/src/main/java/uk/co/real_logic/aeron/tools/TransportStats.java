package uk.co.real_logic.aeron.tools;

public class TransportStats
{
  protected String proto;
  protected String host;
  protected int port;
  protected long pos;
  protected String sessionId;
  protected boolean active;

  public TransportStats()
  {

  }

  public void setPos(long pos)
  {
    if (pos != this.pos)
    {
      this.pos = pos;
      active = true;
    }
  }

  protected void parseChannel(String channel)
  {
    String input = channel;
    proto = input.substring(0, input.indexOf(':'));
    input = input.substring(input.indexOf(':') + 3);
    host = input.substring(0, input.indexOf(':'));
    input = input.substring(input.indexOf(':') + 1);
    try
    {
      port = Integer.parseInt(input.substring(0, input.indexOf(' ')));
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

    input = input.substring(input.indexOf(' ') + 1);
    sessionId = input.substring(0, input.indexOf(' '));

    input = input.substring(input.indexOf(' ') + 1);
  }
}
