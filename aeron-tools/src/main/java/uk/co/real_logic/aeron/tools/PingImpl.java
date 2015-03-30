package uk.co.real_logic.aeron.tools;

public interface PingImpl
{
  void prepare();
  void connect();
  long sendPingAndReceivePong(int msgLen);
  void shutdown();
}
