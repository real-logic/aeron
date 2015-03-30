package uk.co.real_logic.aeron.tools;

public interface StatsOutput
{
  void format(String[] keys, long[] vals) throws Exception;
  void close() throws Exception;
}
