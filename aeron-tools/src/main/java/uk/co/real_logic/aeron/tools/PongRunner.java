package uk.co.real_logic.aeron.tools;

public class PongRunner
{
  private PongImpl impl = null;

  public PongRunner(String[] args)
  {
    if (args[0].equalsIgnoreCase("aeron"))
    {
      impl = new AeronPong();
    }
    else if (args[0].equalsIgnoreCase("qpid"))
    {
      impl = new QpidPong();
    }
    impl.prepare();
    impl.run();
  }

  public static void main(String[] args)
  {
    new PongRunner(args);
  }
}
