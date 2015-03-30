package uk.co.real_logic.aeron.tools.log_analysis;

/**
 * Created by philip on 3/30/15.
 */
public class LogRunner
{
    public static void main(String[] args)
    {
        LogModel model = new LogModel();
        model.processLogBuffer(args[0]);
    }
}
