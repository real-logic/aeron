package uk.co.real_logic.aeron.tools;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Created by ericb on 3/27/15.
 */
public class MessagesAtBitsPerSecondTest
{
    RateController rc;
    List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();

    class Callback implements RateController.Callback
    {
        @Override
        public int onNext()
        {
            return 0;
        }

    }

    Callback callback = new Callback();

    @Test
    public void createWithOneAndOne() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, 1));
        rc = new RateController(callback, ivlsList);
    }

    @Test (expected=Exception.class)
    public void createWithZeroBitsPerSecond() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, 0));
        rc = new RateController(callback, ivlsList);
    }

    @Test (expected=Exception.class)
    public void createWithZeroMessages() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new MessagesAtBitsPerSecondInterval(0, 1));
        rc = new RateController(callback, ivlsList);
    }

    @Test (expected=Exception.class)
    public void createWithNegativeMessages() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new MessagesAtBitsPerSecondInterval(-1, 1));
        rc = new RateController(callback, ivlsList);
    }

    @Test (expected=Exception.class)
    public void createWithNegativeBitsPerSecond() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, -1));
        rc = new RateController(callback, ivlsList);
    }
}
