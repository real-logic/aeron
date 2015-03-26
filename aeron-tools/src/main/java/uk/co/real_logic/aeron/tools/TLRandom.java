package uk.co.real_logic.aeron.tools;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TLRandom
{

    private static final ThreadLocal<Random> RND = new ThreadLocal<Random>();
    private static SeedCallback seedCallback;

    public static void setSeedCallback(SeedCallback callback)
    {
        seedCallback = callback;
    }

    public static Random current()
    {
        Random rnd = RND.get();
        if (rnd == null)
        {
            /* Generate a suggested starting seed. */
            long seed = ThreadLocalRandom.current().nextLong();
            /* Call user's callback if set to give them a chance to
             * record or modify the seed. */
            if (seedCallback != null)
            {
                seed = seedCallback.setSeed(seed);
            }
            rnd = new Random(seed);
            RND.set(rnd);
        }
        return rnd;
    }

    public interface SeedCallback
    {
        long setSeed(long seed);
    }
}
