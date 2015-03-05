package uk.co.real_logic.aeron.tools;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TLRandom {

	final private static ThreadLocal<Random> RND = new ThreadLocal<Random>();
	private static SeedCallback SEED_CALLBACK;
	
	public static void setSeedCallback(SeedCallback seedCallback)
	{
		SEED_CALLBACK = seedCallback;
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
			if (SEED_CALLBACK != null)
			{
				seed = SEED_CALLBACK.setSeed(seed);
			}
			rnd = new Random(seed);
			RND.set(rnd);
		}
		return rnd;
	}
	
	public interface SeedCallback
	{
		public long setSeed(long seed);
	}
}
