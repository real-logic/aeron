/*
 * Copyright 2015 Kaazing Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.tools;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SeedableThreadLocalRandom
{
    private static final ThreadLocal<Random> RND = new ThreadLocal<Random>();
    private static SeedCallback seedCallback;

    public static void setSeedCallback(final SeedCallback callback)
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
