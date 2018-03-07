/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver.ext;

import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;
import java.util.Random;

/**
 * Uniform random loss generator which can be used for testing loss scenarios.
 */
public class RandomLossGenerator implements LossGenerator
{
    private final double lossRate;
    private final Random random;

    /**
     * Construct loss generator with given loss rate as percentage.
     *
     * @param lossRate for generating loss
     */
    public RandomLossGenerator(final double lossRate)
    {
        this(lossRate, -1);
    }

    /**
     * Construct loss generator with given loss rate as percentage and random seed
     *
     * @param lossRate for generating loss
     * @param lossSeed for random seeding
     */
    public RandomLossGenerator(final double lossRate, final long lossSeed)
    {
        this.random = -1 == lossSeed ? new Random() : new Random(lossSeed);
        this.lossRate = lossRate;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return random.nextDouble() <= lossRate;
    }
}
