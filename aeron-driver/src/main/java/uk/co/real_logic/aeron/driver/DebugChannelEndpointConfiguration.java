/*
 * Copyright 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import static java.lang.Long.getLong;
import static java.lang.System.getProperty;

public class DebugChannelEndpointConfiguration
{
    /**
     * Property name for data loss rate.
     */
    public static final String DATA_LOSS_RATE_PROP_NAME = "aeron.debug.data.loss.rate";

    /**
     * Property name for data loss seed.
     */
    public static final String DATA_LOSS_SEED_PROP_NAME = "aeron.debug.data.loss.seed";

    /**
     * Property name for control loss rate.
     */
    public static final String CONTROL_LOSS_RATE_PROP_NAME = "aeron.debug.control.loss.rate";

    /**
     * Property name for control loss seed.
     */
    public static final String CONTROL_LOSS_SEED_PROP_NAME = "aeron.debug.control.loss.seed";

    public static final long DATA_LOSS_SEED = getLong(DATA_LOSS_SEED_PROP_NAME, -1);

    public static final double DATA_LOSS_RATE = Double.parseDouble(getProperty(DATA_LOSS_RATE_PROP_NAME, "0.0"));

    public static final long CONTROL_LOSS_SEED = getLong(CONTROL_LOSS_SEED_PROP_NAME, -1);

    public static final double CONTROL_LOSS_RATE = Double.parseDouble(getProperty(CONTROL_LOSS_RATE_PROP_NAME, "0.0"));

    public static LossGenerator lossGeneratorSupplier(final double lossRate, final long lossSeed)
    {
        if (0 == lossRate)
        {
            return (address, buffer, length) -> false;
        }

        return new RandomLossGenerator(lossRate, lossSeed);
    }

    public static LossGenerator dataLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(DATA_LOSS_RATE, DATA_LOSS_SEED);
    }

    public static LossGenerator controlLossGeneratorSupplier()
    {
        return lossGeneratorSupplier(CONTROL_LOSS_RATE, CONTROL_LOSS_SEED);
    }

}
