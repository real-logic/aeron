/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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

import java.net.InetSocketAddress;

/**
 * Uniform random loss generator
 */
public class PercentageLossGenerator implements LossGenerator
{
    private int lossRate;
    private int framesNeedToBeDropped;
    private long totalFrameCount;
    private int moduloFactor;
    private boolean simpleOperation;

    /**
     * Construct loss generator with given loss rate as percentage
     *
     * @param lossRate for generating loss
     */
    public PercentageLossGenerator(final int myLossRate) throws Exception
    {
        simpleOperation = false;
        setPercentageLossRate(myLossRate);
        framesNeedToBeDropped = 0;
        totalFrameCount = 0;
    }

    public void setPercentageLossRate(final int myLossRate) throws Exception
    {
        if (myLossRate < 0 || myLossRate > 100)
        {
            throw new Exception("Loss rate is out of range " + myLossRate);
        }
        this.lossRate = myLossRate;

        if ((100 % myLossRate) == 0)
        {
            moduloFactor = 100 / myLossRate;
            simpleOperation = true;
        }
        else
        {
            framesNeedToBeDropped = myLossRate;
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean shouldDropFrame(final InetSocketAddress address, final int length)
    {
        boolean needsToBeDropped = false;
        totalFrameCount++;
        if (simpleOperation)
        {
            needsToBeDropped = ((totalFrameCount % (moduloFactor) == 0) && (lossRate > 0)) ? true : false;
        }
        else
        {
            if ((totalFrameCount % 100) != 0)
            {
                if (framesNeedToBeDropped > 0)
                {
                    framesNeedToBeDropped--;
                    needsToBeDropped = true;
                }
                else
                {
                    needsToBeDropped = false;
                }
            }
            else
            {
                framesNeedToBeDropped = lossRate;
                needsToBeDropped =  false;
            }
        }
        return needsToBeDropped;
    }
}
