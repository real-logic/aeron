/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class BufferExhaustionPredictorTest
{

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        Object[][] data = new Object[][]
        {
            { 10, new long[] {30, 30, 30}, true },
            { 100, new long[] {30, 10, 20}, true },
            { 10, new long[] {1, 1, 1}, false },
        };
        return Arrays.asList(data);
    }

    private final BufferExhaustionPredictor predictor = new BufferExhaustionPredictor(100);
    private final long period;
    private final long[] amounts;
    private final boolean result;

    public BufferExhaustionPredictorTest(final long period, final long[] amounts, final boolean result)
    {
        this.period = period;
        this.amounts = amounts;
        this.result = result;
    }

    @Test
    public void canPredictExhaustion()
    {
        long currentTime = System.nanoTime();
        long startTime = currentTime - period;
        for (int i = 0; i < amounts.length; i++)
        {
            predictor.dataWritten(amounts[i], startTime + i);
        }
        assertThat(predictor.predictExhaustion(currentTime), is(result));
    }

}
