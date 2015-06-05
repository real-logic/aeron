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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SecondsAtMessagesPerSecondTest
{
    RateController rc;
    List<RateControllerInterval> ivlsList = new ArrayList<>();

    @Test
    public void createWithOneAndOne() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(1, 1));
        rc = new RateController(() -> 0, ivlsList);
    }

    @Test
    public void createWithZeroMessagesPerSecond() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(1, 0));
        rc = new RateController(() -> 0, ivlsList);
    }

    @Test (expected = Exception.class)
    public void createWithZeroSeconds() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(0, 1));
        rc = new RateController(() -> 0, ivlsList);
    }

    @Test (expected = Exception.class)
    public void createWithNegativeSeconds() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(-1, 1));
        rc = new RateController(() -> 0, ivlsList);
    }

    @Test (expected = Exception.class)
    public void createWithNegativeMessagesPerSecond() throws Exception
    {
        ivlsList.clear();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(1, -1));
        rc = new RateController(() -> 0, ivlsList);
    }
}
