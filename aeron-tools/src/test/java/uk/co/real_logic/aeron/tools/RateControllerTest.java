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

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;

public class RateControllerTest
{
    private RateController rc;

    interface TestCallbackStats
    {
        long numMessagesSent();

        long numBitsSent();
    }

    public abstract class TestCallback implements RateController.Callback, TestCallbackStats
    {
        protected long numMessagesSent = 0;
        protected long numBitsSent = 0;

        public long numMessagesSent()
        {
            return numMessagesSent;
        }

        public long numBitsSent()
        {
            return numBitsSent;
        }
    }

    @Test(expected = Exception.class)
    public void createWithNulls() throws Exception
    {
        rc = new RateController(null, null);
    }

    @Test(expected = Exception.class)
    public void createWithNullCallback() throws Exception
    {
        final RateControllerInterval ivl = new MessagesAtBitsPerSecondInterval(1, 1);
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(ivl);

        rc = new RateController(null, ivlsList);
    }

    @Test(expected = Exception.class)
    public void createWithNullIntervalsList() throws Exception
    {
        rc = new RateController(() -> {
            return 0;
        }, null);
    }

    @Test(expected = Exception.class)
    public void createWithEmptyIntervalsList() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test
    public void createWithMessagesAtMessagesPerSecond() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new MessagesAtMessagesPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test
    public void createWithMessagesAtBitsPerSecond() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new MessagesAtBitsPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test
    public void createWithSecondsAtBitsPerSecond() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new SecondsAtBitsPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test
    public void createWithSecondsAtMessagesPerSecond() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new SecondsAtMessagesPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test(expected = Exception.class)
    public void createWithZeroIterations() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new MessagesAtMessagesPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList, 0);
    }

    @Test(expected = Exception.class)
    public void createWithNegativeIterations() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        final RateControllerInterval ivl = new MessagesAtMessagesPerSecondInterval(1, 1);
        ivlsList.add(ivl);
        rc = new RateController(() -> {
            return 0;
        }, ivlsList, -1);
    }

    @Test
    public void createWithFourIntervals() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(1, 1));
        ivlsList.add(new SecondsAtBitsPerSecondInterval(1, 1));
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(1, 1));
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, 1));
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);
    }

    @Test
    public void sendOneMessage() throws Exception
    {

        class Callback extends TestCallback
        {
            public int onNext()
            {
                numMessagesSent++;
                return 0;
            }
        }

        final RateController.Callback callback = new Callback();
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(1, 1));
        rc = new RateController(callback, ivlsList);
        while (rc.next())
        {

        }
        assertThat("FAIL: Exactly one message should have been sent",
            ((TestCallbackStats)callback).numMessagesSent(), is(1L));
    }

    @Test
    public void sendOneBit() throws Exception
    {

        class Callback extends TestCallback
        {
            public int onNext()
            {
                numMessagesSent++;
                numBitsSent += 1;
                return 1;
            }
        }

        final RateController.Callback callback = new Callback();
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(1, 1));
        rc = new RateController(callback, ivlsList);
        while (rc.next())
        {

        }
        assertThat("FAIL: Exactly one bit should have been sent",
            ((TestCallbackStats)callback).numBitsSent(), is(1L));
    }

    @Test
    public void sendFiveMessagesAtMaxMesagesPerSecond() throws Exception
    {

        class Callback extends TestCallback
        {
            public int onNext()
            {
                numMessagesSent++;
                numBitsSent += 10;
                return 10;
            }
        }

        final RateController.Callback callback = new Callback();
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(5, Long.MAX_VALUE));
        rc = new RateController(callback, ivlsList);
        while (rc.next())
        {

        }
        assertThat("FAIL: Exactly five messages should have been sent",
            ((TestCallbackStats)callback).numMessagesSent(), is(5L));
        assertThat("FAIL: Exactly 50 bits should have been sent",
            ((TestCallbackStats)callback).numBitsSent(), is(50L));
    }

    @Test
    public void sendTwoIntervals() throws Exception
    {

        class Callback extends TestCallback
        {
            public int onNext()
            {
                numMessagesSent++;
                numBitsSent += 10;
                return 10;
            }
        }

        final RateController.Callback callback = new Callback();
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(1, Long.MAX_VALUE));
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, Long.MAX_VALUE));
        rc = new RateController(callback, ivlsList);
        while (rc.next())
        {

        }
        assertThat("FAIL: Exactly two messages should have been sent",
            ((TestCallbackStats)callback).numMessagesSent(), is(2L));
        assertThat("FAIL: Exactly 20 bits should have been sent",
            ((TestCallbackStats)callback).numBitsSent(), is(20L));
    }

    @Test
    public void sendTwoIterationsOfTwoIntervals() throws Exception
    {

        class Callback extends TestCallback
        {
            public int onNext()
            {
                numMessagesSent++;
                numBitsSent += 10;
                return 10;
            }
        }

        final RateController.Callback callback = new Callback();
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new MessagesAtMessagesPerSecondInterval(1, Long.MAX_VALUE));
        ivlsList.add(new MessagesAtBitsPerSecondInterval(1, Long.MAX_VALUE));
        rc = new RateController(callback, ivlsList, 2);
        while (rc.next())
        {

        }
        assertThat("FAIL: Exactly four messages should have been sent",
            ((TestCallbackStats)callback).numMessagesSent(), is(4L));
        assertThat("FAIL: Exactly 40 bits should have been sent",
            ((TestCallbackStats)callback).numBitsSent(), is(40L));
    }

    @Test
    public void sendForOneSecond() throws Exception
    {
        final List<RateControllerInterval> ivlsList = new ArrayList<RateControllerInterval>();
        ivlsList.add(new SecondsAtMessagesPerSecondInterval(1, Long.MAX_VALUE));
        rc = new RateController(() -> {
            return 0;
        }, ivlsList);

        /* Wall-clock time isn't an exact science... so we'll accept a
         * fairly generous range of 0.8 seconds to 1.2 seconds of elapsed
         * time before we complain. */
        final long startTime = System.nanoTime();
        while (rc.next())
        {

        }
        final long endTime = System.nanoTime();
        assertThat("FAIL: Send should have taken about one second",
            endTime,
            both(greaterThanOrEqualTo(startTime + 800000000L)).and(lessThanOrEqualTo(startTime + 1200000000L)));
    }
}
