/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.locks.LockSupport;

/**
 * An idle strategy to be employed in loops that might do significant work on each iteration.
 *
 * If work done, then not idle
 *
 * If work not done, then idle like so: (1) Spin 1 time, (2) Yield 1 time, and (3) Park for min duration
 */
public class SpinYieldParkIdleStrategy implements IdleStrategy
{
    public enum State
    {
        SPIN, YIELD, PARK
    }

    private State state;

    public SpinYieldParkIdleStrategy()
    {
        this.state = State.SPIN;
    }

    /** {@inheritDoc} */
    public void idle(final int workCount)
    {
        if (workCount > 0)
        {
            state = State.SPIN;
            return;
        }

        switch (state)
        {
            case SPIN:
                state = State.YIELD;
                break;

            case YIELD:
                state = State.PARK;
                Thread.yield();
                break;

            case PARK:
                LockSupport.parkNanos(1);
                break;
        }
    }
}
