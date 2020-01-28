/*
 *  Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.cluster.client.ClusterException;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CncFileDescriptor.*;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.TYPE_ID_OFFSET;

/**
 * Toggle control {@link ToggleState}s for a cluster node such as {@link ToggleState#SUSPEND} or
 * {@link ToggleState#RESUME}. This can only be applied to the {@link io.aeron.cluster.service.Cluster.Role#LEADER}.
 */
public class ClusterControl
{
    /**
     * Toggle states for controlling the cluster node once it has entered the active state after initialising.
     * The toggle can only we switched into a new state from {@link #NEUTRAL} and will be reset by the
     * {@link io.aeron.cluster.ConsensusModule} once the triggered action is complete.
     */
    public enum ToggleState
    {
        /**
         * Inactive state, not accepting new actions.
         */
        INACTIVE(0),

        /**
         * Neutral state ready to accept a new action.
         */
        NEUTRAL(1),

        /**
         * Suspend processing of ingress and timers.
         */
        SUSPEND(2),

        /**
         * Resume processing of ingress and timers.
         */
        RESUME(3),

        /**
         * Take a snapshot of cluster state.
         */
        SNAPSHOT(4),

        /**
         * Shut down the cluster in an orderly fashion by taking a snapshot first then terminating.
         */
        SHUTDOWN(5),

        /**
         * Abort processing and terminate the cluster without taking a snapshot.
         */
        ABORT(6);

        private final int code;

        private static final ToggleState[] STATES;
        static
        {
            final ToggleState[] toggleStates = values();
            STATES = new ToggleState[toggleStates.length];
            for (final ToggleState toggleState : toggleStates)
            {
                STATES[toggleState.code()] = toggleState;
            }
        }

        ToggleState(final int code)
        {
            this.code = code;
        }

        /**
         * Code to be used as the indicator in the control toggle counter.
         *
         * @return code to be used as the indicator in the control toggle counter.
         */
        public final int code()
        {
            return code;
        }

        /**
         * Toggle the control counter to trigger the requested {@link ToggleState}.
         * <p>
         * This action is thread safe and will succeed if the toggle is in the {@link ToggleState#NEUTRAL} state,
         * or if toggle is {@link ToggleState#SUSPEND} and requested state is {@link ToggleState#RESUME}.
         *
         * @param controlToggle to change to the trigger state.
         * @return true if the counter toggles or false if it is in a state other than {@link ToggleState#NEUTRAL}.
         */
        public final boolean toggle(final AtomicCounter controlToggle)
        {
            if (code() == RESUME.code() && controlToggle.get() == SUSPEND.code())
            {
                return controlToggle.compareAndSet(SUSPEND.code(), RESUME.code());
            }

            return controlToggle.compareAndSet(NEUTRAL.code(), code());
        }

        /**
         * Reset the toggle to the {@link #NEUTRAL} state.
         *
         * @param controlToggle to be reset.
         */
        public static void reset(final AtomicCounter controlToggle)
        {
            controlToggle.set(NEUTRAL.code());
        }

        /**
         * Activate the toggle by setting it to the {@link #NEUTRAL} state.
         *
         * @param controlToggle to be activated.
         */
        public static void activate(final AtomicCounter controlToggle)
        {
            controlToggle.set(NEUTRAL.code());
        }

        /**
         * Activate the toggle by setting it to the {@link #INACTIVE} state.
         *
         * @param controlToggle to be deactivated.
         */
        public static void deactivate(final AtomicCounter controlToggle)
        {
            controlToggle.set(INACTIVE.code());
        }

        /**
         * Get the {@link ToggleState} for a given control toggle.
         *
         * @param controlToggle to get the current state for.
         * @return the state for the current control toggle.
         * @throws ClusterException if the counter is not one of the valid values.
         */
        public static ToggleState get(final AtomicCounter controlToggle)
        {
            final long toggleValue = controlToggle.get();

            if (toggleValue < 0 || toggleValue > (STATES.length - 1))
            {
                throw new ClusterException("invalid toggle value: " + toggleValue);
            }

            return STATES[(int)toggleValue];
        }
    }

    /**
     * Counter type id for the control toggle.
     */
    public static final int CONTROL_TOGGLE_TYPE_ID = 202;

    /**
     * Map a {@link CountersReader} over the default location CnC file.
     *
     * @return a {@link CountersReader} over the default location CnC file.
     */
    public static CountersReader mapCounters()
    {
        return mapCounters(CommonContext.newDefaultCncFile());
    }

    /**
     * Map a {@link CountersReader} over the provided filename for the CnC file.
     *
     * @param filename for the CnC file.
     * @return a {@link CountersReader} over the provided CnC file.
     */
    public static CountersReader mapCounters(final String filename)
    {
        return mapCounters(new File(filename));
    }

    /**
     * Map a {@link CountersReader} over the provided {@link File} for the CnC file.
     *
     * @param cncFile for the counters.
     * @return a {@link CountersReader} over the provided CnC file.
     */
    public static CountersReader mapCounters(final File cncFile)
    {
        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);

        return new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData),
            StandardCharsets.US_ASCII);
    }

    /**
     * Find the control toggle counter or return null if not found.
     *
     * @param counters to search for the control toggle.
     * @return the control toggle counter or return null if not found.
     */
    public static AtomicCounter findControlToggle(final CountersReader counters)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            final int recordOffset = CountersReader.metaDataOffset(i);

            if (counters.getCounterState(i) == RECORD_ALLOCATED &&
                buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONTROL_TOGGLE_TYPE_ID)
            {
                return new AtomicCounter(counters.valuesBuffer(), i, null);
            }
        }

        return null;
    }

    public static void main(final String[] args)
    {
        checkUsage(args);

        final ToggleState toggleState = ToggleState.valueOf(args[0].toUpperCase());

        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final CountersReader countersReader = mapCounters(cncFile);
        final AtomicCounter controlToggle = findControlToggle(countersReader);

        if (null == controlToggle)
        {
            System.out.println("Failed to find control toggle");
            System.exit(0);
        }

        if (toggleState.toggle(controlToggle))
        {
            System.out.println(toggleState + " toggled successfully");
        }
        else
        {
            System.out.println(toggleState + " did NOT toggle: current state=" + ToggleState.get(controlToggle));
        }
    }

    private static void checkUsage(final String[] args)
    {
        if (1 != args.length)
        {
            System.out.format("Usage: [-Daeron.dir=<directory containing CnC file>] " +
                ClusterControl.class.getSimpleName() + " <action>%n");

            System.exit(0);
        }
    }
}
