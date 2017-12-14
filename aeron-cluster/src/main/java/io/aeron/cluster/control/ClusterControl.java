/*
 *  Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.control;

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CncFileDescriptor.*;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.cluster.control.ClusterControl.Action.NEUTRAL;
import static io.aeron.cluster.control.ClusterControl.Action.RESUME;
import static io.aeron.cluster.control.ClusterControl.Action.SUSPEND;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.TYPE_ID_OFFSET;

/**
 * Toggle control actions for a cluster node such as {@link Action#SUSPEND} or {@link Action#RESUME}.
 */
public class ClusterControl
{
    /**
     * Action request to the cluster.
     */
    public enum Action
    {
        /**
         * Neutral state ready to accept a new action.
         */
        NEUTRAL(0)
        {
            public boolean toggle(final AtomicCounter controlToggle)
            {
                controlToggle.set(NEUTRAL.code());
                return true;
            }
        },

        /**
         * Suspend processing of ingress and timers.
         */
        SUSPEND(1)
        {
            public boolean toggle(final AtomicCounter controlToggle)
            {
                return suspend(controlToggle);
            }
        },

        /**
         * Resume processing of ingress and timers.
         */
        RESUME(2)
        {
            public boolean toggle(final AtomicCounter controlToggle)
            {
                return resume(controlToggle);
            }
        };

        private final long code;

        Action(final long code)
        {
            this.code = code;
        }

        public abstract boolean toggle(AtomicCounter controlToggle);

        /**
         * Code to be used as the indicator in the control toggle counter.
         *
         * @return code to be used as the indicator in the control toggle counter.
         */
        public long code()
        {
            return code;
        }
    }

    /**
     * Counter type id for the control toggle.
     */
    public static final int CONTROL_TOGGLE_TYPE_ID = 200;

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

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException(
                "Aeron CnC version does not match: version=" + cncVersion + " required=" + CNC_VERSION);
        }

        return new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData),
            StandardCharsets.US_ASCII);
    }

    /**
     * Find the control toggle counter or return null if not found.
     *
     * @param countersReader to search for the control toggle.
     * @return the control toggle counter or return null if not found.
     */
    public static AtomicCounter findControlToggle(final CountersReader countersReader)
    {
        final AtomicBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            final int recordOffset = CountersReader.metaDataOffset(i);

            if (countersReader.getCounterState(i) == RECORD_ALLOCATED &&
                buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONTROL_TOGGLE_TYPE_ID)
            {
                return new AtomicCounter(buffer, i, null);
            }
        }

        return null;
    }

    /**
     * Set the control toggle into a suspend state for the cluster to react to.
     *
     * @param controlToggle to be be set to suspend.
     * @return true if successfully set or false the toggle was not in a neutral state.
     */
    public static boolean suspend(final AtomicCounter controlToggle)
    {
        return controlToggle.compareAndSet(NEUTRAL.code(), SUSPEND.code());
    }

    /**
     * Set the control toggle into a resume state for the cluster to react to.
     *
     * @param controlToggle to be be set to resume.
     * @return true if successfully set or false the toggle was not in a neutral state.
     */
    public static boolean resume(final AtomicCounter controlToggle)
    {
        return controlToggle.compareAndSet(NEUTRAL.code(), RESUME.code());
    }

    public static void main(final String[] args)
    {
        checkUsage(args);

        final Action action = Action.valueOf(args[0].toUpperCase());

        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final CountersReader countersReader = mapCounters(cncFile);
        final AtomicCounter controlToggle = findControlToggle(countersReader);

        if (null == controlToggle)
        {
            System.out.println("Failed to find control toggle");
            System.exit(0);
        }

        if (action.toggle(controlToggle))
        {
            System.out.println(action + " toggled successfully");
        }
        else
        {
            System.out.println(action + " did NOT toggle");
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
