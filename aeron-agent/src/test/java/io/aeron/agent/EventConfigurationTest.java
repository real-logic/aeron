/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.agent;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Set;

import static io.aeron.agent.EventConfiguration.*;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public class EventConfigurationTest
{
    @Test
    public void nullPropertyShouldDefaultToProductionEventCodes()
    {
        assertThat(getEnabledDriverEventCodes(null), is(EnumSet.noneOf(DriverEventCode.class)));
    }

    @Test
    public void malformedPropertyShouldDefaultToProductionEventCodes()
    {
        final PrintStream err = System.err;
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(stderr));
        try
        {
            final Set<DriverEventCode> enabledEventCodes = getEnabledDriverEventCodes("list of invalid options");
            assertThat(enabledEventCodes.size(), is(0));
            assertThat(stderr.toString(), startsWith("unknown event code: list of invalid options"));
        }
        finally
        {
            System.setErr(err);
        }
    }

    @Test
    public void allPropertyShouldReturnAllEventCodes()
    {
        assertThat(getEnabledDriverEventCodes("all"), is(ALL_LOGGER_EVENT_CODES));
    }

    @Test
    public void eventCodesPropertyShouldBeParsedAsListOfEventCodes()
    {
        final Set<DriverEventCode> expectedCodes = EnumSet.of(DriverEventCode.FRAME_OUT, DriverEventCode.FRAME_IN);
        assertThat(getEnabledDriverEventCodes("FRAME_OUT,FRAME_IN"), is(expectedCodes));
    }

    @Test
    public void allClusterEventsShouldBeEnabled()
    {
        assertThat(getEnabledClusterEventCodes("all"), is(EnumSet.allOf(ClusterEventCode.class)));
    }

    @Test
    public void makeTagBitSet()
    {
        final Set<DriverEventCode> eventCodes = EnumSet.of(DriverEventCode.FRAME_OUT, DriverEventCode.FRAME_IN);
        final long bitSet = EventConfiguration.makeTagBitSet(eventCodes);
        assertThat(bitSet, is(DriverEventCode.FRAME_OUT.tagBit() | DriverEventCode.FRAME_IN.tagBit()));
    }
}
