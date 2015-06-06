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
package uk.co.real_logic.aeron.driver.event;

import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.driver.event.EventCode.EXCEPTION;
import static uk.co.real_logic.aeron.driver.event.EventCode.FRAME_IN;
import static uk.co.real_logic.aeron.driver.event.EventConfiguration.*;

public class EventConfigurationTest
{
    @Test
    public void nullPropertyShouldDefaultToProductionEventCodes()
    {
        assertThat(getEnabledEventCodes(null), is(PRODUCTION_LOGGER_EVENT_CODES));
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedPropertyShouldDefaultToProductionEventCodes()
    {
        getEnabledEventCodes("list of invalid options");
    }

    @Test
    public void allPropertyShouldReturnAllEventCodes()
    {
        assertThat(getEnabledEventCodes("all"), is(ALL_LOGGER_EVENT_CODES));
    }

    @Test
    public void eventCodesPropertyShouldBeParsedAsAListOfEventCodes()
    {
        final Set<EventCode> expectedCodes = EnumSet.of(EXCEPTION, FRAME_IN);
        assertThat(getEnabledEventCodes("EXCEPTION,FRAME_IN"), is(expectedCodes));
    }

    @Test
    public void makeTagBitSet()
    {
        final Set<EventCode> eventCodes = EnumSet.of(EXCEPTION, FRAME_IN);
        final long bitSet = EventConfiguration.makeTagBitSet(eventCodes);
        assertThat(bitSet, is(EXCEPTION.tagBit() | FRAME_IN.tagBit()));
    }
}
