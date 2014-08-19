package uk.co.real_logic.aeron.common.event;

import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.common.event.EventCode.FRAME_IN;
import static uk.co.real_logic.aeron.common.event.EventCode.EXCEPTION;
import static uk.co.real_logic.aeron.common.event.EventConfiguration.*;

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
