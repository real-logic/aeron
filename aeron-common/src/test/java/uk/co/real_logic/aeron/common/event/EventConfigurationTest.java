package uk.co.real_logic.aeron.common.event;

import org.junit.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.common.event.EventCode.ERROR_SENDING_HEARTBEAT_PACKET;
import static uk.co.real_logic.aeron.common.event.EventCode.EXCEPTION;
import static uk.co.real_logic.aeron.common.event.EventConfiguration.*;

public class EventConfigurationTest
{

    @Test
    public void nullPropertyShouldDefaultToProductionEventCodes()
    {
        assertThat(getEnabledEventCodes(null), is(PRODUCTION_LOGGER_EVENT_CODES));
    }

    @Test
    public void malformedPropertyShouldDefaultToProductionEventCodes()
    {
        assertThat(getEnabledEventCodes("daskjdasklh"), is(PRODUCTION_LOGGER_EVENT_CODES));
    }

    @Test
    public void allPropertyShouldReturnAllEventCodes()
    {
        assertThat(getEnabledEventCodes("all"), is(ALL_LOGGER_EVENT_CODES));
    }

    @Test
    public void eventCodesPropertyShouldBeParsedAsAListOfEventCodes()
    {
        final Set<EventCode> expectedCodes = EnumSet.of(EXCEPTION, ERROR_SENDING_HEARTBEAT_PACKET);
        assertThat(getEnabledEventCodes("EXCEPTION,ERROR_SENDING_HEARTBEAT_PACKET"), is(expectedCodes));
    }

    @Test
    public void makeTagBitSet()
    {
        final Set<EventCode> eventCodes = EnumSet.of(EXCEPTION, ERROR_SENDING_HEARTBEAT_PACKET);
        final long bitSet = EventConfiguration.makeTagBitSet(eventCodes);
        assertThat(bitSet, is(EXCEPTION.tagBit() | ERROR_SENDING_HEARTBEAT_PACKET.tagBit()));
    }

}
