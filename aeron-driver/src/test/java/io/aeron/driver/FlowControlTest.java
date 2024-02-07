package io.aeron.driver;

import org.junit.jupiter.api.Test;

import static io.aeron.driver.FlowControl.calculateRetransmissionLength;
import static org.junit.jupiter.api.Assertions.*;

class FlowControlTest
{
    @Test
    void shouldUseResendLengthIfSmallestValue()
    {
        final int resendLength = 1024;

        assertEquals(resendLength, calculateRetransmissionLength(resendLength, 64 * 1024, 0, 16));
    }

    @Test
    void shouldClampToTheEndOfTheBuffer()
    {
        final int expectedLength = 512;
        final int termLength = 64 * 1024;
        final int termOffset = termLength - expectedLength;

        assertEquals(expectedLength, calculateRetransmissionLength(1024, termLength, termOffset, 16));
    }

    @Test
    void shouldClampToReceiverWindow()
    {
        final int multiplier = 16;
        final int expectedLength = Configuration.INITIAL_WINDOW_LENGTH_DEFAULT * multiplier;

        assertEquals(expectedLength, calculateRetransmissionLength(4 * 1024 * 1024, 8 * 1024 * 1024, 0, 16));
    }
}