package io.aeron;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public final class BufferBuilderUtilTest
{
    @Test
    public void shouldFindMaxCapacityWhenRequested()
    {
        assertThat(BufferBuilderUtil.findSuitableCapacity(0, BufferBuilderUtil.MAX_CAPACITY),
            is(BufferBuilderUtil.MAX_CAPACITY));
    }
}