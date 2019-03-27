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