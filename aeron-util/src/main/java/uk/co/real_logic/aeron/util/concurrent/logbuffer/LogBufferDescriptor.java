/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util.concurrent.logbuffer;

import uk.co.real_logic.aeron.util.BitUtil;

/**
 * Layout description for the log buffer state.
 */
public class LogBufferDescriptor
{
    /** Offset within the trailer where the tail value is stored. */
    public static final int STATE_TAIL_COUNTER_OFFSET;

    /** Total size of the state buffer */
    public static final int STATE_SIZE;

    static
    {
        int offset = 0;
        STATE_TAIL_COUNTER_OFFSET = offset;

        offset += BitUtil.CACHE_LINE_SIZE;
        STATE_SIZE = offset;
    }

    /** Minimum buffer size for the log */
    public static final int LOG_MIN_SIZE = RecordDescriptor.ALIGNMENT * 256;
}
