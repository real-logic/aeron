/*
 * Copyright 2019 Real Logic Ltd.
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
package io.aeron.archive;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Provides API to compute CRC for a data in a buffer.
 */
final class Crc32Helper
{
    private Crc32Helper()
    {
    }

    /**
     * Compute CRC over the buffer's payload.
     *
     * @param state  container for the CRC state.
     * @param buffer containing the data.
     * @param offset at which data begins.
     * @param length of the data in bytes.
     * @return computed CRC checksum
     * @throws NullPointerException     if {@code null == state} or {@code null == buffer}
     * @throws IllegalArgumentException if {@code frameOffset} and/or {@code frameLength} are out of range
     */
    public static int crc32(final CRC32 state, final ByteBuffer buffer, final int offset, final int length)
    {
        final int position = buffer.position();
        final int limit = buffer.limit();
        buffer.limit(offset + length).position(offset);
        state.reset();
        state.update(buffer);
        final int checksum = (int)state.getValue();
        buffer.limit(limit).position(position); // restore original values
        return checksum;
    }
}
