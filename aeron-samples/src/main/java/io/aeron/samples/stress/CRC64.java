/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples.stress;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * CRC-64 implementation with ability to combine checksums calculated over different blocks of data.
 */
public class CRC64
{
    private static final long POLY = 0xc96c5795d7870f42L; // ECMA-182

    /* CRC64 calculation table. */
    private static final long[] TABLE;

    /* Current CRC value. */
    private long value;

    static
    {
        TABLE = new long[256];

        for (int n = 0; n < 256; n++)
        {
            long crc = n;
            for (int k = 0; k < 8; k++)
            {
                if ((crc & 1) == 1)
                {
                    crc = (crc >>> 1) ^ POLY;
                }
                else
                {
                    crc = (crc >>> 1);
                }
            }
            TABLE[n] = crc;
        }
    }

    /**
     * Construct new CRC.
     */
    public CRC64()
    {
        this.value = 0;
    }

    long recalculate(final byte[] b, final int offset, final int length)
    {
        value = 0;
        update(b, offset, length);
        return value;
    }

    long recalculate(final DirectBuffer b, final int offset, final int length)
    {
        value = 0;
        update(b, offset, length);
        return value;
    }

    /**
     * Update CRC64 with new byte block.
     *
     * @param b         input.
     * @param offset    the position to read from.
     * @param len       length to process.
     */
    public void update(final byte[] b, final int offset, final int len)
    {
        int remaining = len;
        int idx = offset;
        this.value = ~this.value;
        while (remaining > 0)
        {
            this.value = TABLE[((int)(this.value ^ b[idx])) & 0xff] ^ (this.value >>> 8);
            idx++;
            remaining--;
        }
        this.value = ~this.value;
    }

    /**
     * Update CRC64 with new DirectBuffer.
     *
     * @param b         input.
     * @param offset    the position to read from.
     * @param len       length to process.
     */
    public void update(final DirectBuffer b, final int offset, final int len)
    {
        int remaining = len;
        int idx = offset;
        this.value = ~this.value;
        while (remaining > 0)
        {
            this.value = TABLE[((int)(this.value ^ b.getByte(idx))) & 0xff] ^ (this.value >>> 8);
            idx++;
            remaining--;
        }
        this.value = ~this.value;
    }

    private static void test(final byte[] b, final int len, final long crcValue) throws Exception
    {
        /* Test CRC64 default calculation. */
        final CRC64 crc = new CRC64();

        final long recalculate = crc.recalculate(new UnsafeBuffer(b), 0, len);
        if (recalculate != crcValue)
        {
            throw new Exception(
                "mismatch: " + String.format("%016x", recalculate) +
                " should be " + String.format("%016x", crcValue));
        }
    }

    /**
     * Entry point.
     *
     * @param args command line args.
     * @throws Exception on failure.
     */
    public static void main(final String[] args) throws Exception
    {
        final byte[] test1 = "123456789".getBytes();
        final int testlen1 = 9;
        final long testcrc1 = 0x995dc9bbdf1939faL; // ECMA.
        test(test1, testlen1, testcrc1);

        final byte[] test2 = "This is a test of the emergency broadcast system.".getBytes();
        final int testlen2 = 49;
        final long testcrc2 = 0x27db187fc15bbc72L; // ECMA.
        test(test2, testlen2, testcrc2);

        final byte[] test3 = "IHATEMATH".getBytes();
        final int testlen3 = 9;
        final long testcrc3 = 0x3920e0f66b6ee0c8L; // ECMA.
        test(test3, testlen3, testcrc3);
    }
}