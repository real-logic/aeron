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
package uk.co.real_logic.aeron.util;

/**
 * Miscellaneous useful functions for dealing with low level bits and bytes.
 */
public class BitUtil
{
    /** Size of a byte in bytes */
    public static final int SIZE_OF_BYTE = 1;
    /** Size of a boolean in bytes */
    public static final int SIZE_OF_BOOLEAN = 1;

    /** Size of a char in bytes */
    public static final int SIZE_OF_CHAR = 2;
    /** Size of a short in bytes */
    public static final int SIZE_OF_SHORT = 2;

    /** Size of an int in bytes */
    public static final int SIZE_OF_INT = 4;
    /** Size of a a float in bytes */
    public static final int SIZE_OF_FLOAT = 4;

    /** Size of a long in bytes */
    public static final int SIZE_OF_LONG = 8;
    /** Size of a double in bytes */
    public static final int SIZE_OF_DOUBLE = 8;

    /** Size of the data blocks used by the CPU cache sub-system in bytes. */
    public static final int CACHE_LINE_SIZE = 64;

    private static final byte[] HEX_DIGIT_TABLE = { '0', '1', '2', '3', '4', '5', '6', '7',
                                                    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * Fast method of finding the next power of 2 greater than or equal to the supplied value.
     *
     * If the value is <= 0 then 1 will be returned.
     *
     * This method is not suitable for {@link Integer#MIN_VALUE} or numbers greater than 2^30.
     *
     * @param value from which to search for next power of 2
     * @return The next power of 2 or the value itself if it is a power of 2
     */
    public static int findNextPositivePowerOfTwo(final int value)
    {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    /**
     * Align a value to the next multiple up of alignment.
     * If the value equals an alignment multiple then it is returned unchanged.
     * <p/>
     * This method executes without branching.
     *
     * @param value to be aligned up.
     * @return the value aligned to the next boundary.
     */
    public static int align(final int value, final int alignment)
    {
        return (value + (alignment - 1)) & ~(alignment - 1);
    }

    /**
     * Generate a byte array that is a hex representation of a given byte array.
     *
     * @param buffer to convert to a hex representation
     * @return new byte array that is hex representation (in Big Endian) of the passed array
     */
    public static byte[] toHexByteArray(final byte[] buffer)
    {
        final byte[] outputBuffer = new byte[buffer.length << 1];

        for (int i = 0; i < (buffer.length << 1); i += 2)
        {
            final byte b = buffer[i >> 1]; // readability

            outputBuffer[i] = HEX_DIGIT_TABLE[(b >> 4) & 0x0F];
            outputBuffer[i + 1] = HEX_DIGIT_TABLE[b & 0x0F];
        }
        return outputBuffer;
    }

    /**
     * Generate a string that is the hex representation of a given byte array.
     *
     * @param buffer to convert to a hex representation
     * @return new String holding the hex representation (in Big Endian) of the passed array
     */
    public static String toHex(final byte[] buffer) throws Exception
    {
        return new String(toHexByteArray(buffer), "UTF-8");
    }
}
