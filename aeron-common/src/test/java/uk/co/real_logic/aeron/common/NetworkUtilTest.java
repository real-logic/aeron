package uk.co.real_logic.aeron.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NetworkUtilTest
{

    @Test
    public void shouldNotMatchIfLengthsAreDifferent()
    {
        assertFalse(NetworkUtil.isMatchWithPrefix(new byte[0], new byte[3], 0));
        assertFalse(NetworkUtil.isMatchWithPrefix(new byte[1], new byte[2], 0));
        assertFalse(NetworkUtil.isMatchWithPrefix(new byte[5], new byte[5000], 0));
    }

    @Test
    public void shouldMatchIfAllBytesMatch() throws Exception
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'c', 'd' };
        assertTrue(NetworkUtil.isMatchWithPrefix(a, b, 32));
    }

    @Test
    public void shouldMatchIfAllBytesWithPrefixMatch() throws Exception
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'c', 'e' };
        assertTrue(NetworkUtil.isMatchWithPrefix(a, b, 24));
    }

    @Test
    public void shouldNotMatchIfNotAllBytesWithPrefixMatch() throws Exception
    {
        final byte[] a = { 'a', 'b', 'c', 'd' };
        final byte[] b = { 'a', 'b', 'd', 'd' };
        assertFalse(NetworkUtil.isMatchWithPrefix(a, b, 24));
    }

    @Test
    public void shouldMatchIfAllBytesWithPrefixUnalignedMatch() throws Exception
    {
        assertTrue(NetworkUtil.isMatchWithPrefix(
            asBytes(0b10101010_11111111_00000000_00000000),
            asBytes(0b10101010_11111110_00000000_00000000),
            15));
    }

    @Test
    public void shouldNotMatchIfNotAllBytesWithUnalignedPrefixMatch() throws Exception
    {
        assertFalse(NetworkUtil.isMatchWithPrefix(
            asBytes(0b10101010_11111111_00000000_00000000),
            asBytes(0b10101010_11111111_10000000_00000000),
            17));
    }

    private static byte[] asBytes(int i)
    {
        final byte[] bs = new byte[4];
        bs[0] = (byte) ((i >> 24) & 0xFF);
        bs[1] = (byte) ((i >> 16) & 0xFF);
        bs[2] = (byte) ((i >> 8) & 0xFF);
        bs[3] = (byte) (i & 0xFF);

        return bs;
    }
}
