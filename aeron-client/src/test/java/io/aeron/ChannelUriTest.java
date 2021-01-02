/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ChannelUriTest
{
    @Test
    public void shouldParseSimpleDefaultUri()
    {
        assertParseWithMedia("aeron:udp", "udp");
        assertParseWithMedia("aeron:ipc", "ipc");
        assertParseWithMediaAndPrefix("aeron-spy:aeron:ipc", "aeron-spy", "ipc");
    }

    @Test
    public void shouldRejectUriWithoutAeronPrefix()
    {
        assertInvalid(":udp");
        assertInvalid("aeron");
        assertInvalid("aron:");
        assertInvalid("eeron:");
    }

    @Test
    public void shouldRejectWithOutOfPlaceColon()
    {
        assertInvalid("aeron:udp:");
    }

    @Test
    public void shouldRejectWithInvalidMedia()
    {
        assertInvalid("aeron:ipcsdfgfdhfgf");
    }

    @Test
    public void shouldRejectWithMissingQuerySeparatorWhenFollowedWithParams()
    {
        assertInvalid("aeron:ipc|sparse=true");
    }

    @Test
    public void shouldRejectWithInvalidParams()
    {
        assertInvalid("aeron:udp?endpoint=localhost:4652|-~@{]|=??#s!£$%====");
    }

    @Test
    public void shouldParseWithMultipleParameters()
    {
        assertParseWithParams(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16",
            "endpoint", "224.10.9.8",
            "port", "4567",
            "interface", "192.168.0.3",
            "ttl", "16");
    }

    @Test
    public void shouldAllowReturnDefaultIfParamNotSpecified()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=224.10.9.8");
        assertNull(uri.get("interface"));
        assertEquals("192.168.0.0", uri.get("interface", "192.168.0.0"));
    }

    @Test
    public void shouldRoundTripToString()
    {
        final String uriString = "aeron:udp?endpoint=224.10.9.8:777";
        final ChannelUri uri = ChannelUri.parse(uriString);

        final String result = uri.toString();
        assertEquals(uriString, result);
    }

    @Test
    public void shouldRoundTripToStringBuilder()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.10.9.8:777");
        final String uriString = builder.build();
        final ChannelUri uri = ChannelUri.parse(uriString);

        assertEquals(uriString, uri.toString());
    }

    @Test
    public void shouldRoundTripToStringBuilderWithPrefix()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .prefix(ChannelUri.SPY_QUALIFIER)
            .media("udp")
            .endpoint("224.10.9.8:777");
        final String uriString = builder.build();
        final ChannelUri uri = ChannelUri.parse(uriString);

        assertEquals(uriString, uri.toString());
    }

    @Test
    void equalsReturnsTrueWhenTheSameInstance()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        assertEquals(channelUri, channelUri);
    }

    @Test
    void equalsReturnsFalseIfComparedWithNull()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        assertNotEquals(channelUri, null);
    }

    @Test
    void equalsReturnsFalseIfComparedAnotherClass()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        //noinspection AssertBetweenInconvertibleTypes
        assertNotEquals(channelUri, 123);
    }

    @ParameterizedTest
    @MethodSource("equalityValues")
    void equality(final String uri1, final String uri2, final boolean expected)
    {
        final ChannelUri parsedUri1 = ChannelUri.parse(uri1);
        final ChannelUri parsedUri2 = ChannelUri.parse(uri2);

        assertEquals(expected, parsedUri1.equals(parsedUri2));
    }

    @ParameterizedTest
    @MethodSource("equalityValues")
    void hashCode(final String uri1, final String uri2, final boolean expected)
    {
        final ChannelUri parsedUri1 = ChannelUri.parse(uri1);
        final ChannelUri parsedUri2 = ChannelUri.parse(uri2);

        if (expected)
        {
            assertEquals(parsedUri1.hashCode(), parsedUri2.hashCode());
        }
        else
        {
            assertNotEquals(parsedUri1.hashCode(), parsedUri2.hashCode());
        }
    }

    private static List<Arguments> equalityValues()
    {
        return asList(
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron:udp?endpoint=224.10.9.8", true),
            arguments(
                "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16",
                "aeron:udp?port=4567|endpoint=224.10.9.8|ttl=16|interface=192.168.0.3",
                true),
            arguments(
                "aeron:udp?endpoint=224.10.9.8|tags=1,2",
                "aeron:udp?tags=1,2|endpoint=224.10.9.8",
                true),
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron-spy:aeron:udp?endpoint=224.10.9.8", false),
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron:ipc?endpoint=224.10.9.8", false),
            arguments("aeron:udp?endpoint=224.10.9.9", "aeron:udp?endpoint=224.10.8.8", false),
            arguments("aeron:udp?endpoint=224.10.9.8|ttl=16", "aeron:ipc?endpoint=224.10.9.8|port=4567", false),
            arguments("aeron:udp?endpoint=224.10.9.8|tags=2,1", "aeron:udp?endpoint=224.10.9.8|tags=1,2", false)
        );
    }

    private void assertParseWithParams(final String uriStr, final String... params)
    {
        if (params.length % 2 != 0)
        {
            throw new IllegalArgumentException();
        }

        final ChannelUri uri = ChannelUri.parse(uriStr);

        for (int i = 0; i < params.length; i += 2)
        {
            assertEquals(params[i + 1], uri.get(params[i]));
        }
    }

    private void assertParseWithMedia(final String uriStr, final String media)
    {
        assertParseWithMediaAndPrefix(uriStr, "", media);
    }

    private void assertParseWithMediaAndPrefix(final String uriStr, final String prefix, final String media)
    {
        final ChannelUri uri = ChannelUri.parse(uriStr);
        assertEquals("aeron", uri.scheme());
        assertEquals(prefix, uri.prefix());
        assertEquals(media, uri.media());
    }

    private static void assertInvalid(final String string)
    {
        try
        {
            ChannelUri.parse(string);
            fail(IllegalArgumentException.class.getName() + " not thrown");
        }
        catch (final IllegalArgumentException | IllegalStateException ignore)
        {
        }
    }
}
