/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.uri;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class AeronUriTest
{
    @Test
    public void shouldParseSimpleDefaultUri()
    {
        assertParseWithMedia("aeron:udp", "udp");
        assertParseWithMedia("aeron:ipc", "ipc");
        assertParseWithMedia("aeron:", "");
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
    public void shouldRejectWithOutOfPlaceColon() throws Exception
    {
        assertInvalid("aeron:udp:");
    }

    @Test
    public void shouldParseWithSingleParameter() throws Exception
    {
        assertParseWithParams("aeron:udp?address=224.10.9.8", "address", "224.10.9.8");
        assertParseWithParams("aeron:udp?add|ress=224.10.9.8", "add|ress", "224.10.9.8");
        assertParseWithParams("aeron:udp?address=224.1=0.9.8", "address", "224.1=0.9.8");
    }

    @Test
    public void shouldParseWithMultipleParameters() throws Exception
    {
        assertParseWithParams(
            "aeron:udp?address=224.10.9.8|port=4567|interface=192.168.0.3",
            "address", "224.10.9.8",
            "port", "4567",
            "interface", "192.168.0.3");
    }

    @Test
    public void shouldAllowReturnDefaultIfParamNotSpecified() throws Exception
    {
        final AeronUri uri = AeronUri.parse("aeron:udp?address=224.10.9.8");
        assertThat(uri.get("interface"), is(nullValue()));
        assertThat(uri.get("interface", "192.168.0.0"), is("192.168.0.0"));
    }

    private void assertParseWithParams(String uriStr, String...params)
    {
        if (params.length % 2 != 0)
        {
            throw new IllegalArgumentException();
        }

        final AeronUri uri = AeronUri.parse(uriStr);

        for (int i = 0; i < params.length; i += 2)
        {
            assertThat(uri.get(params[i]), is(params[i + 1]));
        }
    }

    private void assertParseWithMedia(String uriStr, String media)
    {
        final AeronUri uri = AeronUri.parse(uriStr);
        assertThat(uri.scheme(), is("aeron"));
        assertThat(uri.media(), is(media));
    }

    private static void assertInvalid(String string)
    {
        try
        {
            AeronUri.parse(string);
            fail(IllegalArgumentException.class.getName() + " not thrown");
        }
        catch (final IllegalArgumentException ignore)
        {
        }
    }
}
