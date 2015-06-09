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
package uk.co.real_logic.aeron.driver;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.driver.UriUtil.parseQueryString;

public class UriUtilTest
{
    @Test
    public void shouldParseSimpleQueryParameters() throws Exception
    {
        final Map<String, String> params =
            doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24"));

        assertThat(params.size(), is(1));
        assertThat(params.get("mask"), is("24"));
    }

    @Test
    public void shouldParseWithMissingValuePart() throws Exception
    {
        final Map<String, String> params =
            doParse(URI.create("http://example.com:12345/foo/bar.html?mask"));

        assertThat(params.containsKey("mask"), is(true));
        assertThat(params.get("mask"), is(""));
    }

    @Test
    public void shouldParseWithMultipleArguments() throws Exception
    {
        final Map<String, String> params =
            doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24&wibble&bits=0110102"));

        assertThat(params.get("mask"), is("24"));
        assertThat(params.get("bits"), is("0110102"));
        assertThat(params.get("wibble"), is(""));
    }

    @Test
    public void shouldParseEncodedArguments() throws Exception
    {
        final Map<String, String> params =
            doParse(URI.create(format(
                "http://example.com:12345/foo/bar.html?mask=24&ip=%s",
                URLEncoder.encode("FE80::0202:B3FF:FE1E:8329", "UTF-8"))));

        assertThat(params.get("ip"), is("FE80::0202:B3FF:FE1E:8329"));
    }

    @Test
    public void shouldAllowArgumentsThatAreNotEncodedButShouldBe() throws Exception
    {
        final Map<String, String> params =
            doParse(URI.create(format(
                "http://example.com:12345/foo/bar.html?mask=24&ip=%s",
                "FE80::0202:B3FF:FE1E:8329")));

        assertThat(params.get("ip"), is("FE80::0202:B3FF:FE1E:8329"));
    }

    @Test(expected = URISyntaxException.class)
    public void shouldThrowMalformedUriExceptionIfQueryParametersAreInvalid() throws Exception
    {
        doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24&ip=abc=4"));
    }

    private static Map<String, String> doParse(final URI uri) throws URISyntaxException
    {
        return parseQueryString(uri, new HashMap<>());
    }
}
