package uk.co.real_logic.aeron.common;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static uk.co.real_logic.aeron.common.UriUtil.parseQueryString;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class UriUtilTest
{
    @Test
    public void shouldParseSimpleQueryParameters() throws Exception
    {
        Map<String, String> params = doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24"));

        assertThat(params.size(), is(1));
        assertThat(params.get("mask"), is("24"));
    }

    @Test
    public void shouldParseWithMissingValuePart() throws Exception
    {
        Map<String, String> params = doParse(URI.create("http://example.com:12345/foo/bar.html?mask"));

        assertThat(params.containsKey("mask"), is(true));
        assertThat(params.get("mask"), is(""));
    }

    @Test
    public void shouldParseWithMultipleArguments() throws Exception
    {
        Map<String, String> params = doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24&wibble&bits=0110102"));

        assertThat(params.get("mask"), is("24"));
        assertThat(params.get("bits"), is("0110102"));
        assertThat(params.get("wibble"), is(""));
    }

    @Test
    public void shouldParseEncodedArguments() throws Exception
    {
        Map<String, String> params =
            doParse(URI.create(format(
                "http://example.com:12345/foo/bar.html?mask=24&ip=%s",
                URLEncoder.encode("FE80::0202:B3FF:FE1E:8329", "UTF-8"))));

        assertThat(params.get("ip"), is("FE80::0202:B3FF:FE1E:8329"));
    }

    @Test
    public void shouldAllowArgumentsThatAreNotEncodedButShouldBe() throws Exception
    {
        Map<String, String> params =
            doParse(URI.create(format(
                "http://example.com:12345/foo/bar.html?mask=24&ip=%s",
                "FE80::0202:B3FF:FE1E:8329")));

        assertThat(params.get("ip"), is("FE80::0202:B3FF:FE1E:8329"));
    }

    @Test(expected = MalformedURLException.class)
    public void shouldThrowMalformedUriExceptionIfQueryParametersAreInvalid() throws Exception
    {
        doParse(URI.create("http://example.com:12345/foo/bar.html?mask=24&ip=abc=4"));
    }

    private static Map<String, String> doParse(URI uri) throws MalformedURLException
    {
        return parseQueryString(uri, new HashMap<String, String>());
    }
}
