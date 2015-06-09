/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ThwackerOptionsTest
{
    private ThwackerOptions opts;

    @Before
    public void setUp()
    {
        opts = new ThwackerOptions();
    }

    @Test
    public void verifyShort() throws Exception
    {
        final String[] args = {"-v", "yes"};
        opts.parseArgs(args);
        assertThat(opts.verifiable(), is(true));
    }

    @Test
    public void verifyOn() throws Exception
    {
        final String[] args = {"--verify", "Yes"};
        opts.parseArgs(args);
        assertThat(opts.verifiable(), is(true));
    }

    @Test
    public void verifyOff() throws Exception
    {
        final String[] args = {"--verify", "No"};
        opts.parseArgs(args);
        assertThat(opts.verifiable(), is(false));
    }

    @Test
    public void verifyDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --verify should be false", opts.verifiable(), is(false));
    }

    @Test(expected = ParseException.class)
    public void verifyException() throws Exception
    {
        final String[] args = {"--verify", "schmerify"};
        opts.parseArgs(args);
    }

    @Test
    public void sameSIDOn() throws Exception
    {
        final String[] args = {"--same-sid", "Yes"};
        opts.parseArgs(args);
        assertThat(opts.sameSID(), is(true));
    }

    @Test
    public void sameSIDOff() throws Exception
    {
        final String[] args = {"--same-sid", "No"};
        opts.parseArgs(args);
        assertThat(opts.sameSID(), is(false));
    }

    @Test
    public void sameSIDDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --same-sid should be false", opts.sameSID(), is(false));
    }

    @Test(expected = ParseException.class)
    public void sameSIDException() throws Exception
    {
        final String[] args = {"--same-sid", "wrongstring"};
        opts.parseArgs(args);
    }

    @Test
    public void channelPerPubOn() throws Exception
    {
        final String[] args = {"--channel-per-pub", "Yes"};
        opts.parseArgs(args);
        assertThat(opts.channelPerPub(), is(true));
    }

    @Test
    public void channelPerPubOff() throws Exception
    {
        final String[] args = {"--channel-per-pub", "No"};
        opts.parseArgs(args);
        assertThat(opts.channelPerPub(), is(false));
    }

    @Test
    public void channelPerPubDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --channel-per-pub should be false", opts.channelPerPub(), is(false));
    }

    @Test(expected = ParseException.class)
    public void channelPerPubException() throws Exception
    {
        final String[] args = {"--channel-per-pub", "chanperpub"};
        opts.parseArgs(args);
    }

    @Test
    public void channelDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --channel should be udp://localhost", opts.channel(), is("udp://localhost"));
    }

    @Test
    public void channel() throws Exception
    {
        final String[] args = {"--channel", "blahblahblah"};
        opts.parseArgs(args);
        assertThat(opts.channel(), is("blahblahblah"));
    }

    @Test
    public void channelShort() throws Exception
    {
        final String[] args = {"-c", "blahblahblah"};
        opts.parseArgs(args);
        assertThat(opts.channel(), is("blahblahblah"));
    }

    @Test
    public void portPass() throws Exception
    {
        final String[] args = {"--port", "12345"};
        opts.parseArgs(args);
        assertThat(opts.port(), is(12345));
    }

    @Test
    public void portShortPass() throws Exception
    {
        final String[] args = {"-p", "12345"};
        opts.parseArgs(args);
        assertThat(opts.port(), is(12345));
    }

    @Test(expected = ParseException.class)
    public void portFail() throws Exception
    {
        final String[] args = {"--port", "-12345"};
        opts.parseArgs(args);
    }

    @Test
    public void portDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --port should be " + 51234, opts.port(), is(51234));
    }

    @Test
    public void durationPass() throws Exception
    {
        final String[] args = {"--duration", "77969403"};
        opts.parseArgs(args);
        assertThat(opts.duration(), is(77969403));
    }

    @Test
    public void durationShortPass() throws Exception
    {
        final String[] args = {"-d", "77969403"};
        opts.parseArgs(args);
        assertThat(opts.duration(), is(77969403));
    }

    @Test(expected = ParseException.class)
    public void durationFail() throws Exception
    {
        final String[] args = {"--duration", "-2345145"};
        opts.parseArgs(args);
    }

    @Test
    public void durationDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --duration should be '30000'", opts.duration(), is(30000));
    }

    @Test
    public void iterationsPass() throws Exception
    {
        final String[] args = {"--iterations", "7703"};
        opts.parseArgs(args);
        assertThat(opts.iterations(), is(7703));
    }

    @Test
    public void iterationsShortPass() throws Exception
    {
        final String[] args = {"-i", "7703"};
        opts.parseArgs(args);
        assertThat(opts.iterations(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void iterationsFail() throws Exception
    {
        final String[] args = {"--iterations", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void iterationsDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --iterations should be 1", opts.iterations(), is(1));
    }

    @Test
    public void sendersPass() throws Exception
    {
        final String[] args = {"--senders", "7703"};
        opts.parseArgs(args);
        assertThat(opts.senders(), is(7703));
    }

    @Test
    public void sendersShortPass() throws Exception
    {
        final String[] args = {"-s", "7703"};
        opts.parseArgs(args);
        assertThat(opts.senders(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void sendersFail() throws Exception
    {
        final String[] args = {"--senders", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void sendersDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --iterations should be 1", opts.senders(), is(1));
    }

    @Test
    public void receiversPass() throws Exception
    {
        final String[] args = {"--receivers", "7703"};
        opts.parseArgs(args);
        assertThat(opts.receivers(), is(7703));
    }

    @Test
    public void receiversShortPass() throws Exception
    {
        final String[] args = {"-r", "7703"};
        opts.parseArgs(args);
        assertThat(opts.receivers(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void receiversFail() throws Exception
    {
        final String[] args = {"--receivers", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void receiversDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --iterations should be 1", opts.receivers(), is(1));
    }

    @Test
    public void addersPass() throws Exception
    {
        final String[] args = {"--adders", "7703"};
        opts.parseArgs(args);
        assertThat(opts.adders(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void addersFail() throws Exception
    {
        final String[] args = {"--adders", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void addersDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --adders should be 1", opts.adders(), is(1));
    }

    @Test
    public void removersPass() throws Exception
    {
        final String[] args = {"--removers", "7703"};
        opts.parseArgs(args);
        assertThat(opts.removers(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void removersFail() throws Exception
    {
        final String[] args = {"--removers", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void removersDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --removers should be 1", opts.removers(), is(1));
    }

    @Test
    public void elementsPass() throws Exception
    {
        final String[] args = {"--elements", "7703"};
        opts.parseArgs(args);
        assertThat(opts.elements(), is(7703));
    }

    @Test
    public void elementsShortPass() throws Exception
    {
        final String[] args = {"-e", "7703"};
        opts.parseArgs(args);
        assertThat(opts.elements(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void elementsFail() throws Exception
    {
        final String[] args = {"--elements", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void elementsDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --elements should be 10", opts.elements(), is(10));
    }

    @Test
    public void maxSizePass() throws Exception
    {
        final String[] args = {"--max-size", "7703"};
        opts.parseArgs(args);
        assertThat(opts.maxMsgSize(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void maxSizeFail() throws Exception
    {
        final String[] args = {"--max-size", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void maxSizeDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --max-size should be 35", opts.maxMsgSize(), is(35));
    }

    @Test
    public void minSizePass() throws Exception
    {
        final String[] args = {"--min-size", "7703"};
        opts.parseArgs(args);
        assertThat(opts.minMsgSize(), is(7703));
    }

    @Test(expected = ParseException.class)
    public void minSizeFail() throws Exception
    {
        final String[] args = {"--min-size", "-235725"};
        opts.parseArgs(args);
    }

    @Test
    public void minSizeDefault() throws Exception
    {
        final String[] args = {};
        opts.parseArgs(args);
        assertThat("FAIL: Default for --min-size should be 35", opts.minMsgSize(), is(35));
    }

    @Test
    public void help() throws Exception
    {
        final String[] args = {"--help"};
        assertThat(opts.parseArgs(args), is(1));
    }

    @Test
    public void helpShorthand() throws Exception
    {
        final String[] args = {"-h"};
        assertThat(opts.parseArgs(args), is(1));
    }
}
