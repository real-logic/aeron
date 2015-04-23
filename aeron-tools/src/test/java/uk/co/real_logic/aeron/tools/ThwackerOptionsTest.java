package uk.co.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Created by mike on 4/16/2015.
 */
public class ThwackerOptionsTest
{
    public class PubSubOptionsTest
    {
        ThwackerOptions opts;

        @Before
        public void setUp()
        {
            MockitoAnnotations.initMocks(this);
            opts = new ThwackerOptions();
        }

        @Test
        public void verifyShort() throws Exception
        {
            String[] args = {"-v yes"};
            opts.parseArgs(args);
            assertThat(opts.getVerifiable(), is(true));
        }

        @Test
        public void verifyOn() throws Exception
        {
            String[] args = { "--verify", "Yes" };
            opts.parseArgs(args);
            assertThat(opts.getVerifiable(), is(true));
        }

        @Test
        public void verifyOff() throws Exception
        {
            String[] args = { "--verify", "No" };
            opts.parseArgs(args);
            assertThat(opts.getVerifiable(), is(false));
        }

        @Test
        public void verifyDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --verify should be " + opts.defaultVerifiableMessageStream,
                    opts.getVerifiable(), is(opts.defaultVerifiableMessageStream));
        }

        @Test (expected=ParseException.class)
        public void verifyException() throws Exception
        {
            String[] args = { "--verify", "schmerify" };
            opts.parseArgs(args);
        }


        @Test
        public void sameSIDOn() throws Exception
        {
            String[] args = { "--same-sid", "Yes" };
            opts.parseArgs(args);
            assertThat(opts.getSameSID(), is(true));
        }

        @Test
        public void sameSIDOff() throws Exception
        {
            String[] args = { "--same-sid", "No" };
            opts.parseArgs(args);
            assertThat(opts.getSameSID(), is(false));
        }

        @Test
        public void sameSIDDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --same-sid should be " + opts.defaultUseSameSID,
                    opts.getSameSID(), is(opts.defaultUseSameSID));
        }

        @Test (expected=ParseException.class)
        public void sameSIDException() throws Exception
        {
            String[] args = { "--same-sid", "wrongstring" };
            opts.parseArgs(args);
        }


        @Test
        public void channelPerPubOn() throws Exception
        {
            String[] args = { "--channel-per-pub", "Yes" };
            opts.parseArgs(args);
            assertThat(opts.getChannelPerPub(), is(true));
        }

        @Test
        public void channelPerPubOff() throws Exception
        {
            String[] args = { "--channel-per-pub", "No" };
            opts.parseArgs(args);
            assertThat(opts.getChannelPerPub(), is(false));
        }

        @Test
        public void channelPerPubDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --channel-per-pub should be " + opts.defaultUseChannelPerPub,
                    opts.getChannelPerPub(), is(opts.defaultUseChannelPerPub));
        }

        @Test (expected=ParseException.class)
        public void channelPerPubException() throws Exception
        {
            String[] args = { "--channel-per-pub", "chanperpub" };
            opts.parseArgs(args);
        }
        @Test
        public void channelDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --channel should be " + opts.defaultChannel,
                    opts.getChannel(), is(opts.defaultChannel));
        }
        @Test
        public void channel() throws Exception
        {
            String[] args = { "--channel", "blahblahblah" };
            opts.parseArgs(args);
            assertThat(opts.getChannel(), is("blahblahblah"));
        }
        @Test
        public void channelShort() throws Exception
        {
            String[] args = { "-c", "blahblahblah" };
            opts.parseArgs(args);
            assertThat(opts.getChannel(), is("blahblahblah"));
        }
        @Test
        public void portPass() throws Exception
        {
            String[] args = { "--port", "12345" };
            opts.parseArgs(args);
            assertThat(opts.getPort(), is(12345));
        }
        @Test
        public void portShortPass() throws Exception
        {
            String[] args = { "-p", "12345" };
            opts.parseArgs(args);
            assertThat(opts.getPort(), is(12345));
        }
        @Test (expected=ParseException.class)
        public void portFail() throws Exception
        {
            String[] args = { "--port", "-12345" };
            opts.parseArgs(args);
        }
        @Test
        public void portDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --port should be " + opts.defaultPort,
                    opts.getPort(), is(opts.defaultPort));
        }
        @Test
        public void durationPass() throws Exception
        {
            String[] args = { "--duration", "77969403" };
            opts.parseArgs(args);
            assertThat(opts.getDuration(), is(77969403));
        }
        @Test
        public void durationShortPass() throws Exception
        {
            String[] args = { "-d", "77969403" };
            opts.parseArgs(args);
            assertThat(opts.getDuration(), is(77969403));
        }
        @Test (expected=ParseException.class)
        public void durationFail() throws Exception
        {
            String[] args = { "--duration", "-2345145" };
            opts.parseArgs(args);
        }
        @Test
        public void durationDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --duration should be " + opts.defaultDuration,
                    opts.getDuration(), is(opts.defaultDuration));
        }
        @Test
         public void iterationsPass() throws Exception
        {
            String[] args = { "--iterations", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getIterations(), is(7703));
        }
        @Test
        public void iterationsShortPass() throws Exception
        {
            String[] args = { "-i", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getIterations(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void iterationsFail() throws Exception
        {
            String[] args = { "--iterations", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void iterationsDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --iterations should be " + opts.defaultIterations,
                    opts.getIterations(), is(opts.defaultIterations));
        }
        @Test
        public void sendersPass() throws Exception
        {
            String[] args = { "--senders", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getSenders(), is(7703));
        }
        @Test
        public void sendersShortPass() throws Exception
        {
            String[] args = { "-s", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getSenders(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void sendersFail() throws Exception
        {
            String[] args = { "--senders", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void sendersDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --iterations should be " + opts.defaultSenders,
                    opts.getSenders(), is(opts.defaultSenders));
        }
        @Test
        public void receiversPass() throws Exception
        {
            String[] args = { "--receivers", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getReceivers(), is(7703));
        }
        @Test
        public void receiversShortPass() throws Exception
        {
            String[] args = { "-r", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getReceivers(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void receiversFail() throws Exception
        {
            String[] args = { "--receivers", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void receiversDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --iterations should be " + opts.defaultReceivers,
                    opts.getReceivers(), is(opts.defaultReceivers));
        }
        @Test
        public void addersPass() throws Exception
        {
            String[] args = { "--adders", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getAdders(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void addersFail() throws Exception
        {
            String[] args = { "--adders", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void addersDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --adders should be " + opts.defaultAdders,
                    opts.getAdders(), is(opts.defaultAdders));
        }
        @Test
        public void removersPass() throws Exception
        {
            String[] args = { "--removers", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getRemovers(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void removersFail() throws Exception
        {
            String[] args = { "--removers", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void removersDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --removers should be " + opts.defaultRemovers,
                    opts.getRemovers(), is(opts.defaultRemovers));
        }
        @Test
        public void elementsPass() throws Exception
        {
            String[] args = { "--elements", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getElements(), is(7703));
        }
        @Test
        public void elementsShortPass() throws Exception
        {
            String[] args = { "-e", "7703" };
            opts.parseArgs(args);
            assertThat(opts.getElements(), is(7703));
        }
        @Test (expected=ParseException.class)
        public void elementsFail() throws Exception
        {
            String[] args = { "--elements", "-235725" };
            opts.parseArgs(args);
        }
        @Test
        public void elementsDefault() throws Exception
        {
            String[] args = { };
            opts.parseArgs(args);
            assertThat("FAIL: Default for --elements should be " + opts.defaultElements,
                    opts.getElements(), is(opts.defaultElements));
        }
    }
}
