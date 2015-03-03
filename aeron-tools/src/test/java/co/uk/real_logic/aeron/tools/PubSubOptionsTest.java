package co.uk.real_logic.aeron.tools;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import uk.co.real_logic.aeron.tools.PubSubOptions;

/**
 * Created by bhorst on 3/3/15.
 */
public class PubSubOptionsTest
{
    PubSubOptions opts;
    @Before
    public void setUp()
    {
        opts = new PubSubOptions();
    }

    @Test
    public void threadsShorthandValid() throws Exception
    {
        String[] args = { "-t", "1234" };
        opts.parseArgs(args);
        assertThat(opts.getThreads(), is(1234L));
    }

    @Test
    public void threadsLonghandValid() throws Exception
    {
        String[] args = { "--threads", "1234" };
        opts.parseArgs(args);
        assertThat(opts.getThreads(), is(1234L));
    }

    @Test (expected=ParseException.class)
    public void threadsInvalid() throws Exception
    {
        String[] args = { "-t", "asdf" };
        opts.parseArgs(args);
    }

    @Test (expected=ParseException.class)
    public void threadsLonghandInvalid() throws Exception
    {
        String[] args = { "--threads", "asdf" };
        opts.parseArgs(args);
    }
}
