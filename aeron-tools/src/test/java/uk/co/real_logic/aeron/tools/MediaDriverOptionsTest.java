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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.commons.cli.ParseException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

/**
 * Created by bhorst on 4/13/15.
 */
public class MediaDriverOptionsTest
{
    MediaDriverOptions opts;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        opts = new MediaDriverOptions();
    }

    @Test
    public void help() throws Exception
    {
        final String[] args = { "--help" };
        assertThat(opts.parseArgs(args), is(1));
    }

    @Test
    public void helpShorthand() throws Exception
    {
        final String[] args = { "-h" };
        assertThat(opts.parseArgs(args), is(1));
    }

    @Test
    public void defaultsToNull() throws Exception
    {
        // everything should be NULL after calling parseArgs with no parameters
        final String[] args = { "" };
        opts.parseArgs(args);

        assertThat(opts.getProperties(), is(nullValue()));
        assertThat(opts.getConductorIdleStrategy(), is(nullValue()));
        assertThat(opts.getSenderIdleStrategy(), is(nullValue()));
        assertThat(opts.getReceiverIdleStrategy(), is(nullValue()));
        assertThat(opts.getSharedNetworkIdleStrategy(), is(nullValue()));
        assertThat(opts.getSharedIdleStrategy(), is(nullValue()));
    }

    @Test
    public void propertiesFile() throws Exception
    {
        // Use spy to return our own input stream.
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "hello.world=testing";
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat("FAIL: Properties object should have been created",
                spyOpts.getProperties(), is(not(nullValue())));
        assertThat(spyOpts.getProperties().getProperty("hello.world"), is("testing"));
    }

    /** Class instantiated via reflection by MediaDriverOptions */
    static class TestIdleStrategy implements IdleStrategy
    {
        @Override
        public void idle(final int workCount)
        {
        }
    }

    @Test
    public void senderIdleStrategy() throws Exception
    {
        final String[] args = { "--sender", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.getSenderIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void receiverIdleStrategy() throws Exception
    {
        final String[] args = { "--receiver", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.getReceiverIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void conductorIdleStrategy() throws Exception
    {
        final String[] args = { "--conductor", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.getConductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void sharedNetworkIdleStrategy() throws Exception
    {
        final String[] args = { "--network", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.getSharedNetworkIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void sharedIdleStrategy() throws Exception
    {
        final String[] args = { "--shared", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.getSharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void senderIdleStrategyProperty() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.sender=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getSenderIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void receiverIdleStrategyProperty() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.receiver=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getReceiverIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void conductorIdleStrategyProperty() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.conductor=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getConductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void netowrkIdleStrategyProperty() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.network=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getSharedNetworkIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void sharedIdleStrategyProperty() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.shared=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getSharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void commandLineOverrideStrategyFromProperties1() throws Exception
    {
        // Tests overriding null value from file to valid class.
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.shared=null";
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--shared", TestIdleStrategy.class.getName(), "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getSharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void commandLineOverrideStrategyFromProperties2() throws Exception
    {
        // Tests overriding valid class from file to null value.
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.shared=" + TestIdleStrategy.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");

        final String[] args = { "--shared", "null", "--properties", "filename" };
        spyOpts.parseArgs(args);
        assertThat(spyOpts.getSharedIdleStrategy(), is(nullValue()));
    }

    @Test
    public void testBackoffIdleStrategy() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final TestIdleStrategy testIdleStrategy = new TestIdleStrategy();
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() };
        doReturn(testIdleStrategy).when(spyOpts).makeBackoffIdleStrategy(anyInt(), anyInt(), anyInt(), anyInt());

        spyOpts.parseArgs(args);
        // we should get back our test idle strategy, because we modified that makeBackoffIdleStrategy method.
        assertThat(spyOpts.getConductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void testBackoffIdleStrategyWithInput() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final TestIdleStrategy testIdleStrategy = new TestIdleStrategy();
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() + "(10, 20, 30 ,40)" };
        doReturn(testIdleStrategy).when(spyOpts).makeBackoffIdleStrategy(10, 20, 30, 40);

        spyOpts.parseArgs(args);
        assertThat(spyOpts.getConductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test (expected = ParseException.class)
    public void testBackoffIdleStrategyBadInput() throws Exception
    {
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() + "(10,20,30)" };
        opts.parseArgs(args);
    }
}
