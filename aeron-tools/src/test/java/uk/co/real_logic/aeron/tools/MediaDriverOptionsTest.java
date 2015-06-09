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
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import uk.co.real_logic.aeron.driver.LossGenerator;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doReturn;

/**
 * Unit tests for the MediaDriverOptions class.
 */
public class MediaDriverOptionsTest
{
    MediaDriverOptions opts;

    /* IdleStrategy instantiated via reflection by MediaDriverOptions */
    static class TestIdleStrategy implements IdleStrategy
    {
        public void idle(final int workCount)
        {
        }
    }

    /* LossGenerator instantiated via reflection by MediaDriverOptions */
    static class TestLossGenerator implements LossGenerator
    {
        public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
        {
            return false;
        }
    }

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

        assertThat(opts.properties(), is(nullValue()));
        assertThat(opts.conductorIdleStrategy(), is(nullValue()));
        assertThat(opts.senderIdleStrategy(), is(nullValue()));
        assertThat(opts.receiverIdleStrategy(), is(nullValue()));
        assertThat(opts.sharedNetworkIdleStrategy(), is(nullValue()));
        assertThat(opts.sharedIdleStrategy(), is(nullValue()));
        assertThat(opts.dataLossGenerator(), is(nullValue()));
        assertThat(opts.controlLossGenerator(), is(nullValue()));
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
            spyOpts.properties(), is(not(nullValue())));
        assertThat(spyOpts.properties().getProperty("hello.world"), is("testing"));
    }

    @Test
    public void senderIdleStrategy() throws Exception
    {
        final String[] args = { "--sender", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.senderIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void receiverIdleStrategy() throws Exception
    {
        final String[] args = { "--receiver", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.receiverIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void conductorIdleStrategy() throws Exception
    {
        final String[] args = { "--conductor", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.conductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void sharedNetworkIdleStrategy() throws Exception
    {
        final String[] args = { "--network", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.sharedNetworkIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void sharedIdleStrategy() throws Exception
    {
        final String[] args = { "--shared", "uk.co.real_logic.aeron.tools.MediaDriverOptionsTest$TestIdleStrategy" };
        opts.parseArgs(args);
        assertThat(opts.sharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.senderIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.receiverIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.conductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.sharedNetworkIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.sharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.sharedIdleStrategy(), instanceOf(TestIdleStrategy.class));
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
        assertThat(spyOpts.sharedIdleStrategy(), is(nullValue()));
    }

    @Test
    public void testBackoffIdleStrategy() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final TestIdleStrategy testIdleStrategy = new TestIdleStrategy();
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() };
        doReturn(testIdleStrategy).when(spyOpts).newBackoffIdleStrategy(anyInt(), anyInt(), anyInt(), anyInt());

        spyOpts.parseArgs(args);
        // we should get back our test idle strategy, because we modified that newBackoffIdleStrategy method.
        assertThat(spyOpts.conductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test
    public void testBackoffIdleStrategyWithInput() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final TestIdleStrategy testIdleStrategy = new TestIdleStrategy();
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() + "(10, 20, 30 ,40)" };
        doReturn(testIdleStrategy).when(spyOpts).newBackoffIdleStrategy(10, 20, 30, 40);

        spyOpts.parseArgs(args);
        assertThat(spyOpts.conductorIdleStrategy(), instanceOf(TestIdleStrategy.class));
    }

    @Test (expected = ParseException.class)
    public void testBackoffIdleStrategyBadInput() throws Exception
    {
        final String[] args = { "--conductor", BackoffIdleStrategy.class.getName() + "(10,20,30)" };
        opts.parseArgs(args);
    }

    @Test
    public void testDataLossGenerator() throws Exception
    {
        final String[] args = { "--data-loss", TestLossGenerator.class.getName() };
        opts.parseArgs(args);
        assertThat(opts.dataLossGenerator(), instanceOf(TestLossGenerator.class));
    }

    @Test
    public void testControlLossGenerator() throws Exception
    {
        final String[] args = { "--control-loss", TestLossGenerator.class.getName() };
        opts.parseArgs(args);
        assertThat(opts.controlLossGenerator(), instanceOf(TestLossGenerator.class));
    }

    @Test
    public void testDataLossGeneratorFromProperties() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.data.loss=" + TestLossGenerator.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        final String[] args = { "--properties", "filename" };

        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");
        spyOpts.parseArgs(args);
        assertThat(spyOpts.dataLossGenerator(), instanceOf(TestLossGenerator.class));
    }

    @Test
    public void testControlLossGeneratorFromProperties() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.control.loss=" + TestLossGenerator.class.getName();
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        final String[] args = { "--properties", "filename" };

        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");
        spyOpts.parseArgs(args);
        assertThat(spyOpts.controlLossGenerator(), instanceOf(TestLossGenerator.class));
    }

    @Test
    public void testDataLossGeneratorCommandLineOverridesProperties() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.data.loss=null";
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        final String[] args = { "--properties", "filename", "--data-loss", TestLossGenerator.class.getName() };

        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");
        spyOpts.parseArgs(args);
        assertThat(spyOpts.dataLossGenerator(), instanceOf(TestLossGenerator.class));
    }

    @Test
    public void testControlLossGeneratorCommandLineOverridesProperties() throws Exception
    {
        final MediaDriverOptions spyOpts = Mockito.spy(opts);
        final String fileText = "aeron.tools.mediadriver.control.loss=null";
        final InputStream inputStream = new ByteArrayInputStream(fileText.getBytes());
        final String[] args = { "--properties", "filename", "--control-loss", TestLossGenerator.class.getName() };

        doReturn(inputStream).when(spyOpts).newFileInputStream("filename");
        spyOpts.parseArgs(args);
        assertThat(spyOpts.controlLossGenerator(), instanceOf(TestLossGenerator.class));
    }
}
