/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.ChannelEndpointException;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.StatusIndicatorReader;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

public class ChannelEndpointErrorTest
{
    private static final String URI = "aeron:udp?endpoint=localhost:54326";
    private static final String URI_NO_CONFLICT = "aeron:udp?endpoint=localhost:54327";
    private static final String URI_WITH_INTERFACE_PORT =
        "aeron:udp?endpoint=localhost:54326|interface=localhost:34567";

    private static final int STREAM_ID = 1;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.DEDICATED;

    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR =
        IoUtil.tmpDirName() + "aeron-system-tests-" + UUID.randomUUID().toString() + File.separator;

    private Aeron clientA;
    private Aeron clientB;
    private Aeron clientC;
    private MediaDriver driverA;
    private MediaDriver driverB;
    private Publication publicationA;
    private Publication publicationB;
    private Subscription subscriptionA;
    private Subscription subscriptionB;
    private Subscription subscriptionC;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);

    private ErrorHandler errorHandlerClientA = mock(ErrorHandler.class);
    private ErrorHandler errorHandlerClientB = mock(ErrorHandler.class);
    private ErrorHandler errorHandlerClientC = mock(ErrorHandler.class);
    private ArgumentCaptor<Throwable> captorA = ArgumentCaptor.forClass(Throwable.class);
    private ArgumentCaptor<Throwable> captorB = ArgumentCaptor.forClass(Throwable.class);
    private ArgumentCaptor<Throwable> captorC = ArgumentCaptor.forClass(Throwable.class);

    private void launch()
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        final MediaDriver.Context driverAContext = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(THREADING_MODE);

        final MediaDriver.Context driverBContext = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .threadingMode(THREADING_MODE);

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverAContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientA));

        clientB = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientB));

        clientC = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientC));
    }

    @After
    public void closeEverything()
    {
        CloseHelper.quietClose(publicationA);
        CloseHelper.quietClose(publicationB);
        CloseHelper.quietClose(subscriptionA);
        CloseHelper.quietClose(subscriptionB);
        CloseHelper.quietClose(subscriptionC);

        CloseHelper.quietClose(clientC);
        CloseHelper.quietClose(clientB);
        CloseHelper.quietClose(clientA);

        driverB.close();
        driverA.close();

        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test(timeout = 2000)
    public void shouldBeAbleToQueryChannelStatusForSubscription() throws Exception
    {
        launch();

        subscriptionA = clientA.addSubscription(URI, STREAM_ID);

        final StatusIndicatorReader statusIndicatorReader = subscriptionA.channelStatusIndicator();

        while (statusIndicatorReader.getVolatile() != ChannelEndpointStatus.ACTIVE)
        {
            Thread.sleep(1);
        }

        assertThat(statusIndicatorReader.getVolatile(), is(ChannelEndpointStatus.ACTIVE));
    }

    @Test(timeout = 2000)
    public void shouldBeAbleToQueryChannelStatusForPublication() throws Exception
    {
        launch();

        publicationA = clientA.addPublication(URI, STREAM_ID);

        final StatusIndicatorReader statusIndicatorReader = publicationA.channelStatusIndicator();

        while (statusIndicatorReader.getVolatile() != ChannelEndpointStatus.ACTIVE)
        {
            Thread.sleep(1);
        }

        assertThat(statusIndicatorReader.getVolatile(), is(ChannelEndpointStatus.ACTIVE));
    }

    @Test(timeout = 2000)
    public void shouldCatchErrorOnAddressAlreadyInUseForSubscriptions() throws Exception
    {
        launch();

        subscriptionA = clientA.addSubscription(URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(URI, STREAM_ID);

        verify(errorHandlerClientB, timeout(1000)).onError(captorB.capture());

        assertThat(captorB.getValue(), instanceOf(ChannelEndpointException.class));

        final ChannelEndpointException channelEndpointException = (ChannelEndpointException)captorB.getValue();
        final long status = clientB.countersReader().getCounterValue(channelEndpointException.statusIndicatorId());

        assertThat(status, is(ChannelEndpointStatus.ERRORED));
        assertThat(subscriptionB.channelStatusIndicator().id(), is(channelEndpointException.statusIndicatorId()));
    }

    @Test(timeout = 2000)
    public void shouldCatchErrorOnAddressAlreadyInUseForPublications() throws Exception
    {
        launch();

        publicationA = clientA.addPublication(URI_WITH_INTERFACE_PORT, STREAM_ID);
        publicationB = clientB.addPublication(URI_WITH_INTERFACE_PORT, STREAM_ID);

        verify(errorHandlerClientB, timeout(1000)).onError(captorB.capture());

        assertThat(captorB.getValue(), instanceOf(ChannelEndpointException.class));

        final ChannelEndpointException channelEndpointException = (ChannelEndpointException)captorB.getValue();
        final long status = clientB.countersReader().getCounterValue(channelEndpointException.statusIndicatorId());

        assertThat(status, is(ChannelEndpointStatus.ERRORED));
        assertThat(publicationB.channelStatusIndicator().id(), is(channelEndpointException.statusIndicatorId()));
    }

    @Test(timeout = 2000)
    public void shouldNotErrorOnAddressAlreadyInUseOnActiveChannelEndpointForSubscripions() throws Exception
    {
        launch();

        subscriptionA = clientA.addSubscription(URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(URI_NO_CONFLICT, STREAM_ID);
        subscriptionC = clientC.addSubscription(URI, STREAM_ID);

        verify(errorHandlerClientC, timeout(1000)).onError(captorC.capture());
        verify(errorHandlerClientB, after(500).never()).onError(any());
    }
}
