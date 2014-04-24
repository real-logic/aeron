/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagementStrategy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;

/**
 * .
 */
public class ReceiverThreadTest
{
    private static final String URI = "udp://localhost:45678";
    private static final long CHANNEL_ID = 10;
    private static final long[] ONE_CHANNEL = { CHANNEL_ID };

    private ReceiverThread thread;
    private BufferManagementStrategy bufferManagementStrategy;
    private ReceiverThreadCursor cursor;
    private RcvFrameHandlerFactory frameHandlerFactory;

    @Before
    public void setup() throws Exception
    {
        bufferManagementStrategy = mock(BufferManagementStrategy.class);
        frameHandlerFactory = mock(RcvFrameHandlerFactory.class);

        final MediaDriver.MediaDriverContext context = new MediaDriver.MediaDriverContext()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .bufferManagementStrategy(bufferManagementStrategy)
                .rcvFrameHandlerFactory(frameHandlerFactory);

        cursor = new ReceiverThreadCursor(context.receiverThreadCommandBuffer(), context.rcvNioSelector());
        thread = new ReceiverThread(context);
    }

    @Test
    public void addingConsumerShouldCreateHandler() throws Exception
    {
        UdpDestination destination = UdpDestination.parse(URI);
        RcvFrameHandler frameHandler = mock(RcvFrameHandler.class);
        Mockito.when(frameHandlerFactory.newInstance(destination)).thenReturn(frameHandler);

        cursor.addNewConsumerEvent(URI, ONE_CHANNEL);
        thread.process();

        verify(frameHandlerFactory).newInstance(destination);
        verify(frameHandler).addChannels(ONE_CHANNEL);
    }

}
