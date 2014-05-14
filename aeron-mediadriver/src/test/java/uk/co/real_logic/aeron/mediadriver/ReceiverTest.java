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
import org.mockito.Mockito;
import uk.co.real_logic.aeron.mediadriver.buffer.BufferManagement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;

/**
 * .
 */
public class ReceiverTest
{
    private static final String URI = "udp://localhost:45678";
    private static final long CHANNEL_ID = 10;
    private static final long[] ONE_CHANNEL = { CHANNEL_ID };

    private Receiver receiver;
    private ReceiverCursor cursor;
    private RcvFrameHandlerFactory frameHandlerFactory;

    @Before
    public void setUp() throws Exception
    {
        final BufferManagement bufferManagement = mock(BufferManagement.class);
        frameHandlerFactory = mock(RcvFrameHandlerFactory.class);

        final MediaDriver.Context context = new MediaDriver.Context()
                .conductorCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .bufferManagement(bufferManagement)
                .rcvFrameHandlerFactory(frameHandlerFactory);

        cursor = new ReceiverCursor(context.receiverCommandBuffer(), context.rcvNioSelector());
        receiver = new Receiver(context);
    }

    @Test
    public void addingSubscriberShouldCreateHandler() throws Exception
    {
        UdpDestination destination = UdpDestination.parse(URI);
        RcvFrameHandler frameHandler = mock(RcvFrameHandler.class);
        Mockito.when(frameHandlerFactory.newInstance(destination, receiver.sessionState())).thenReturn(frameHandler);

        cursor.addNewSubscriberEvent(URI, ONE_CHANNEL);
        receiver.process();

        verify(frameHandlerFactory).newInstance(destination, receiver.sessionState());
        verify(frameHandler).addChannels(ONE_CHANNEL);
    }
}
