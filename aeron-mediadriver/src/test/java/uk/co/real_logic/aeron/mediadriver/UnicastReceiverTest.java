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

import org.junit.*;
import uk.co.real_logic.aeron.mediadriver.buffer.BasicBufferManagementStrategy;
import uk.co.real_logic.aeron.util.AdminBuffers;
import uk.co.real_logic.aeron.util.SharedDirectories;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.ErrorHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.stream.IntStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.ErrorCode.INVALID_DESTINATION;
import static uk.co.real_logic.aeron.util.ErrorCode.RECEIVER_NOT_REGISTERED;
import static uk.co.real_logic.aeron.util.command.ControlProtocolEvents.*;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferTestUtil.assertEventRead;

public class UnicastReceiverTest
{
    private static final String URI = "udp://localhost:45678";
    private static final String INVALID_URI = "udp://";
    private static final long[] ONE_CHANNEL = { 10 };
    private static final long[] ANOTHER_CHANNEL = { 20 };
    private static final long[] THREE_CHANNELS = { 10, 20, 30 };
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long TERM_ID = 0xec1L;

    @ClassRule
    public static AdminBuffers buffers = new AdminBuffers(COMMAND_BUFFER_SZ + TRAILER_SIZE);

    @ClassRule
    public static SharedDirectories directory = new SharedDirectories();

    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final ErrorHeaderFlyweight error = new ErrorHeaderFlyweight();
    private final ReceiverMessageFlyweight receiverMessage = new ReceiverMessageFlyweight();

    private SenderThread senderThread;
    private ReceiverThread receiverThread;
    private MediaDriverAdminThread mediaDriverAdminThread;
    private DatagramChannel senderChannel;

    @Before
    public void setUp() throws Exception
    {
        final MediaDriver.TopologyBuilder builder = new MediaDriver.TopologyBuilder()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(buffers.strategy())
                .bufferManagementStrategy(new BasicBufferManagementStrategy(directory.dataDir()));

        senderThread = mock(SenderThread.class);
        receiverThread = new ReceiverThread(builder);
        mediaDriverAdminThread = new MediaDriverAdminThread(builder, receiverThread, senderThread);
        senderChannel = DatagramChannel.open();
        sendBuffer.clear();
    }

    @After
    public void tearDown() throws Exception
    {
        senderChannel.close();
        receiverThread.close();
        receiverThread.nioSelector().selectNowWithNoProcessing();
        mediaDriverAdminThread.close();
        mediaDriverAdminThread.nioSelector().selectNowWithNoProcessing();
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddReceiver() throws Exception
    {
        writeReceiverMessage(ADD_RECEIVER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForInvalidUri() throws Exception
    {
        writeReceiverMessage(ADD_RECEIVER, INVALID_URI, ONE_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(INVALID_DESTINATION));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(INVALID_URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForRemovingNonExistentReceiver() throws Exception
    {
        writeReceiverMessage(REMOVE_RECEIVER, URI, ONE_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(RECEIVER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ONE_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 1000)
    public void shouldSendErrorForRemovingReceiverFromWrongChannels() throws Exception
    {
        writeReceiverMessage(ADD_RECEIVER, URI, ONE_CHANNEL);
        writeReceiverMessage(REMOVE_RECEIVER, URI, ANOTHER_CHANNEL);

        processThreads(5);

        assertEventRead(buffers.toApi(), (eventTypeId, buffer, index, length) ->
        {
            assertThat(eventTypeId, is(ERROR_RESPONSE));

            error.wrap(buffer, index);
            assertThat(error.errorCode(), is(RECEIVER_NOT_REGISTERED));
            assertThat(error.errorStringLength(), is(0));

            receiverMessage.wrap(buffer, error.offendingHeaderOffset());
            assertThat(receiverMessage.channelIds(), is(ANOTHER_CHANNEL));
            assertThat(receiverMessage.destination(), is(URI));
        });
    }

    @Test(timeout = 2000)
    public void shouldBeAbleToCreateRcvTermOnZeroLengthData() throws Exception
    {
        writeReceiverMessage(ADD_RECEIVER, URI, ONE_CHANNEL);

        processThreads(5);

        final UdpDestination dest = UdpDestination.parse(URI);

        assertNotNull(receiverThread.frameHandler(dest));

        sendDataFrame(dest, ONE_CHANNEL[0], 0x0);

        processThreads(5);

        // TODO: finish. assert on term buffer existence.
        // final File termFile = termLocation(new File(adminPath), SESSION_ID, ONE_CHANNEL[0], TERM_ID, false, URI);
        //assertThat(termFile.exists(), is(true));

        processThreads(5);

        // once the buffer is attached, the receiver should send back an SM
        final ByteBuffer rcvBuffer = ByteBuffer.allocateDirect(256);
        final InetSocketAddress rcvAddr = (InetSocketAddress)senderChannel.receive(rcvBuffer);

        statusMessage.wrap(rcvBuffer);
        assertNotNull(rcvAddr);
        assertThat(rcvAddr.getPort(), is(dest.remoteData().getPort()));
        assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
        assertThat(statusMessage.channelId(), is(ONE_CHANNEL[0]));
        assertThat(statusMessage.sessionId(), is(SESSION_ID));
        assertThat(statusMessage.termId(), is(TERM_ID));
        assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.LENGTH));
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddThenRemoveReceiverWithoutTermBuffer() throws Exception
    {
        writeReceiverMessage(ADD_RECEIVER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNotNull(receiverThread.frameHandler(UdpDestination.parse(URI)));

        writeReceiverMessage(ControlProtocolEvents.REMOVE_RECEIVER, URI, ONE_CHANNEL);

        processThreads(5);

        assertNull(receiverThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToRemoveReceiverWithSingleTermBuffer()
    {
        // TODO: finish
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToHandleTermBufferRolloverCorrectly()
    {
        // TODO: finish
    }

    private void writeReceiverMessage(final int eventTypeId, final String destination, final long[] channelIds)
            throws IOException
    {
        final RingBuffer adminCommands = buffers.mappedToMediaDriver();

        receiverMessage.wrap(writeBuffer, 0);

        receiverMessage.channelIds(channelIds)
                                .destination(destination);

        adminCommands.write(eventTypeId, writeBuffer, 0, receiverMessage.length());
    }

    private void sendDataFrame(final UdpDestination destination, final long channelId, final int seqNum)
            throws Exception
    {
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        dataHeaderFlyweight.wrap(writeBuffer, 0);

        dataHeaderFlyweight.sequenceNumber(seqNum)
                           .sessionId(SESSION_ID)
                           .channelId(channelId)
                           .termId(TERM_ID)
                           .version(HeaderFlyweight.CURRENT_VERSION)
                           .flags((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                           .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                           .frameLength(DataHeaderFlyweight.HEADER_LENGTH);

        sendBuffer.position(0);
        sendBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);
        senderChannel.send(sendBuffer, destination.remoteData());
    }

    private void processThreads(final int iterations)
    {
        IntStream.range(0, iterations).forEach((i) ->
        {
            mediaDriverAdminThread.process();
            receiverThread.process();
        });
    }
}
