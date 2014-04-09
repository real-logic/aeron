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
import uk.co.real_logic.aeron.util.CreatingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.IoUtil;
import uk.co.real_logic.aeron.util.MappingAdminBufferStrategy;
import uk.co.real_logic.aeron.util.command.ChannelMessageFlyweight;
import uk.co.real_logic.aeron.util.command.ControlProtocolEvents;
import uk.co.real_logic.aeron.util.command.ReceiverMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.mediadriver.MediaDriver.COMMAND_BUFFER_SZ;
import static uk.co.real_logic.aeron.util.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_SIZE;

public class UnicastReceiverTest
{
    private static final String ADMIN_DIR = "adminDir";
    private static final String URI = "udp://localhost:45678";
    private static final String INVALID_URI = "udp://";
    private static final long[] ONE_CHANNEL = { 10 };
    private static final long[] THREE_CHANNELS = { 10, 20, 30 };
    private static final long SESSION_ID = 0xdeadbeefL;
    private static final long TERM_ID = 0xec1L;

    private static String adminPath;

    @BeforeClass
    public static void setupDirectories() throws IOException
    {
        final File adminDir = new File(System.getProperty("java.io.tmpdir"), ADMIN_DIR);
        if (adminDir.exists())
        {
            IoUtil.delete(adminDir, false);
        }
        IoUtil.ensureDirectoryExists(adminDir, ADMIN_DIR);
        adminPath = adminDir.getAbsolutePath();
    }

    private SenderThread senderThread;
    private ReceiverThread receiverThread;
    private MediaDriverAdminThread mediaDriverAdminThread;
    private DatagramChannel senderChannel;
    private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(256);
    private final AtomicBuffer writeBuffer = new AtomicBuffer(sendBuffer);

    @Before
    public void setUp() throws Exception
    {
        final MediaDriver.TopologyBuilder builder = new MediaDriver.TopologyBuilder()
                .adminThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .receiverThreadCommandBuffer(COMMAND_BUFFER_SZ)
                .rcvNioSelector(new NioSelector())
                .adminNioSelector(new NioSelector())
                .senderFlowControl(DefaultSenderFlowControlStrategy::new)
                .adminBufferStrategy(new CreatingAdminBufferStrategy(adminPath, COMMAND_BUFFER_SZ + TRAILER_SIZE))
                .bufferManagementStrategy(new BasicBufferManagementStrategy(adminPath));

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
    }

    @Test(timeout = 1000)
    public void shouldBeAbleToAddReceiver() throws Exception
    {
        writeReceiverMessage(ControlProtocolEvents.ADD_RECEIVER, URI, ONE_CHANNEL);

        mediaDriverAdminThread.process();
        receiverThread.process();

        assertNotNull(receiverThread.frameHandler(UdpDestination.parse(URI)));
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldSendErrorForInvalidUri() throws Exception
    {
        writeReceiverMessage(ControlProtocolEvents.ADD_RECEIVER, INVALID_URI, ONE_CHANNEL);

        mediaDriverAdminThread.process();
        // TODO: check control buffer to consumer to see if error is there.
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldSendErrorForRemovingNonExistentReceiver() throws Exception
    {
        // TODO: finish
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToCreateRcvTermOnZeroLengthData() throws Exception
    {
        writeReceiverMessage(ControlProtocolEvents.ADD_RECEIVER, URI, ONE_CHANNEL);

        mediaDriverAdminThread.process();
        receiverThread.process();

        final UdpDestination dest = UdpDestination.parse(URI);

        assertNotNull(receiverThread.frameHandler(dest));

        sendDataFrame(dest, ONE_CHANNEL[0], 0x0);
        receiverThread.process();
        mediaDriverAdminThread.process();

        //TODO: finish. assert on term buffer existence. Also, loop on admin and recv threads as NioSelectorTest
    }

    @Ignore
    @Test(timeout = 1000)
    public void shouldBeAbleToAddThenRemoveReceiverWithoutTermBuffer()
    {
        // TODO: finish
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
        final ByteBuffer buffer = new MappingAdminBufferStrategy(adminPath).toMediaDriver();
        final RingBuffer adminCommands = new ManyToOneRingBuffer(new AtomicBuffer(buffer));

        final ReceiverMessageFlyweight receiverMessageFlyweight = new ReceiverMessageFlyweight();
        receiverMessageFlyweight.wrap(writeBuffer, 0);

        receiverMessageFlyweight.channelIds(channelIds)
                                .destination(destination);

        adminCommands.write(eventTypeId, writeBuffer, 0, receiverMessageFlyweight.length());
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
                           .version((byte) 1)
                           .flags((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                           .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                           .frameLength(DataHeaderFlyweight.HEADER_LENGTH);

        sendBuffer.position(0);
        sendBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);
        senderChannel.send(sendBuffer, destination.remoteData());
    }
}
