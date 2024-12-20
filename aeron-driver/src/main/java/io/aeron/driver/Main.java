package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CommonContext.InferableBoolean.FORCE_TRUE;
import static io.aeron.CommonContext.InferableBoolean.INFER;

// todo: will be removed in final commit.
public class Main
{

    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
    private static final int STREAM_ID = 1001;

    public static void main(String[] args)
    {
        // Start an embedded MediaDriver
        MediaDriver mediaDriver = MediaDriver.launchEmbedded();

        // Aeron context using the embedded driver's directory
        Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());

        // Start Aeron
        Aeron aeron = Aeron.connect(ctx);

        // Start the server in a separate thread
        Thread serverThread = new Thread(() -> runServer(aeron));
        serverThread.start();

        // Start the client
        runClient(aeron);

        // Clean up
        try
        {
            serverThread.join();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            aeron.close();
            mediaDriver.close();
        }
    }

    private static void runServer(Aeron aeron)
    {
        try (Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            System.out.println("Server started. Listening on channel: " + CHANNEL + ", stream ID: " + STREAM_ID);

            FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
            {
                String message = buffer.getStringWithoutLengthUtf8(offset, length);
                System.out.println("Server received message: " + message);
            };

            while (!Thread.currentThread().isInterrupted())
            {
                int fragmentsRead = subscription.poll(fragmentHandler, 10);
                if (fragmentsRead == 0)
                {
                    Thread.onSpinWait();
                }
            }
        }
    }

    private static void runClient(Aeron aeron)
    {
        try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
        {
            String message = "Hello from the client!";
            byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);

            // Wrap the byte array in an UnsafeBuffer
            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(messageBytes.length));
            buffer.putBytes(0, messageBytes);

            while (true)
            {
                long result = publication.offer(buffer, 0, messageBytes.length);
                if (result > 0)
                {
                    System.out.println("Client sent message: " + message);
                    break;
                }
                else if (result == Publication.BACK_PRESSURED)
                {
                    System.out.println("Client back pressured. Retrying...");
                }
                else if (result == Publication.NOT_CONNECTED)
                {
                    System.out.println("Client not connected to the subscription.");
                }
                else if (result == Publication.ADMIN_ACTION)
                {
                    System.out.println("Publication is being administratively managed. Retrying...");
                }
                else
                {
                    System.out.println("Unexpected offer result: " + result);
                }

                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }


    private static String enrichImagePublicationChannelUri(
        final ReceiveChannelEndpoint receiveChannelEndpoint,
           final SubscriptionParams params,
        final CongestionControl congestionControl)
    {
        final ChannelUriStringBuilder channelUriStringBuilder = new ChannelUriStringBuilder(
            receiveChannelEndpoint.udpChannel().channelUri());

        final int streamId=0;
        final int initialTermId=0;
        final int activeTermId=0;
        final int termBufferLength=0;
        final int termOffset=0;
        final int senderMtuLength=0;
        final boolean isSparse=false;
        boolean isTether = params.isTether;
        boolean isRejoin = params.isRejoin;
        boolean isReliable = params.isReliable;
        String name = congestionControl.getClass().getName();
        boolean hasSessionId = params.hasSessionId;
        int sessionId = params.sessionId;
        int socketRcvbufLength = receiveChannelEndpoint.socketRcvbufLength();
        int receiverWindowLength = params.receiverWindowLength;
        int socketSndbufLength = receiveChannelEndpoint.socketSndbufLength();
        boolean group = FORCE_TRUE == params.group;


        channelUriStringBuilder.sessionId(sessionId);

        channelUriStringBuilder.streamId(streamId);

        channelUriStringBuilder.termLength(termBufferLength);
        channelUriStringBuilder.mtu(senderMtuLength);

        channelUriStringBuilder.initialTermId(initialTermId);
        channelUriStringBuilder.termId(activeTermId);

        channelUriStringBuilder.termOffset(termOffset);

        channelUriStringBuilder.tether(isTether);
        channelUriStringBuilder.rejoin(isRejoin);
        channelUriStringBuilder.reliable(isReliable);
        if (INFER != params.group)
        {
            channelUriStringBuilder.group(group);
        }
        channelUriStringBuilder.sparse(isSparse);

        channelUriStringBuilder.congestionControl(name);

        if (hasSessionId)
        {
            channelUriStringBuilder.sessionId(sessionId);
        }

        channelUriStringBuilder.socketRcvbufLength(socketRcvbufLength);
        channelUriStringBuilder.socketSndbufLength(socketSndbufLength);

        channelUriStringBuilder.receiverWindowLength(receiverWindowLength);

        //  channelUriStringBuilder.group(params.group);

//        boolean hasJoinPosition = false;
//        boolean isResponse = false;
//        InferableBoolean group = InferableBoolean.INFER;
//
        return channelUriStringBuilder.toString();
    }
}
