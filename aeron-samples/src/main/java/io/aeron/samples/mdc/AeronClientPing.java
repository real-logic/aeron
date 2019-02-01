package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.SampleConfiguration;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.io.File;
import java.nio.file.Paths;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class AeronClientPing
{

    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int MAX_POLL_FRAGMENT_LIMIT = 8;

    public static void main(String[] args)
    {
        // generate temp aeron dir name
        final String aeronDirectoryName = IoUtil.tmpDirName() + "aeron-" + System.getProperty("user.name", "default") + '-' + UUID.randomUUID().toString();

        // setup MediaDriver
        Supplier<IdleStrategy> idleStrategySupplier = () -> new BackoffIdleStrategy(1, 1, 1, 100);
        MediaDriver.Context mediaContext =
            new MediaDriver.Context()
                .errorHandler(th -> System.err.println("Exception occurred on MediaDriver: " + th))
                .mtuLength(Configuration.MTU_LENGTH)
                .warnIfDirectoryExists(true)
                .dirDeleteOnStart(true)
                .threadingMode(ThreadingMode.DEDICATED)
                .conductorIdleStrategy(idleStrategySupplier.get())
                .receiverIdleStrategy(idleStrategySupplier.get())
                .senderIdleStrategy(idleStrategySupplier.get())
                .termBufferSparseFile(false)
                .publicationReservedSessionIdLow(0)
                .publicationReservedSessionIdHigh(Integer.MAX_VALUE)
                .aeronDirectoryName(aeronDirectoryName);
        try (MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaContext))
        {

            // set shutdown hook to delete aeron dir name
            Runtime.getRuntime()
                .addShutdownHook(
                    new Thread(
                        () -> {
                            File aeronDirectory = Paths.get(mediaDriver.aeronDirectoryName()).toFile();
                            if (aeronDirectory.exists())
                            {
                                IoUtil.delete(aeronDirectory, true);
                            }
                        }));

            // launch aeron
            try (Aeron aeron =
                     Aeron.connect(
                         new Context()
                             .errorHandler(th -> System.err.println("Aeron exception occurred: " + th))
                             .aeronDirectoryName(mediaDriver.aeronDirectoryName())))
            {

                // start client
                new Client(aeron).start();
            }
        }
    }

    private static class Client
    {

        private static final int STREAM_ID = 0xcafe0000;
        private static final String address = "localhost";
        private static final int port = 13000;
        private static final int controlPort = 13001;
        private static final ChannelUriStringBuilder outboundChannelBuilder =
            new ChannelUriStringBuilder()
                .endpoint(address + ':' + port)
                .reliable(Boolean.TRUE)
                .media("udp");
        private static final ChannelUriStringBuilder inboundChannelBuilder =
            new ChannelUriStringBuilder()
                .controlEndpoint(address + ':' + controlPort)
                .controlMode(CommonContext.MDC_CONTROL_MODE_DYNAMIC)
                .reliable(Boolean.TRUE)
                .media("udp");

        private final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        private final MsgFragmentHandler fragmentHandler = new MsgFragmentHandler(histogram);
        private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);

        private final Aeron aeron;

        private Client(Aeron aeron)
        {
            this.aeron = aeron;
        }

        private void start()
        {
            int sid = new Random().nextInt(Integer.MAX_VALUE);
            String outboundChannel = outboundChannelBuilder.sessionId(sid).build();
            Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);

            int sessionId = publication.sessionId();

            String inboundChannel =
                inboundChannelBuilder.sessionId(sessionId ^ Integer.MAX_VALUE).build();

            Subscription subscription =
                aeron.addSubscription(
                    inboundChannel,
                    STREAM_ID,
                    image ->
                        System.out.println(
                            "onClientImageAvailable: "
                                + image.sessionId()
                                + " / "
                                + image.sourceIdentity()),
                    image ->
                        System.out.println(
                            "onClientImageUnavailable: "
                                + image.sessionId()
                                + " / "
                                + image.sourceIdentity()));

            MsgPublication msgPublication = new MsgPublication(sessionId, publication);

            // wait for available image
            while (true)
            {
                if (subscription.imageCount() > 0)
                {
                    break;
                }
                idleStrategy.idle();
            }

            idleStrategy.reset();
            histogram.reset();

            Image image = subscription.images().get(0);

            for (int i = 0; i < NUMBER_OF_MESSAGES; )
            {

                ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(Long.BYTES);
                buffer.putLong(0, System.nanoTime());

                int sentCount = msgPublication.send(buffer);
                if (sentCount > 0)
                {
                    i++;
                }

                int readCount = image.poll(fragmentHandler, MAX_POLL_FRAGMENT_LIMIT);

                idleStrategy.idle(sentCount + readCount);
            }

            while (histogram.getTotalCount() < NUMBER_OF_MESSAGES)
            {
                idleStrategy.idle(image.poll(fragmentHandler, MAX_POLL_FRAGMENT_LIMIT));
            }

            histogram.outputPercentileDistribution(System.out, 1000.0);
        }

        private static class MsgFragmentHandler implements FragmentHandler
        {

            private final Histogram histogram;

            private MsgFragmentHandler(Histogram histogram)
            {
                this.histogram = histogram;
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
            {
                long start = buffer.getLong(offset);
                long diff = System.nanoTime() - start;
                histogram.recordValue(diff);
            }
        }
    }
}
