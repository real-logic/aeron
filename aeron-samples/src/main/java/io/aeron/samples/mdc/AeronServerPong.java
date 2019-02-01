package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

public class AeronServerPong
{

    private static final int MAX_POLL_FRAGMENT_LIMIT = 8;

    public static void main(String[] args)
    {
        // generate temp aeron dir name
        String aeronDirectoryName =
            IoUtil.tmpDirName()
                + "aeron"
                + '-'
                + System.getProperty("user.name", "default")
                + '-'
                + UUID.randomUUID().toString();

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
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaContext);

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
        Aeron aeron =
            Aeron.connect(
                new Context()
                    .errorHandler(th -> System.err.println("Aeron exception occurred: " + th))
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        // start server
        new Server(aeron).start();
    }

    private static class Server
    {

        private static final int STREAM_ID = 0xcafe0000;
        private static final String address = "localhost";
        private static final int port = 13000;
        private static final int controlPort = 13001;
        private static final String acceptorChannel =
            new ChannelUriStringBuilder()
                .endpoint(address + ':' + port)
                .reliable(Boolean.TRUE)
                .media("udp")
                .build();
        private static final ChannelUriStringBuilder outboundChannelBuilder =
            new ChannelUriStringBuilder()
                .controlEndpoint(address + ':' + controlPort)
                .reliable(Boolean.TRUE)
                .media("udp");

        private final Aeron aeron;
        private final List<MsgPublication> publications = new CopyOnWriteArrayList<>();
        private final Executor executor = Executors.newSingleThreadExecutor();
        private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);
        private volatile Subscription acceptSubscription;

        Server(Aeron aeron)
        {
            this.aeron = aeron;
        }

        private void start()
        {
            System.out.println("bind on " + acceptorChannel);

            acceptSubscription =
                aeron.addSubscription(
                    acceptorChannel,
                    STREAM_ID,
                    this::onAcceptImageAvailable,
                    this::onAcceptImageUnavailable);

            executor.execute(
                () -> {
                    // wait for available publication and image (duplex connection)
                    while (true)
                    {
                        int pSize = publications.size();
                        int iSize = acceptSubscription.imageCount();
                        if (pSize > 0 && iSize > 0)
                        {
                            break;
                        }
                        idleStrategy.idle();
                    }

                    idleStrategy.reset();

                    MsgPublication publication = publications.get(0);
                    Image image = acceptSubscription.images().get(0);

                    while (true)
                    {
                        idleStrategy.idle(process(image, publication));
                    }
                });
        }

        private int process(Image image, MsgPublication publication)
        {
            int result = 0;
            result +=
                image.poll(
                    (buffer, offset, length, header) -> {
                        UnsafeBuffer incoming = new UnsafeBuffer(buffer, offset, length);
                        int r;
                        do
                        {
                            r = publication.send(incoming);
                        } while (r < 1);
                    },
                    MAX_POLL_FRAGMENT_LIMIT);
            return result;
        }

        private void onAcceptImageAvailable(Image image)
        {
            int sessionId = image.sessionId();
            String outboundChannel =
                outboundChannelBuilder.sessionId(sessionId ^ Integer.MAX_VALUE).build();

            System.out.println("onImageAvailable: " + sessionId + " / " + image.sourceIdentity());
            System.out.println("create outbound " + outboundChannel + "for sessionId: " + sessionId);

            ForkJoinPool.commonPool()
                .execute(
                    () -> {
                        Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);
                        publications.add(new MsgPublication(sessionId, publication));
                    });
        }

        private void onAcceptImageUnavailable(Image image)
        {
            int sessionId = image.sessionId();

            System.out.println("onImageUnavailable: " + sessionId + " / " + image.sourceIdentity());

            ForkJoinPool.commonPool()
                .execute(
                    () -> {
                        publications
                            .stream()
                            .filter(publication -> publication.sessionId() == sessionId)
                            .findFirst()
                            .ifPresent(MsgPublication::close);
                        publications.removeIf(msgPublication -> msgPublication.sessionId() == sessionId);
                    });
        }
    }
}
