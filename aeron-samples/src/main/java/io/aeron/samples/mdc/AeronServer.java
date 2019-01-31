package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.samples.mdc.AeronResources.MsgPublication;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

abstract class AeronServer {

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

  private volatile Subscription acceptSubscription;
  private final List<MsgPublication> publications = new CopyOnWriteArrayList<>();

  private final Executor scheduler = Executors.newSingleThreadExecutor();
  private final Executor commandExecutor = Executors.newSingleThreadExecutor();
  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);

  AeronServer(Aeron aeron) {
    this.aeron = aeron;
  }

  final void start() {
    System.out.println("bind on " + acceptorChannel);

    acceptSubscription =
        aeron.addSubscription(
            acceptorChannel,
            STREAM_ID,
            this::onAcceptImageAvailable,
            this::onAcceptImageUnavailable);

    scheduler.execute(
        () -> {
          // wait for available publication and image (duplex connection)
          while (true) {
            int pSize = publications.size();
            int iSize = acceptSubscription.imageCount();
            if (pSize > 0 && iSize > 0) {
              break;
            }
            idleStrategy.idle();
          }


          MsgPublication publication = publications.get(0);
          Image image = acceptSubscription.images().get(0);

          while (true) {
            idleStrategy.idle(process(image, publication));
          }
        });
  }

  private void onAcceptImageAvailable(Image image) {
    int sessionId = image.sessionId();
    String outboundChannel =
        outboundChannelBuilder.sessionId(sessionId ^ Integer.MAX_VALUE).build();

    System.out.println(
        "onImageAvailable: "
            + sessionId
            + " / "
            + image.sourceIdentity()
            + ", create outbound "
            + outboundChannel);

    commandExecutor.execute(
        () -> {
          Publication publication = aeron.addExclusivePublication(outboundChannel, STREAM_ID);
          publications.add(new MsgPublication(sessionId, publication));
        });
  }

  private void onAcceptImageUnavailable(Image image) {
    int sessionId = image.sessionId();

    System.out.println("onImageUnavailable: " + sessionId + " / " + image.sourceIdentity());

    commandExecutor.execute(
        () -> {
          publications
              .stream()
              .filter(publication -> publication.sessionId() == sessionId)
              .findFirst()
              .ifPresent(MsgPublication::close);
          publications.removeIf(msgPublication -> msgPublication.sessionId() == sessionId);
        });
  }

  int process(Image image, MsgPublication publication) {
    return 0;
  }
}
