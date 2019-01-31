package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.samples.mdc.AeronResources.MsgPublication;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

abstract class AeronClient {

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

  private final Aeron aeron;

  private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 100);
  private Executor scheduler;

  AeronClient(Aeron aeron) {
    this.aeron = aeron;
    this.scheduler = Executors.newSingleThreadExecutor();
  }

  final void start() {
    scheduler.execute(
        () -> {
          String outboundChannel = outboundChannelBuilder.build();

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

          while (true) {
            if (subscription.imageCount() > 0) {
              break;
            }
            idleStrategy.idle();
          }

          Image image = subscription.images().get(0);

          while (true) {
            int workCount = processOutbound(msgPublication);
            workCount += processInbound(image);
            idleStrategy.idle(workCount);
          }
        });
  }

  int processInbound(Image image) {
    return 0;
  }

  int processOutbound(MsgPublication msgPublication) {
    return 0;
  }
}
