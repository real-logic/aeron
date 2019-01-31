package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.mdc.AeronResources.MsgPublication;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Recorder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * VM options:
 * -XX:BiasedLockingStartupDelay=0
 * -XX:+UnlockDiagnosticVMOptions
 * -XX:GuaranteedSafepointInterval=300000
 * -Djava.net.preferIPv4Stack=true
 * -Daeron.mtu.length=4k
 * -Daeron.socket.so_sndbuf=256k
 * -Daeron.socket.so_rcvbuf=256k
 * -Daeron.rcv.initial.window.length=256k
 * -Dagrona.disable.bounds.checks=true
 * -Daeron.term.buffer.sparse.file=false
 */
public class AeronClientPing {

  private static final int MAX_POLL_FRAGMENT_LIMIT = 1;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    Aeron aeron = AeronResources.start();
    new Client(aeron).start();
  }

  private static class Client extends AeronClient {

    private final Recorder histogram;
    private final HistogramFragmentHandler fragmentHandler;

    private Client(Aeron aeron) {
      super(aeron);

      // start reporter
      histogram = new Recorder(3600000000000L, 3);

      fragmentHandler = new HistogramFragmentHandler(histogram);

      Executors.newSingleThreadScheduledExecutor()
          .scheduleWithFixedDelay(
              () -> {
                System.out.println("---- PING/PONG HISTO ----");
                histogram
                    .getIntervalHistogram()
                    .outputPercentileDistribution(System.out, 5, 1000.0, false);
                System.out.println("---- PING/PONG HISTO ----");
              },
              1,
              1,
              TimeUnit.SECONDS);
    }

    @Override
    int processOutbound(MsgPublication msgPublication) {
      return msgPublication.proceed(generatePayload());
    }

    @Override
    int processInbound(Image image) {
      return image.poll(fragmentHandler, MAX_POLL_FRAGMENT_LIMIT);
    }

    private DirectBuffer generatePayload() {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(Long.BYTES);
      buffer.putLong(0, System.nanoTime());
      return buffer;
    }

    private static class HistogramFragmentHandler implements FragmentHandler {

      private final Recorder histogram;

      private HistogramFragmentHandler(Recorder histogram) {
        this.histogram = histogram;
      }

      @Override
      public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        long start = buffer.getLong(offset);
        long diff = System.nanoTime() - start;
        histogram.recordValue(diff);
      }
    }
  }
}
