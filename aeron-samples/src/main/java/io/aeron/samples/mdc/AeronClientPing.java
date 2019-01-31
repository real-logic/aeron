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
 * -Daeron.mtu.length=1k
 * -Daeron.socket.so_sndbuf=4k
 * -Daeron.socket.so_rcvbuf=4k
 * -Daeron.rcv.initial.window.length=4k
 * -Dagrona.disable.bounds.checks=true
 * -Daeron.term.buffer.sparse.file=false
 */
public class AeronClientPing {

  private static final int MAX_POLL_FRAGMENT_LIMIT = 8;

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
    int process(Image image, MsgPublication msgPublication) {
      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(Long.BYTES);
      buffer.putLong(0, System.nanoTime());

      int result = msgPublication.proceed(buffer);

      result += image.poll(fragmentHandler, MAX_POLL_FRAGMENT_LIMIT);
      return result;
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
