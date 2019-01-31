package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.samples.mdc.AeronResources.MsgPublication;
import java.util.List;
import java.util.Queue;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;

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
public class AeronServerPong {

  private static final int QUEUE_CAPACITY = 256;
  private static final int MAX_POLL_FRAGMENT_LIMIT = 1;
  private static final int MAX_WRITE_LIMIT = 1;

  /**
   * Main runner.
   *
   * @param args program arguments.
   */
  public static void main(String[] args) {
    Aeron aeron = AeronResources.start();
    new Server(aeron).start();
  }

  private static class Server extends AeronServer {

    Queue<DirectBuffer> queue = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);

    Server(Aeron aeron) {
      super(aeron);
    }

    @Override
    int processInbound(List<Image> images) {
      int result = 0;
      if (queue.size() <= QUEUE_CAPACITY - MAX_POLL_FRAGMENT_LIMIT) {
        for (Image image : images) {
          result +=
              image.poll(
                  (buffer, offset, length, header) ->
                      queue.add(new UnsafeBuffer(buffer, offset, length)),
                  MAX_POLL_FRAGMENT_LIMIT);
        }
      }
      return result;
    }

    @Override
    int processOutbound(List<MsgPublication> publications) {
      int result = 0;
      if (!queue.isEmpty()) {
        for (int i = 0, current; i < MAX_WRITE_LIMIT; i++) {
          DirectBuffer buffer = queue.peek();
          current = 0;
          if (buffer != null) {
            for (MsgPublication publication : publications) {
              current += publication.proceed(buffer);
            }
          }
          if (current < 1) {
            break;
          }
          result += current;
        }
      }
      return result;
    }
  }
}
