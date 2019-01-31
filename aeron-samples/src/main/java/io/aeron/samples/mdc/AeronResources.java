package io.aeron.samples.mdc;

import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.Publication;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

class AeronResources {

  static Aeron start() {

    Supplier<IdleStrategy> idleStrategySupplier = () -> new BackoffIdleStrategy(1, 1, 1, 100);

    String aeronDirectoryName =
        IoUtil.tmpDirName()
            + "aeron"
            + '-'
            + System.getProperty("user.name", "default")
            + '-'
            + UUID.randomUUID().toString();

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

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  File aeronDirectory = Paths.get(mediaDriver.aeronDirectoryName()).toFile();
                  if (aeronDirectory.exists()) {
                    IoUtil.delete(aeronDirectory, true);
                  }
                }));

    return Aeron.connect(
        new Context()
            .errorHandler(th -> System.err.println("Aeron exception occurred: " + th))
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));
  }

  static class MsgPublication {
    private static final ThreadLocal<BufferClaim> bufferClaims =
        ThreadLocal.withInitial(BufferClaim::new);

    private final int sessionId;
    private final Publication publication;

    MsgPublication(int sessionId, Publication publication) {
      this.sessionId = sessionId;
      this.publication = publication;
    }

    int proceed(DirectBuffer buffer) {
      long result = publish(buffer);

      if (result > 0) {
        return 1;
      }

      if (result < Publication.ADMIN_ACTION) {
        System.err.println("Publication@" + sessionId + " received result: " + result);
      }
      return 0;
    }

    private long publish(DirectBuffer buffer) {
      int length = buffer.capacity();

      if (length < publication.maxPayloadLength()) {
        BufferClaim bufferClaim = bufferClaims.get();
        long result = publication.tryClaim(length, bufferClaim);
        if (result > 0) {
          try {
            MutableDirectBuffer directBuffer = bufferClaim.buffer();
            int offset = bufferClaim.offset();
            directBuffer.putBytes(offset, buffer, 0, length);
            bufferClaim.commit();
          } catch (Exception ex) {
            bufferClaim.abort();
            throw ex;
          }
        }
        return result;
      } else {
        return publication.offer(new UnsafeBuffer(buffer, 0, length));
      }
    }

    void close() {
      publication.close();
    }

    int sessionId() {
      return sessionId;
    }
  }
}
