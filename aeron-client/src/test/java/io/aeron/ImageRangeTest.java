package io.aeron;

import io.aeron.driver.buffer.FileStoreLogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static io.aeron.logbuffer.LogBufferDescriptor.initialTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.isConnected;
import static io.aeron.logbuffer.LogBufferDescriptor.mtuLength;
import static io.aeron.logbuffer.LogBufferDescriptor.pageSize;
import static io.aeron.logbuffer.LogBufferDescriptor.termLength;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ImageRangeTest
{
    @SuppressWarnings("JUnitMalformedDeclaration")
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldHandleAllPossibleOffsets(final boolean useSpareFiles, final @TempDir File baseDir)
    {
        final int termBufferLength = 65536;
        final int filePageSize = 4096;
        final FileStoreLogFactory fileStoreLogFactory = new FileStoreLogFactory(
            baseDir.getAbsolutePath(), filePageSize, false, 0, new RethrowingErrorHandler());
        final long subscriberPositionThatWillTriggerException = 3147497471L;

        final RawLog rawLog = fileStoreLogFactory.newImage(0, termBufferLength, useSpareFiles);

        initialTermId(rawLog.metaData(), 0);
        mtuLength(rawLog.metaData(), 1408);
        termLength(rawLog.metaData(), termBufferLength);
        pageSize(rawLog.metaData(), filePageSize);
        isConnected(rawLog.metaData(), true);

        System.out.println(rawLog.fileName());
        final LogBuffers logBuffers = new LogBuffers(rawLog.fileName());

        assertEquals(termBufferLength, logBuffers.termLength());
        final Position subscriberPosition = new AtomicLongPosition();

        final Image image = new Image(
            null, 1, subscriberPosition, logBuffers, new RethrowingErrorHandler(), "127.0.0.1:123", 0);

        subscriberPosition.set(subscriberPositionThatWillTriggerException);

        image.boundedControlledPoll(
            (buffer, offset, length, header) -> ControlledFragmentHandler.Action.COMMIT, 1024, 1);
        image.boundedPoll(
            (buffer, offset, length, header) -> {}, 1024, 1);
    }
}