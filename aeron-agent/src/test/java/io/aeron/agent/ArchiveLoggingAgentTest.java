package io.aeron.agent;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ArchiveLoggingAgentTest
{
    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private String testDirName;
    private ArchivingMediaDriver archivingMediaDriver;
    private AeronArchive aeronArchive;

    @Before
    public void before()
    {
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        Common.beforeAgent();
    }

    @After
    public void after()
    {
        Common.afterAfter();

        CloseHelper.close(aeronArchive);
        CloseHelper.close(archivingMediaDriver);

        if (testDirName != null)
        {
            IoUtil.delete(new File(testDirName), false);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldLogMessages() throws Exception
    {
        testDirName = Paths.get(IoUtil.tmpDirName(), "archive-test").toString();
        final File testDir = new File(testDirName);
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }

        final String aeronDirectoryName = Paths.get(testDirName, "media").toString();

        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .aeronDirectoryName(aeronDirectoryName)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8010")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?term-length=64k|endpoint=localhost:8020")
            .controlResponseStreamId(101)
            .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:8030");

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(Throwable::printStackTrace)
            .archiveDir(new File(testDirName, "archive"))
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        archivingMediaDriver = ArchivingMediaDriver.launch(mediaDriverCtx, archiveCtx);
        aeronArchive = AeronArchive.connect(aeronArchiveContext);

        LATCH.await();
    }

    static class StubEventLogReaderAgent implements Agent, MessageHandler
    {
        public String roleName()
        {
            return "event-log-reader";
        }

        public int doWork()
        {
            return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
        }

        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
        {
            if (ArchiveEventLogger.toEventCodeId(ArchiveEventCode.CMD_IN_AUTH_CONNECT) == msgTypeId)
            {
                LATCH.countDown();
            }
        }
    }
}
