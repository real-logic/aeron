package io.aeron;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static io.aeron.archive.TestUtil.await;

@RunWith(Theories.class)
public class ImageCloseTest
{
    private static final int FRAGMENT_COUNT_LIMIT = 10;
    public static final int STREAM_ID = 123;
    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";
    @DataPoint
    public static final String IPC_CHANNEL = CommonContext.IPC_CHANNEL;

    @DataPoint
    public static final int MANY_PUBLICATIONS = 5;
    @DataPoint
    public static final int SINGLE_PUBLICATION = 1;
    @DataPoint
    public static final int NO_PUBLICATIONS = 0;

    @DataPoint
    public static final boolean WITH = true;
    @DataPoint
    public static final boolean WITHOUT = false;


    private final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .errorHandler(Throwable::printStackTrace)
        .threadingMode(ThreadingMode.SHARED));
    private final Aeron aeron = Aeron.connect();


    @After
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(driver);
        IoUtil.delete(driver.context().aeronDirectory(), true);
    }


    @Test
    @Theory
    public void imageShouldBeClosed(
        final String channel,
        final int concurrentPublicationsNumber,
        final int exclusivePublicationsNumber,
        final boolean withTraffic,
        final boolean withArchive)
    {
        Archive archive = null;
        AeronArchive aeronArchive = null;
        if (withArchive)
        {
            archive = Archive.launch(new Archive.Context().errorHandler(Throwable::printStackTrace));
            aeronArchive = AeronArchive.connect();
            aeronArchive.startRecording(channel, STREAM_ID, SourceLocation.LOCAL);
        }
        final ArrayList<Publication> concurrentPublications = new ArrayList<>(concurrentPublicationsNumber);
        for (int i = 0; i < concurrentPublicationsNumber; i++)
        {
            concurrentPublications.add(aeron.addPublication(channel, STREAM_ID));
        }
        final ArrayList<Publication> exclusivePublications = new ArrayList<>(exclusivePublicationsNumber);
        for (int i = 0; i < exclusivePublicationsNumber; i++)
        {
            exclusivePublications.add(aeron.addExclusivePublication(channel, STREAM_ID));
        }

        try (Subscription subscription = aeron.addSubscription(channel, STREAM_ID))
        {
            await(() -> subscription.imageCount() ==
                exclusivePublicationsNumber + (concurrentPublicationsNumber == 0 ? 0 : 1));

            if (withTraffic && concurrentPublicationsNumber + exclusivePublicationsNumber != 0)
            {
                final Publication publication = findRandomPublication(
                    concurrentPublications,
                    exclusivePublications);

                testTraffic(publication, subscription);
            }

            final List<Image> images = Arrays.asList(subscription.images);
            concurrentPublications.forEach(Publication::close);
            exclusivePublications.forEach(Publication::close);

            await(() -> images.stream().allMatch(Image::isClosed) && subscription.hasNoImages());
        }
        finally
        {
            concurrentPublications.forEach(Publication::close);
            exclusivePublications.forEach(Publication::close);
            if (withArchive)
            {
                aeronArchive.stopRecording(channel, STREAM_ID);
                CloseHelper.close(aeronArchive);
                CloseHelper.close(archive);
                archive.context().deleteArchiveDirectory();
            }
        }
    }

    @SuppressWarnings({"ComparatorMethodParameterNotUsed", "OptionalGetWithoutIsPresent"})
    private Publication findRandomPublication(
        final ArrayList<Publication> concurrentPublications,
        final ArrayList<Publication> exclusivePublications)
    {
        return Stream.concat(
            concurrentPublications.stream(),
            exclusivePublications.stream())
            .sorted((l, r) -> ThreadLocalRandom.current().nextInt(-1, 2))
            .findAny()
            .get();
    }

    private void testTraffic(final Publication publication, final Subscription subscription)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(128);
        final MutableInteger receiveMessages = new MutableInteger(0);
        final FragmentHandler fragmentHandler =
            (buffer1, offset, length, header) -> receiveMessages.value++;

        final int testMessages = 10;
        for (int i = 0; i < testMessages; i++)
        {
            while (publication.offer(buffer) < 0)
            {
                subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
            }
        }
        while (receiveMessages.get() < testMessages)
        {
            subscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT);
        }
    }
}
