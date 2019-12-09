package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.TestMediaDriver;
import org.agrona.ErrorHandler;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class SpecifiedPositionPublicationTest
{
    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
        .errorHandler(mockErrorHandler)
        .dirDeleteOnShutdown(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);

    @Test(expected = RegistrationException.class)
    public void shouldRejectSpecifiedPositionForConcurrentPublications()
    {
        try (TestMediaDriver testMediaDriver = TestMediaDriver.launch(mediaDriverContext);
            Aeron aeron = Aeron.connect())
        {
            final String channel = new ChannelUriStringBuilder()
                .media("ipc")
                .initialPosition(1024, -873648623, 65536)
                .build();
            aeron.addPublication(channel, 101);
        }
    }
}
