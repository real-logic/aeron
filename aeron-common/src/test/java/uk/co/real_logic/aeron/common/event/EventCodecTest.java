package uk.co.real_logic.aeron.common.event;

import org.junit.Test;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class EventCodecTest
{
    private static final String MESSAGE = "End of the world!";
    private static final String DECLARING_CLASS = EventCodecTest.class.getName();
    private static final String METHOD = "someMethod";
    private static final String FILE = EventCodecTest.class.getSimpleName() + ".java";
    private static final int LINE_NUMBER = 10;

    private static final int BUFFER_SIZE = 1024 * 10;

    private AtomicBuffer buffer = new AtomicBuffer(new byte[BUFFER_SIZE]);

    @Test
    public void dissectAsExceptionShouldContainTheValuesEncoded()
    {
        final Exception ex = new Exception(MESSAGE);
        ex.fillInStackTrace();

        final int size = EventCodec.encode(buffer, ex);
        final String written = EventCodec.dissectAsException(EventCode.EXCEPTION, buffer, 0, size);

        assertThat(written, containsString(MESSAGE));
        assertThat(written, containsString(ex.getClass().getName()));
        assertThat(written, containsString(getClass().getName()));
        assertThat(written, containsString(getClass().getSimpleName() + ".java"));
        assertThat(written, containsString("dissectAsExceptionShouldContainTheValuesEncoded"));
        assertThat(written, containsString(":25")); // Line number of ex.fillInStackTrace() above
    }

    @Test
    public void dissectAsInvocationShouldContainTheValuesEncoded()
    {
        final StackTraceElement element = new StackTraceElement(DECLARING_CLASS, METHOD, FILE, LINE_NUMBER);

        final int size = EventCodec.encode(buffer, element);
        final String written = EventCodec.dissectAsInvocation(EventCode.EXCEPTION, buffer, 0, size);

        assertThat(written, containsString(DECLARING_CLASS));
        assertThat(written, containsString(METHOD));
        assertThat(written, containsString(FILE));
        assertThat(written, containsString(":" + LINE_NUMBER));
    }

    @Test
    public void dissectAsStringShouldContainTheValuesEncoded()
    {
        final int size = EventCodec.encode(buffer, MESSAGE);
        final String written = EventCodec.dissectAsString(EventCode.FRAME_OUT_INCOMPLETE_SENDTO, buffer, 0, size);

        assertThat(written, containsString(MESSAGE));
    }
}
