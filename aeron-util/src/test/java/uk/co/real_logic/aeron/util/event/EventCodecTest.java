package uk.co.real_logic.aeron.util.event;

import org.junit.Test;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class EventCodecTest
{

    private static final String MESSAGE = "End of the world!";
    public static final String DECLARING_CLASS = EventCodecTest.class.getName();
    public static final String METHOD = "someMethod";
    public static final String FILE = EventCodecTest.class.getSimpleName() + ".java";
    public static final int lineNumber = 10;

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
        // Line number of ex.fillInStackTrace();
        assertThat(written, containsString(":26"));
    }

    @Test
    public void dissectAsInvocationShouldContainTheValuesEncoded()
    {
        StackTraceElement element = new StackTraceElement(DECLARING_CLASS, METHOD, FILE, lineNumber);

        final int size = EventCodec.encode(buffer, element);
        final String written = EventCodec.dissectAsInvocation(EventCode.EXCEPTION, buffer, 0, size);

        assertThat(written, containsString(DECLARING_CLASS));
        assertThat(written, containsString(METHOD));
        assertThat(written, containsString(FILE));
        assertThat(written, containsString(":" + lineNumber));
    }

}
