package uk.co.real_logic.aeron.tools;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;

/**
 * Created by bhorst on 3/6/15.
 */
public class MessageSizePatternTest
{
    MessageSizePattern p;
    @Test
    public void createWithMessageSize() throws Exception
    {
        p = new MessageSizePattern(1000);
        assertThat("FAIL: getNext() didn't return correct value",
                p.getNext(), is(1000));
    }

    @Test
    public void createWithMessageCountAndSize() throws Exception
    {
        p = new MessageSizePattern(200, 1000);
        assertThat("FAIL: getNext() didn't return correct value",
                p.getNext(), is(1000));
    }

    @Test
    public void createWithMessageCountAndRange() throws Exception
    {
        p = new MessageSizePattern(200, 1000, 1001);
        assertThat("FAIL: getNext() returned unexpected value",
                p.getNext(), both(greaterThanOrEqualTo(1000)).and(lessThanOrEqualTo(1001)));
    }

    @Test
    public void copyConstructor() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 1001, 1002);

        assertThat(p.getNext(), is(1000));

        MessageSizePattern p2 = new MessageSizePattern(p);
        assertThat("FAIL: Copied pattern didn't start at the beginning", p2.getNext(), is(1000));
        assertThat(p.getNext(), both(greaterThanOrEqualTo(1001)).and(lessThanOrEqualTo(1002)));
        assertThat(p2.getNext(), both(greaterThanOrEqualTo(1001)).and(lessThanOrEqualTo(1002)));
    }

    @Test
    public void addSizeEntry() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 2000);
        p.getNext();
        assertThat("FAIL: 2nd call to getNext() returned unexpected value",
                p.getNext(), is(2000));
    }

    @Test
    public void addRangedEntry() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 1001, 1002);
        p.getNext();
        assertThat("FAIL: 2nd call to getNext() returned out of range value",
                p.getNext(), both(greaterThanOrEqualTo(1001)).and(lessThanOrEqualTo(1002)));
    }

    @Test
    public void repeatPattern() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 2000);
        p.addPatternEntry(1, 3000);

        assertThat(p.getNext(), is(1000));
        assertThat(p.getNext(), is(2000));
        assertThat(p.getNext(), is(3000));
        assertThat("FAIL: Pattern did not wrap.", p.getNext(), is(1000));
        assertThat(p.getNext(), is(2000));
        assertThat(p.getNext(), is(3000));
    }

    @Test
    public void checkPatternMinMax() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 500);
        p.addPatternEntry(1, 1500);

        assertThat("FAIL: Pattern minimum value is wrong",
                p.getMinimum(), is(500));
        assertThat("FAIL: Pattern maximum value is wrong",
                p.getMaximum(), is(1500));
    }

    @Test public void checkCurrentRangeMinMax() throws Exception
    {
        p = new MessageSizePattern(1, 1000, 2000);
        p.addPatternEntry(1, 2000, 3000);

        // Check overall min and max
        assertThat("FAIL: Pattern minimum value is wrong",
                p.getMinimum(), is(1000));
        assertThat("FAIL: Pattern maximum value is wrong",
                p.getMaximum(), is(3000));

        // Now check individual message range
        p.getNext();
        assertThat(p.getCurrentRangeMinimum(), is(1000));
        assertThat(p.getCurrentRangeMaximum(), is(2000));
        p.getNext();
        assertThat(p.getCurrentRangeMinimum(), is(2000));
        assertThat(p.getCurrentRangeMaximum(), is(3000));
    }

    @Test
    public void reset() throws Exception
    {
        p = new MessageSizePattern(1, 1000);
        p.addPatternEntry(1, 2000);

        p.getNext();
        p.reset();
        assertThat("FAIL: reset did not go back to the beginning of the pattern",
                p.getNext(), is(1000));
    }

    @Test (expected=Exception.class)
    public void invalidSize() throws Exception
    {
        p = new MessageSizePattern(-10);
    }

    @Test (expected=Exception.class)
    public void invalidNumberOfMessages() throws Exception
    {
        p = new MessageSizePattern(0, 1000);
    }

    @Test (expected=Exception.class)
    public void negativeRange() throws Exception
    {
        p = new MessageSizePattern(1, -1000, -500);
    }

    @Test (expected=Exception.class)
    public void invalidRange() throws Exception
    {
        p = new MessageSizePattern(1, 1000, 500);
    }
}
