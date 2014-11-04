package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.status.PositionReporter;

/**
* .
*/
class HeapPositionReporter implements PositionReporter
{

    private long position;

    public void position(final long value)
    {
        position = value;
    }

    public long position()
    {
        return position;
    }

    public void close()
    {

    }

    public int id()
    {
        return 0;
    }

}
