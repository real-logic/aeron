package io.aeron.agent;

/**
 * Super-type for {@link EventCode} that can be handled by the logging agent.
 */
public enum EventCodeType
{
    DRIVER(0),
    ARCHIVE(1),
    CLUSTER(2);

    private final int typeCode;

    EventCodeType(final int typeCode)
    {
        this.typeCode = typeCode;
    }

    public int getTypeCode()
    {
        return typeCode;
    }
}