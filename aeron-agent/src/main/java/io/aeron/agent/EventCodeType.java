package io.aeron.agent;

/**
 * Specifies the type of EventCode that can be handled by the logging agent.
 */
public enum EventCodeType
{
    DRIVER(0),
    ARCHIVE(1),
    CLUSTER(2),
    USER(0xFFFF);

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