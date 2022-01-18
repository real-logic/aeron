package io.aeron.samples.echo.api;

public class ProvisioningConstants
{
    public static final String IO_AERON_TYPE_PROVISIONING_NAME_TESTING = "io.aeron:type=Provisioning,name=testing";
    public static final String IO_AERON_TYPE_ECHO_PAIR_PREFIX = "io.aeron:type=EchoPair,name=";

    public static String echoPairObjectName(long correlationId)
    {
        return IO_AERON_TYPE_ECHO_PAIR_PREFIX + correlationId;
    }
}
