package io.aeron.samples.echo.api;

public interface ProvisioningMBean
{
    void createEchoPair(
        final long correlationId,
        String subChannel,
        int subStreamId,
        String pubChannel,
        int pubStreamId);

    void removeAll();
}
