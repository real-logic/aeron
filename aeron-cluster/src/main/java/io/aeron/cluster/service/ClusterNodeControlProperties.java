package io.aeron.cluster.service;

public class ClusterNodeControlProperties
{
    public final String aeronDirectoryName;
    public final String archiveChannel;
    public final String serviceControlChannel;
    public final int toServiceStreamId;
    public final int toConsensusModuleStreamId;

    public ClusterNodeControlProperties(
        final String aeronDirectoryName,
        final String archiveChannel,
        final String serviceControlChannel,
        final int toServiceStreamId,
        final int toConsensusModuleStreamId)
    {

        this.aeronDirectoryName = aeronDirectoryName;
        this.archiveChannel = archiveChannel;
        this.serviceControlChannel = serviceControlChannel;
        this.toServiceStreamId = toServiceStreamId;
        this.toConsensusModuleStreamId = toConsensusModuleStreamId;
    }
}
