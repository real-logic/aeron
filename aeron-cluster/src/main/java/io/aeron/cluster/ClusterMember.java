/*
 * Copyright 2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

/**
 * Represents a member of the cluster that participates in replication.
 */
public final class ClusterMember
{
    private boolean isLeader;
    private final int id;
    private long termPosition;
    private final String clientFacingEndpoint;
    private final String memberFacingEndpoint;
    private final String logEndpoint;

    /**
     * Construct a new member of the cluster.
     *
     * @param id                   unique id for the member.
     * @param clientFacingEndpoint address and port endpoint to which cluster clients connect.
     * @param memberFacingEndpoint address and port endpoint to which other cluster members connect.
     * @param logEndpoint          address and port endpoint to which the log is replicated.
     */
    public ClusterMember(
        final int id,
        final String clientFacingEndpoint,
        final String memberFacingEndpoint,
        final String logEndpoint)
    {
        this.id = id;
        this.clientFacingEndpoint = clientFacingEndpoint;
        this.memberFacingEndpoint = memberFacingEndpoint;
        this.logEndpoint = logEndpoint;
    }

    /**
     * Set if this member should be leader.
     *
     * @param isLeader value.
     */
    public void isLeader(final boolean isLeader)
    {
        this.isLeader = isLeader;
    }

    /**
     * Is this member currently the leader?
     *
     * @return true if this member is currently the leader otherwise false.
     */
    public boolean isLeader()
    {
        return isLeader;
    }

    /**
     * Unique identity for this member in the cluster.
     *
     * @return the unique identity for this member in the cluster.
     */
    public int id()
    {
        return id;
    }

    /**
     * The log position this member has persisted within the current leadership term.
     *
     * @param termPosition in the log this member has persisted within the current leadership term.
     */
    public void termPosition(final long termPosition)
    {
        this.termPosition = termPosition;
    }

    /**
     * The log position this member has persisted within the current leadership term.
     *
     * @return the log position this member has persisted within the current leadership term.
     */
    public long termPosition()
    {
        return termPosition;
    }

    /**
     * The address:port endpoint for this cluster member that clients will connect to.
     *
     * @return the address:port endpoint for this cluster member that clients will connect to.
     */
    public String clientFacingEndpoint()
    {
        return clientFacingEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member that other members connect to.
     *
     * @return the address:port endpoint for this cluster member that other members will connect to.
     */
    public String memberFacingEndpoint()
    {
        return memberFacingEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member that the log is replicated to.
     *
     * @return the address:port endpoint for this cluster member that the log is replicated to.
     */
    public String logEndpoint()
    {
        return logEndpoint;
    }

    /**
     * Parse the details for a cluster members from a string.
     * <p>
     * <code>
     *     0,client-facing:port,member-facing:port,log:port|1,client-facing:port,member-facing:port,log:port| ...
     * </code>
     *
     * @param value of the string to be parsed.
     * @return An array of cluster members.
     */
    public static ClusterMember[] parse(final String value)
    {
        final String[] memberValues = value.split("\\|");
        final int length = memberValues.length;
        final ClusterMember[] members = new ClusterMember[length];

        for (int i = 0; i < length; i++)
        {
            final String[] memberAttributes = memberValues[i].split(",");
            if (memberAttributes.length != 4)
            {
                throw new IllegalStateException("Invalid member value: " + memberValues[i]);
            }

            members[i] = new ClusterMember(
                Integer.parseInt(memberAttributes[0]),
                memberAttributes[1],
                memberAttributes[2],
                memberAttributes[3]);
        }

        return members;
    }

    /**
     * The threshold of clusters members required to achieve quorum given a count of cluster members.
     *
     * @param memberCount for the cluster
     * @return the threshold for achieving quorum.
     */
    public static int quorumThreshold(final int memberCount)
    {
        return (memberCount / 2) + 1;
    }

    /**
     * Calculate the position reached by a quorum of cluster members.
     *
     * @param members         of the cluster.
     * @param rankedPositions temp array to be used for sorting the positions to avoid allocation.
     * @return the position reached by a quorum of cluster members.
     */
    public static long quorumPosition(final ClusterMember[] members, final long[] rankedPositions)
    {
        final int length = rankedPositions.length;
        for (int i = 0; i < length; i++)
        {
            rankedPositions[i] = 0;
        }

        for (final ClusterMember member : members)
        {
            long newPosition = member.termPosition;

            for (int i = 0; i < length; i++)
            {
                final long rankedPosition = rankedPositions[i];

                if (newPosition > rankedPosition)
                {
                    rankedPositions[i] = newPosition;
                    newPosition = rankedPosition;
                }
            }
        }

        return rankedPositions[length - 1];
    }
}
