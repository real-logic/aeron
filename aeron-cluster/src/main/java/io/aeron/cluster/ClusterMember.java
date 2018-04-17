/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import org.agrona.CloseHelper;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Represents a member of the cluster that participates in consensus.
 */
public final class ClusterMember
{
    public static final int NULL_MEMBER_ID = -1;

    private boolean isBallotSent;
    private boolean isLeader;
    private final int id;
    private int votedForId = NULL_MEMBER_ID;
    private long leadershipTermId;
    private long termPosition = NULL_POSITION;
    private final String clientFacingEndpoint;
    private final String memberFacingEndpoint;
    private final String logEndpoint;
    private final String archiveEndpoint;
    private final String endpointsDetail;
    private Publication publication;

    /**
     * Construct a new member of the cluster.
     *
     * @param id                   unique id for the member.
     * @param clientFacingEndpoint address and port endpoint to which cluster clients connect.
     * @param memberFacingEndpoint address and port endpoint to which other cluster members connect.
     * @param logEndpoint          address and port endpoint to which the log is replicated.
     * @param archiveEndpoint      address and port endpoint to which the archive control channel can be reached.
     * @param endpointsDetail      comma separated list of endpoints.
     */
    public ClusterMember(
        final int id,
        final String clientFacingEndpoint,
        final String memberFacingEndpoint,
        final String logEndpoint,
        final String archiveEndpoint,
        final String endpointsDetail)
    {
        this.id = id;
        this.clientFacingEndpoint = clientFacingEndpoint;
        this.memberFacingEndpoint = memberFacingEndpoint;
        this.logEndpoint = logEndpoint;
        this.archiveEndpoint = archiveEndpoint;
        this.endpointsDetail = endpointsDetail;
    }

    /**
     * Set if this member should be leader.
     *
     * @param isLeader value.
     * @return this for a fluent API.
     */
    public ClusterMember isLeader(final boolean isLeader)
    {
        this.isLeader = isLeader;
        return this;
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
     * Is the ballot for the current election sent to this member?
     *
     * @param isBallotSent is the ballot for the current election sent to this member?
     * @return this for a fluent API.
     */
    public ClusterMember isBallotSent(final boolean isBallotSent)
    {
        this.isBallotSent = isBallotSent;
        return this;
    }

    /**
     * Is the ballot for the current election sent to this member?
     *
     * @return true if the ballot has been sent for this member in the current election.
     */
    public boolean isBallotSent()
    {
        return isBallotSent;
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
     * Set this member voted for in the election.
     *
     * @param votedForId in the election.
     * @return this for a fluent API.
     */
    public ClusterMember votedForId(final int votedForId)
    {
        this.votedForId = votedForId;
        return this;
    }

    /**
     * Who this member voted for in the last election.
     *
     * @return who this member voted for in the last election.
     */
    public int votedForId()
    {
        return votedForId;
    }

    /**
     * The leadership term reached for the cluster member.
     *
     * @param leadershipTermId leadership term reached for the cluster member.
     * @return this for a fluent API.
     */
    public ClusterMember leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
        return this;
    }

    /**
     * The leadership term reached for the cluster member.
     *
     * @return The leadership term reached for the cluster member.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * The log position this member has persisted within the current leadership term.
     *
     * @param termPosition in the log this member has persisted within the current leadership term.
     * @return this for a fluent API.
     */
    public ClusterMember termPosition(final long termPosition)
    {
        this.termPosition = termPosition;
        return this;
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
     * The address:port endpoint for this cluster member that the archive can be reached.
     *
     * @return the address:port endpoint for this cluster member that the archive can be reached.
     */
    public String archiveEndpoint()
    {
        return archiveEndpoint;
    }

    /**
     * The string of endpoints for this member in a comma separated list in the same order they are parsed.
     *
     * @return list of endpoints for this member in a comma separated list.
     * @see #parse(String)
     */
    public String endpointsDetail()
    {
        return endpointsDetail;
    }

    /**
     * The {@link Publication} used for send status updates to the member.
     *
     * @return {@link Publication} used for send status updates to the member.
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * {@link Publication} used for send status updates to the member.
     *
     * @param publication used for send status updates to the member.
     */
    public void publication(final Publication publication)
    {
        this.publication = publication;
    }

    /**
     * Parse the details for a cluster members from a string.
     * <p>
     * <code>
     * member-id,client-facing:port,member-facing:port,log:port,archive:port|1,...
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
            final String endpointsDetail = memberValues[i];
            final String[] memberAttributes = endpointsDetail.split(",");
            if (memberAttributes.length != 5)
            {
                throw new IllegalStateException("Invalid member value: " + endpointsDetail + " within: " + value);
            }

            members[i] = new ClusterMember(
                Integer.parseInt(memberAttributes[0]),
                memberAttributes[1],
                memberAttributes[2],
                memberAttributes[3],
                memberAttributes[4],
                endpointsDetail);
        }

        return members;
    }

    /**
     * Add the publications for sending status messages to the other members of the cluster.
     *
     * @param members    of the cluster.
     * @param exclude    this member when adding publications.
     * @param channelUri for the publication.
     * @param streamId   for the publication.
     * @param aeron      to add the publications to.
     */
    public static void addMemberStatusPublications(
        final ClusterMember[] members,
        final ClusterMember exclude,
        final ChannelUri channelUri,
        final int streamId,
        final Aeron aeron)
    {
        for (final ClusterMember member : members)
        {
            if (member != exclude)
            {
                channelUri.put(ENDPOINT_PARAM_NAME, member.memberFacingEndpoint());
                final String channel = channelUri.toString();
                member.publication(aeron.addExclusivePublication(channel, streamId));
            }
        }
    }

    /**
     * Close the publications associated with members of the cluster.
     *
     * @param clusterMembers to close the publications for.
     */
    public static void closeMemberPublications(final ClusterMember[] clusterMembers)
    {
        for (final ClusterMember member : clusterMembers)
        {
            CloseHelper.close(member.publication());
        }
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

    /**
     * Reset the term position of all the members to the provided value.
     *
     * @param clusterMembers to be reset.
     * @param termPosition   to set for them all.
     */
    public static void resetTermPositions(final ClusterMember[] clusterMembers, final long termPosition)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.termPosition(termPosition);
        }
    }

    /**
     * Has the members of the cluster all reached the provided position in their log.
     *
     * @param clusterMembers to check.
     * @param position       to compare the {@link #termPosition()} against.
     * @return true if all members have reached this position otherwise false.
     */
    public static boolean hasReachedPosition(final ClusterMember[] clusterMembers, final int position)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.termPosition() < position)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Become a candidate by voting for yourself and resetting the other votes to {@link #NULL_MEMBER_ID}.
     *
     * @param clusterMembers    to reset the votes for.
     * @param candidateMemberId for the election.
     */
    public static void becomeCandidate(final ClusterMember[] clusterMembers, final int candidateMemberId)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.votedForId(member.id == candidateMemberId ? candidateMemberId : NULL_MEMBER_ID);
            member.isBallotSent(member.id == candidateMemberId);
        }
    }

    /**
     * Are we still awaiting votes from some cluster members?
     *
     * @param clusterMembers to check for votes.
     * @return true if votes are outstanding.
     */
    public static boolean awaitingVotes(final ClusterMember[] clusterMembers)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.votedForId() == NULL_MEMBER_ID)
            {
                return true;
            }
        }

        return false;
    }

    public static void checkArchiveEndpoint(final ClusterMember thisMember, final ChannelUri archiveControlRequestUri)
    {
        if (!UDP_MEDIA.equals(archiveControlRequestUri.media()))
        {
            throw new IllegalStateException("archive control request channel must be udp");
        }

        final String archiveEndpoint = archiveControlRequestUri.get(ENDPOINT_PARAM_NAME);

        if (archiveEndpoint != null && !archiveEndpoint.equals(thisMember.archiveEndpoint))
        {
            throw new IllegalStateException(
                "archive control request endpoint must match cluster members configuration: " + archiveEndpoint +
                " != " + thisMember.archiveEndpoint);
        }
    }

    /**
     * Is the member certain to be a suitable candidate in an election.
     *
     * @param clusterMembers to compare the candidate against.
     * @param candidate      for leadership.
     * @return true if the candidate is suitable otherwise false.
     */
    public static boolean isCertainCandidate(final ClusterMember[] clusterMembers, final ClusterMember candidate)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member == candidate)
            {
                continue;
            }

            if (NULL_POSITION == member.termPosition)
            {
                return false;
            }

            if (candidate.leadershipTermId < member.leadershipTermId || candidate.termPosition < member.termPosition)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Is the member achieved a quorum view to be a suitable candidate in an election.
     *
     * @param clusterMembers to compare the candidate against.
     * @param candidate      for leadership.
     * @return true if the candidate is suitable otherwise false.
     */
    public static boolean isQuorumCandidate(final ClusterMember[] clusterMembers, final ClusterMember candidate)
    {
        int possibleVotes = 0;
        for (final ClusterMember member : clusterMembers)
        {
            if (NULL_POSITION == member.termPosition ||
                candidate.leadershipTermId < member.leadershipTermId ||
                candidate.termPosition < member.termPosition)
            {
                continue;
            }

            ++possibleVotes;
        }

        return possibleVotes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }
}
