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
import io.aeron.cluster.client.ClusterException;
import org.agrona.CloseHelper;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Represents a member of the cluster that participates in consensus.
 */
public final class ClusterMember
{
    private boolean isBallotSent;
    private boolean isLeader;
    private final int id;
    private long leadershipTermId = Aeron.NULL_VALUE;
    private long logPosition = NULL_POSITION;
    private long candidateTermId = Aeron.NULL_VALUE;
    private long catchupReplaySessionId = Aeron.NULL_VALUE;
    private final String clientFacingEndpoint;
    private final String memberFacingEndpoint;
    private final String logEndpoint;
    private final String transferEndpoint;
    private final String archiveEndpoint;
    private final String endpointsDetail;
    private Publication publication;
    private Boolean vote = null;

    /**
     * Construct a new member of the cluster.
     *
     * @param id                   unique id for the member.
     * @param clientFacingEndpoint address and port endpoint to which cluster clients connect.
     * @param memberFacingEndpoint address and port endpoint to which other cluster members connect.
     * @param logEndpoint          address and port endpoint to which the log is replicated.
     * @param transferEndpoint     address and port endpoint to which a stream is replayed to catchup the leader.
     * @param archiveEndpoint      address and port endpoint to which the archive control channel can be reached.
     * @param endpointsDetail      comma separated list of endpoints.
     */
    public ClusterMember(
        final int id,
        final String clientFacingEndpoint,
        final String memberFacingEndpoint,
        final String logEndpoint,
        final String transferEndpoint,
        final String archiveEndpoint,
        final String endpointsDetail)
    {
        this.id = id;
        this.clientFacingEndpoint = clientFacingEndpoint;
        this.memberFacingEndpoint = memberFacingEndpoint;
        this.logEndpoint = logEndpoint;
        this.transferEndpoint = transferEndpoint;
        this.archiveEndpoint = archiveEndpoint;
        this.endpointsDetail = endpointsDetail;
    }

    /**
     * Reset the state of a cluster member so it can be canvassed and reestablished.
     */
    public void reset()
    {
        isBallotSent = false;
        isLeader = false;
        vote = null;
        candidateTermId = Aeron.NULL_VALUE;
        leadershipTermId = Aeron.NULL_VALUE;
        logPosition = NULL_POSITION;
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
     * Set the result of the vote for this member. {@link Boolean#TRUE} means they voted for this member,
     * {@link Boolean#FALSE} means they voted against this member, and null means no vote was received.
     *
     * @param vote for this member in the election.
     * @return this for a fluent API.
     */
    public ClusterMember vote(final Boolean vote)
    {
        this.vote = vote;
        return this;
    }

    /**
     * The status of the vote for this member in an election. {@link Boolean#TRUE} means they voted for this member,
     * {@link Boolean#FALSE} means they voted against this member, and null means no vote was received.
     *
     * @return the status of the vote for this member in an election.
     */
    public Boolean vote()
    {
        return vote;
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
     * The log position this member has persisted.
     *
     * @param logPosition this member has persisted.
     * @return this for a fluent API.
     */
    public ClusterMember logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
        return this;
    }

    /**
     * The log position this member has persisted.
     *
     * @return the log position this member has persisted.
     */
    public long logPosition()
    {
        return logPosition;
    }

    /**
     * The candidate term id used when voting.
     *
     * @param candidateTermId used when voting.
     * @return this for a fluent API.
     */
    public ClusterMember candidateTermId(final long candidateTermId)
    {
        this.candidateTermId = candidateTermId;
        return this;
    }

    /**
     * The candidate term id used when voting.
     *
     * @return the candidate term id used when voting.
     */
    public long candidateTermId()
    {
        return candidateTermId;
    }

    public ClusterMember catchupReplaySessionId(final long replaySessionId)
    {
        this.catchupReplaySessionId = replaySessionId;
        return this;
    }

    public long catchupReplaySessionId()
    {
        return catchupReplaySessionId;
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
     * The address:port endpoint for this cluster member to which a stream is replayed to for leader catchup.
     *
     * @return the address:port endpoint for this cluster member to which a stream is replayed to for leader catchup.
     */
    public String transferEndpoint()
    {
        return transferEndpoint;
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
     * member-id,client-facing:port,member-facing:port,log:port,transfer:port,archive:port|1,...
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
            if (memberAttributes.length != 6)
            {
                throw new ClusterException("invalid member value: " + endpointsDetail + " within: " + value);
            }

            members[i] = new ClusterMember(
                Integer.parseInt(memberAttributes[0]),
                memberAttributes[1],
                memberAttributes[2],
                memberAttributes[3],
                memberAttributes[4],
                memberAttributes[5],
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
            CloseHelper.close(member.publication);
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
            long newPosition = member.logPosition;

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
     * Reset the log position of all the members to the provided value.
     *
     * @param clusterMembers to be reset.
     * @param logPosition    to set for them all.
     */
    public static void resetLogPositions(final ClusterMember[] clusterMembers, final long logPosition)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.logPosition(logPosition);
        }
    }

    /**
     * Has the members of the cluster the voted reached the provided position in their log.
     *
     * @param clusterMembers   to check.
     * @param position         to compare the {@link #logPosition()} against.
     * @param leadershipTermId expected of the members.
     * @return true if all members have reached this position otherwise false.
     */
    public static boolean haveVotersReachedPosition(
        final ClusterMember[] clusterMembers, final long position, final long leadershipTermId)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.vote != null && (member.logPosition < position || member.leadershipTermId != leadershipTermId))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Reset the state of all cluster members.
     *
     * @param clusterMembers to reset.
     */
    public static void reset(final ClusterMember[] clusterMembers)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.reset();
        }
    }

    /**
     * Become a candidate by voting for yourself and resetting the other votes to {@link Aeron#NULL_VALUE}.
     *
     * @param clusterMembers    to reset the votes for.
     * @param candidateTermId   for the candidacy.
     * @param candidateMemberId for the election.
     */
    public static void becomeCandidate(
        final ClusterMember[] clusterMembers, final long candidateTermId, final int candidateMemberId)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.id == candidateMemberId)
            {
                member.vote(Boolean.TRUE)
                    .candidateTermId(candidateTermId)
                    .isBallotSent(true);
            }
            else
            {
                member.vote(null)
                    .candidateTermId(Aeron.NULL_VALUE)
                    .isBallotSent(false);
            }
        }
    }

    /**
     * Has the candidate got unanimous support of the cluster?
     *
     * @param clusterMembers  to check for votes.
     * @param candidateTermId for the vote.
     * @return false if any member has not voted for the candidate.
     */
    public static boolean hasWonVoteOnFullCount(final ClusterMember[] clusterMembers, final long candidateTermId)
    {
        int votes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (null == member.vote || member.candidateTermId != candidateTermId)
            {
                return false;
            }

            votes += member.vote ? 1 : 0;
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Has sufficient votes being counted for a majority for all members observed during {@link Election.State#CANVASS}?
     *
     * @param clusterMembers  to check for votes.
     * @param candidateTermId for the vote.
     * @return false if any member has not voted for the candidate.
     */
    public static boolean hasMajorityVoteWithCanvassMembers(
        final ClusterMember[] clusterMembers, final long candidateTermId)
    {
        int votes = 0;
        for (final ClusterMember member : clusterMembers)
        {
            if (NULL_POSITION != member.logPosition && null == member.vote)
            {
                return false;
            }

            if (Boolean.TRUE.equals(member.vote) && member.candidateTermId == candidateTermId)
            {
                ++votes;
            }
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Has sufficient votes being counted for a majority?
     *
     * @param clusterMembers  to check for votes.
     * @param candidateTermId for the vote.
     * @return true if a majority of positive votes.
     */
    public static boolean hasMajorityVote(final ClusterMember[] clusterMembers, final long candidateTermId)
    {
        int votes = 0;
        for (final ClusterMember member : clusterMembers)
        {
            if (Boolean.TRUE.equals(member.vote) && member.candidateTermId == candidateTermId)
            {
                ++votes;
            }
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Check that the archive endpoint is correctly configured for the cluster member.
     *
     * @param member                   to check the configuration.
     * @param archiveControlRequestUri for the parsed URI string.
     */
    public static void checkArchiveEndpoint(final ClusterMember member, final ChannelUri archiveControlRequestUri)
    {
        if (!UDP_MEDIA.equals(archiveControlRequestUri.media()))
        {
            throw new ClusterException("archive control request channel must be udp");
        }

        final String archiveEndpoint = archiveControlRequestUri.get(ENDPOINT_PARAM_NAME);
        if (archiveEndpoint != null && !archiveEndpoint.equals(member.archiveEndpoint))
        {
            throw new ClusterException(
                "archive control request endpoint must match cluster member configuration: " + archiveEndpoint +
                " != " + member.archiveEndpoint);
        }
    }

    /**
     * Has the member achieved a unanimous view to be a suitable candidate in an election.
     *
     * @param clusterMembers to compare the candidate against.
     * @param candidate      for leadership.
     * @return true if the candidate is suitable otherwise false.
     */
    public static boolean isUnanimousCandidate(final ClusterMember[] clusterMembers, final ClusterMember candidate)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (NULL_POSITION == member.logPosition || compareLog(candidate, member) < 0)
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Has the member achieved a quorum view to be a suitable candidate in an election.
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
            if (NULL_POSITION == member.logPosition || compareLog(candidate, member) < 0)
            {
                continue;
            }

            ++possibleVotes;
        }

        return possibleVotes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * The result is positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     *
     * @param lhsLogLeadershipTermId term for which the position is most recent.
     * @param lhsLogPosition         reached in the provided term.
     * @param rhsLogLeadershipTermId term for which the position is most recent.
     * @param rhsLogPosition         reached in the provided term.
     * @return positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     *         recent log.
     */
    public static int compareLog(
        final long lhsLogLeadershipTermId,
        final long lhsLogPosition,
        final long rhsLogLeadershipTermId,
        final long rhsLogPosition)
    {
        if (lhsLogLeadershipTermId > rhsLogLeadershipTermId)
        {
            return 1;
        }
        else if (lhsLogLeadershipTermId < rhsLogLeadershipTermId)
        {
            return -1;
        }
        else if (lhsLogPosition > rhsLogPosition)
        {
            return 1;
        }
        else if (lhsLogPosition < rhsLogPosition)
        {
            return -1;
        }

        return 0;
    }

    /**
     * The result is positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     *
     * @param lhsMember to compare.
     * @param rhsMember to compare.
     * @return positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     *         recent log.
     */
    public static int compareLog(final ClusterMember lhsMember, final ClusterMember rhsMember)
    {
        return compareLog(
            lhsMember.leadershipTermId, lhsMember.logPosition, rhsMember.leadershipTermId, rhsMember.logPosition);
    }

    public String toString()
    {
        return "ClusterMember{" +
            "id=" + id +
            ", isLeader=" + isLeader +
            ", isBallotSent=" + isBallotSent +
            ", vote=" + vote +
            ", leadershipTermId=" + leadershipTermId +
            ", logPosition=" + logPosition +
            ", candidateTermId=" + candidateTermId +
            ", clientFacingEndpoint='" + clientFacingEndpoint + '\'' +
            ", memberFacingEndpoint='" + memberFacingEndpoint + '\'' +
            ", logEndpoint='" + logEndpoint + '\'' +
            ", transferEndpoint='" + transferEndpoint + '\'' +
            ", archiveEndpoint='" + archiveEndpoint + '\'' +
            ", endpointsDetail='" + endpointsDetail + '\'' +
            '}';
    }
}
