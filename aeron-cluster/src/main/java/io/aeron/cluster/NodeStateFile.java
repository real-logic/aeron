/*
 * Copyright 2014-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.node.*;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import static org.agrona.BitUtil.align;
import static org.agrona.concurrent.UnsafeBuffer.ALIGNMENT;

/**
 * An extensible list of information relating to a specific cluster node.  Used to track persistent state that is node
 * specific and shouldn't be present in the snapshot.  E.g. candidateTermId.
 * <p>
 *     The structure consists of a node header at the beginning of the file followed by n entries that use the
 *     standard open framing header, followed by the message header, and finally the body.
 * </p>
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                     Node State Header                         |
 *  +---------------------------------------------------------------+
 *  </pre>
 *  <p>
 *      Entry.  All messages must be laid so that the message body has an 8-byte alignment.  Fields with the message
 *      body that required volatile accesses must be aligned to an 8 byte boundary.  The framing header and message
 *      header are 8-bytes long to aid with this.
 *  </p>
 *  <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +---------------------------------------------------------------+
 *  |                       Message Header                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Message Body (Variable)               ...
 *  ...                                                             |
 *  +---------------------------------------------------------------+
 * </pre>
 * <p>
 *     The current structure contains:
 * <pre>
 *     &lt;Node State Header&gt;
 *     &lt;Candidate Term&gt;
 *     &lt;Cluster Members&gt;
 *     &lt;Node State Footer&gt;
 * </pre>
 */
public class NodeStateFile implements AutoCloseable
{
    public static final String FILENAME = "node-state.dat";
    private static final int MINIMUM_FILE_LENGTH = 1 << 20;
    private final CandidateTerm candidateTerm = new CandidateTerm();
    private final ClusterMembers clusterMembers = new ClusterMembers();
    private final MappedByteBuffer mappedFile;
    private final File clusterDir;
    private final int fileSyncLevel;
    private final NodeStateHeaderDecoder nodeStateHeaderDecoder = new NodeStateHeaderDecoder();
    private final ClusterMembersDecoder clusterMembersDecoder = new ClusterMembersDecoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final CandidateTermDecoder candidateTermDecoder = new CandidateTermDecoder();
    private final UnsafeBuffer buffer;
    private int candidateTermIdOffset;

    /**
     * Construct the NodeStateFile.
     *
     * @param clusterDir    directory containing the NodeStateFile.
     * @param createNew     whether a new file should be created if one does not already exist.
     * @param fileSyncLevel whether the mapped byte buffer should be synchronised with the underlying filesystem on
     *                      change.
     * @throws IOException if there is an error creating the file or <code>createNew == false</code> and the file does
     * not already exist.
     */
    public NodeStateFile(final File clusterDir, final boolean createNew, final int fileSyncLevel) throws IOException
    {
        this.clusterDir = clusterDir;
        this.fileSyncLevel = fileSyncLevel;
        final UnsafeBuffer buffer;
        MappedByteBuffer mappedFile = null;

        try
        {
            final File nodeStateFile = new File(clusterDir, NodeStateFile.FILENAME);
            if (!nodeStateFile.exists())
            {
                if (!createNew)
                {
                    throw new IOException("NodeStateFile does not exist and createNew=false");
                }

                mappedFile = IoUtil.mapNewFile(nodeStateFile, MINIMUM_FILE_LENGTH);
                buffer = new UnsafeBuffer(mappedFile, 0, mappedFile.capacity());
                buffer.verifyAlignment();

                initialiseEncodersAndDecodersOnCreation(
                    buffer,
                    nodeStateHeaderDecoder,
                    messageHeaderDecoder,
                    candidateTermDecoder,
                    clusterMembersDecoder);

                candidateTermIdOffset = calculateAndValidateCandidateTermIdOffset();
                buffer.putLongVolatile(candidateTermIdOffset, Aeron.NULL_VALUE);
            }
            else
            {
                mappedFile = IoUtil.mapExistingFile(nodeStateFile, "NodeState");
                buffer = new UnsafeBuffer(mappedFile, 0, mappedFile.capacity());

                loadDecodersAndOffsets(buffer);
            }

            syncFile(mappedFile);
        }
        catch (final IOException | RuntimeException ex)
        {
            if (null != mappedFile)
            {
                IoUtil.unmap(mappedFile);
            }
            throw ex;
        }

        this.mappedFile = mappedFile;
        this.buffer = buffer;
    }

    private int calculateAndValidateCandidateTermIdOffset()
    {
        final int candidateTermIdOffset;
        candidateTermIdOffset =
            candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();
        verifyAlignment(candidateTermIdOffset);
        return candidateTermIdOffset;
    }

    private static void verifyAlignment(final int offset)
    {
        if (0 != (offset & (ALIGNMENT - 1)))
        {
            throw new IllegalStateException(
                "offset=" + offset + " is not correctly aligned, it is not divisible by " + ALIGNMENT);
        }
    }

    private static void loadInitialState(
        final MutableDirectBuffer buffer,
        final NodeStateHeaderDecoder nodeStateHeaderDecoder,
        final CandidateTermDecoder candidateTermDecoder,
        final ClusterMembersDecoder clusterMembersDecoder,
        final MessageHeaderDecoder messageHeaderDecoder)
    {
        nodeStateHeaderDecoder.wrap(
            buffer, 0, NodeStateHeaderDecoder.BLOCK_LENGTH, NodeStateHeaderDecoder.SCHEMA_VERSION);

        final int version = nodeStateHeaderDecoder.version();
        if (ClusterMarkFile.MAJOR_VERSION != SemanticVersion.major(version))
        {
            throw new ClusterException(
                "mark file major version " + SemanticVersion.major(version) +
                " does not match software: " + ClusterMarkFile.MAJOR_VERSION);
        }

        final int footerOffset = scanForMessageTypeOffset(
            nodeStateHeaderDecoder.sbeBlockLength(),
            NodeStateFooterDecoder.TEMPLATE_ID,
            buffer,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == footerOffset)
        {
            throw new IllegalStateException("failed to find NodeStateFooter entry, file corrupt?");
        }

        final int candidateTermOffset = scanForMessageTypeOffset(
            nodeStateHeaderDecoder.sbeBlockLength(),
            CandidateTermDecoder.TEMPLATE_ID,
            buffer,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == candidateTermOffset)
        {
            throw new IllegalStateException("failed to find CandidateTerm entry");
        }

        candidateTermDecoder.wrapAndApplyHeader(buffer, candidateTermOffset, messageHeaderDecoder);

        final int clusterMembersOffset = scanForMessageTypeOffset(
            nodeStateHeaderDecoder.sbeBlockLength(),
            ClusterMembersDecoder.TEMPLATE_ID,
            buffer,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == clusterMembersOffset)
        {
            throw new IllegalStateException("failed to find ClusterMembers entry");
        }

        clusterMembersDecoder.wrapAndApplyHeader(buffer, clusterMembersOffset, messageHeaderDecoder);
    }

    private static int scanForMessageTypeOffset(
        final int startPosition,
        final int templateId,
        final DirectBuffer buffer,
        final MessageHeaderDecoder messageHeaderDecoder)
    {
        verifyAlignment(startPosition);
        int position = startPosition;

        while (position < buffer.capacity())
        {
            messageHeaderDecoder.wrap(buffer, position);

            final int messageLength = messageHeaderDecoder.frameLength();

            if (templateId == messageHeaderDecoder.templateId())
            {
                return position;
            }
            else if (NodeStateFooterEncoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
            {
                return Aeron.NULL_VALUE;
            }

            if (messageLength < 0)
            {
                throw new IllegalStateException("Message length < 0, file corrupt?");
            }
            else if (0 == messageLength)
            {
                return Aeron.NULL_VALUE;
            }

            position += align(messageLength, ALIGNMENT);
        }

        return Aeron.NULL_VALUE;
    }

    private static void initialiseEncodersAndDecodersOnCreation(
        final MutableDirectBuffer buffer,
        final NodeStateHeaderDecoder nodeStateHeaderDecoder,
        final MessageHeaderDecoder messageHeaderDecoder,
        final CandidateTermDecoder candidateTermDecoder,
        final ClusterMembersDecoder clusterMembersDecoder)
    {
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

        nodeStateHeaderDecoder.wrap(
            buffer, 0, NodeStateHeaderDecoder.BLOCK_LENGTH, NodeStateHeaderDecoder.SCHEMA_VERSION);
        new NodeStateHeaderEncoder()
            .wrap(buffer, 0)
            .version(ClusterMarkFile.SEMANTIC_VERSION);

        final int candidateTermFrameOffset = NodeStateHeaderDecoder.BLOCK_LENGTH;
        verifyAlignment(candidateTermFrameOffset);

        // Set candidateTermId
        final CandidateTermEncoder candidateTermEncoder = new CandidateTermEncoder();
        candidateTermEncoder.wrapAndApplyHeader(buffer, candidateTermFrameOffset, messageHeaderEncoder);
        candidateTermDecoder.wrapAndApplyHeader(buffer, candidateTermFrameOffset, messageHeaderDecoder);
        candidateTermEncoder
            .logPosition(Aeron.NULL_VALUE)
            .timestamp(Aeron.NULL_VALUE)
            .candidateTermId(Aeron.NULL_VALUE);
        messageHeaderEncoder.frameLength(MessageHeaderEncoder.ENCODED_LENGTH + candidateTermEncoder.encodedLength());

        final int clusterMembersFrameOffset = candidateTermFrameOffset +
            align(messageHeaderDecoder.frameLength(), ALIGNMENT);
        verifyAlignment(candidateTermFrameOffset);

        final ClusterMembersEncoder clusterMembersEncoder = new ClusterMembersEncoder();
        clusterMembersEncoder.wrapAndApplyHeader(buffer, clusterMembersFrameOffset, messageHeaderEncoder);
        clusterMembersDecoder.wrapAndApplyHeader(buffer, clusterMembersFrameOffset, messageHeaderDecoder);
        clusterMembersEncoder
            .leadershipTermId(Aeron.NULL_VALUE)
            .memberId(Aeron.NULL_VALUE)
            .highMemberId(Aeron.NULL_VALUE)
            .clusterMembers("");
        messageHeaderEncoder.frameLength(MessageHeaderEncoder.ENCODED_LENGTH + clusterMembersEncoder.encodedLength());

        final int footerOffset = clusterMembersFrameOffset + align(messageHeaderDecoder.frameLength(), ALIGNMENT);
        final NodeStateFooterEncoder nodeStateFooterEncoder = new NodeStateFooterEncoder();
        nodeStateFooterEncoder.wrapAndApplyHeader(buffer, footerOffset, messageHeaderEncoder);
        messageHeaderEncoder
            .frameLength(MessageHeaderEncoder.ENCODED_LENGTH + nodeStateFooterEncoder.encodedLength()); // Fixed length
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        IoUtil.unmap(mappedFile);
    }

    /**
     * Set the current candidate term id with associated information
     * @param candidateTermId   current candidate term id
     * @param logPosition       log position where the term id change occurred
     * @param timestampMs       timestamp of the candidate term id change
     */
    public void updateCandidateTermId(final long candidateTermId, final long logPosition, final long timestampMs)
    {
        buffer.putLong(candidateTermDecoder.offset() + CandidateTermDecoder.logPositionEncodingOffset(), logPosition);
        buffer.putLong(candidateTermDecoder.offset() + CandidateTermDecoder.timestampEncodingOffset(), timestampMs);
        buffer.putLongVolatile(candidateTermIdOffset, candidateTermId);
        syncFile(mappedFile);
    }

    /**
     * Set the current candidate term id with associated information
     * @param candidateTermId   current candidate term id
     * @param logPosition       log position where the term id change occurred
     * @param timestampMs       timestamp of the candidate term id change
     * @return the new candidate term id.
     */
    public long proposeMaxCandidateTermId(final long candidateTermId, final long logPosition, final long timestampMs)
    {
        final long existingCandidateTermId = candidateTerm.candidateTermId();

        if (candidateTermId > existingCandidateTermId)
        {
            updateCandidateTermId(candidateTermId, logPosition, timestampMs);
            return candidateTermId;
        }

        return existingCandidateTermId;
    }

    /**
     * Get the reference to CandidateTerm wrapper that can be used to fetch the values associated with the current
     * candidate term.
     *
     * @return the CandidateTerm wrapper.
     */
    public CandidateTerm candidateTerm()
    {
        return candidateTerm;
    }

    /**
     * Get the reference to the ClusterMembers wrapper that can be used to fetch the values associated with the most
     * recent change to the list of cluster members.
     *
     * @return the most recently updated cluster members, <code>null</code> if it has never changed.
     */
    public ClusterMembers clusterMembers()
    {
        if (Aeron.NULL_VALUE == clusterMembersDecoder.leadershipTermId())
        {
            return null;
        }

        return clusterMembers;
    }

    /**
     * Update the cluster members entry with the supplied values.  This is a slow operation as the cluster members entry
     * is a dynamically sized record.  To work correctly it will need to move all the trailing records in the file so
     * that they line up correctly.  After updating the file it will rescan to ensure all of the cached decoders have
     * the correct offsets.
     *
     * @param leadershipTermId      leadership where the new set of cluster members reached consensus.
     * @param memberId              current member id.
     * @param highMemberId          high member id.
     * @param clusterMembers        list of all the members in the cluster with the associated endpoints.
     */
    public void updateClusterMembers(
        final long leadershipTermId,
        final int memberId,
        final int highMemberId,
        final String clusterMembers)
    {
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final ClusterMembersEncoder clusterMembersEncoder = new ClusterMembersEncoder();
        final MutableDirectBuffer tempBuffer = new ExpandableArrayBuffer(128);
        clusterMembersEncoder.wrapAndApplyHeader(tempBuffer, 0, messageHeaderEncoder);
        clusterMembersEncoder
            .leadershipTermId(leadershipTermId)
            .memberId(memberId)
            .highMemberId(highMemberId)
            .clusterMembers(clusterMembers);
        final int newFrameLength = messageHeaderEncoder.encodedLength() + clusterMembersEncoder.encodedLength();
        messageHeaderEncoder.frameLength(newFrameLength);

        final int clusterMembersOffset = clusterMembersDecoder.offset();

        insertRecord(tempBuffer, clusterMembersOffset, newFrameLength);
    }

    private void insertRecord(
        final MutableDirectBuffer record,
        final int recordOffset,
        final int recordLength)
    {
        final int frameOffset = recordOffset - MessageHeaderDecoder.ENCODED_LENGTH;
        messageHeaderDecoder.wrap(buffer, frameOffset);
        final int existingFrameLengthAligned = align(messageHeaderDecoder.frameLength(), ALIGNMENT);
        final int nextRecordOffset = frameOffset + existingFrameLengthAligned;
        final int amountToMoveTrailingRecords = align(recordLength, ALIGNMENT) - existingFrameLengthAligned;
        final int newNextRecordOffset = nextRecordOffset + amountToMoveTrailingRecords;

        final int footerOffset = scanForMessageTypeOffset(
            nodeStateHeaderDecoder.encodedLength(),
            NodeStateFooterDecoder.TEMPLATE_ID,
            buffer,
            messageHeaderDecoder);
        if (Aeron.NULL_VALUE == footerOffset)
        {
            throw new IllegalStateException("failed to find footer, file corrupt?");
        }

        final int footerEndOffset =
            footerOffset + MessageHeaderDecoder.ENCODED_LENGTH + NodeStateFooterDecoder.BLOCK_LENGTH;
        final int lengthOfTrailingRecords = footerEndOffset - nextRecordOffset;
        moveTrailingRecords(buffer, nextRecordOffset, lengthOfTrailingRecords, newNextRecordOffset);

        buffer.putBytes(frameOffset, record, 0, recordLength);
        loadDecodersAndOffsets(buffer);

        syncFile(mappedFile);
    }

    private void loadDecodersAndOffsets(final UnsafeBuffer buffer)
    {
        loadInitialState(
            buffer,
            nodeStateHeaderDecoder,
            candidateTermDecoder,
            clusterMembersDecoder,
            messageHeaderDecoder);

        candidateTermIdOffset = calculateAndValidateCandidateTermIdOffset();
    }

    static void moveTrailingRecords(
        final UnsafeBuffer buffer,
        final int srcOffset,
        final int length,
        final int dstOffset)
    {
        verifyAlignment(length);

        if (dstOffset < srcOffset)
        {
            for (int i = 0; i < length; i += ALIGNMENT)
            {
                buffer.putLong(dstOffset + i, buffer.getLong(srcOffset + i));
            }
        }
        else if (srcOffset < dstOffset)
        {
            for (int i = (length - ALIGNMENT); 0 <= i; i -= ALIGNMENT)
            {
                buffer.putLong(dstOffset + i, buffer.getLong(srcOffset + i));
            }
        }
    }

    /**
     * Wrapper class for the candidate term.
     */
    public final class CandidateTerm
    {
        private CandidateTerm()
        {
        }

        /**
         * Gets the current candidateTermId.
         * @return the candidateTermId
         */
        public long candidateTermId()
        {
            return buffer.getLongVolatile(candidateTermIdOffset);
        }

        /**
         * Get the timestamp of the latest candidateTermId update.
         *
         * @return epoch timestamp in ms.
         */
        public long timestamp()
        {
            return candidateTermDecoder.timestamp();
        }

        /**
         * Get the log position of the latest candidateTermId update
         *
         * @return log position.
         */
        public long logPosition()
        {
            return candidateTermDecoder.logPosition();
        }
    }

    /**
     * Wrapper class for the most recent set of cluster members.
     */
    public final class ClusterMembers
    {
        /**
         * This node's cluster member id.
         *
         * @return this node's cluster member id.
         */
        public int memberId()
        {
            return clusterMembersDecoder.memberId();
        }

        /**
         * High member id.
         * @return high member id.
         */
        public int highMemberId()
        {
            return clusterMembersDecoder.highMemberId();
        }

        /**
         * Leadership term when the new set of cluster members reached consensus.
         *
         * @return id of the leadership term.
         */
        public long leadershipTermId()
        {
            return clusterMembersDecoder.leadershipTermId();
        }

        /**
         * List of the cluster members along with their associated endpoints in the format specified by
         * {@link ConsensusModule.Configuration#CLUSTER_MEMBERS_PROP_NAME}.
         *
         * @return cluster members list.
         * @see ConsensusModule.Configuration#CLUSTER_MEMBERS_PROP_NAME
         */
        public String clusterMembers()
        {
            return clusterMembersDecoder.clusterMembers();
        }
    }

    private void syncFile(final MappedByteBuffer mappedFile)
    {
        if (0 < fileSyncLevel)
        {
            mappedFile.force();
        }
    }
}
