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
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Framing Header                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Message Header                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                       Message Body (Variable                ...
 *  ...                                                             |
 *  +---------------------------------------------------------------+
 * </pre>
 * <p>
 *     The current structure contains:
 * <pre>
 *     &lt;Node State Header&gt;
 *     &lt;Candidate Term Id&gt;
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
    private final NodeStateHeaderEncoder nodeStateHeaderEncoder = new NodeStateHeaderEncoder();
    private final ClusterMembersEncoder clusterMembersEncoder = new ClusterMembersEncoder();
    private final ClusterMembersDecoder clusterMembersDecoder = new ClusterMembersDecoder();
    private final FramingHeaderDecoder framingHeaderDecoder = new FramingHeaderDecoder();
    private final FramingHeaderEncoder framingHeaderEncoder = new FramingHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final CandidateTermDecoder candidateTermDecoder = new CandidateTermDecoder();
    private final CandidateTermEncoder candidateTermEncoder = new CandidateTermEncoder();
    private final UnsafeBuffer buffer;
    private final int candidateTermIdOffset;

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
                    nodeStateHeaderEncoder,
                    framingHeaderDecoder,
                    framingHeaderEncoder,
                    messageHeaderDecoder,
                    messageHeaderEncoder,
                    candidateTermDecoder,
                    candidateTermEncoder,
                    clusterMembersDecoder,
                    clusterMembersEncoder);

                candidateTermIdOffset =
                    candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();
                verifyAlignment(candidateTermIdOffset);

                candidateTermEncoder
                    .logPosition(Aeron.NULL_VALUE)
                    .timestamp(Aeron.NULL_VALUE);

                buffer.putLongVolatile(candidateTermIdOffset, Aeron.NULL_VALUE);
            }
            else
            {
                mappedFile = IoUtil.mapExistingFile(nodeStateFile, "NodeState");
                buffer = new UnsafeBuffer(mappedFile, 0, mappedFile.capacity());

                loadInitialState(
                    buffer,
                    nodeStateHeaderDecoder,
                    nodeStateHeaderEncoder,
                    candidateTermDecoder,
                    candidateTermEncoder,
                    clusterMembersDecoder,
                    clusterMembersEncoder,
                    framingHeaderDecoder,
                    messageHeaderDecoder);

                candidateTermIdOffset =
                    candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();
                verifyAlignment(candidateTermIdOffset);
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
        final NodeStateHeaderEncoder nodeStateHeaderEncoder,
        final CandidateTermDecoder candidateTermDecoder,
        final CandidateTermEncoder candidateTermEncoder,
        final ClusterMembersDecoder clusterMembersDecoder,
        final ClusterMembersEncoder clusterMembersEncoder,
        final FramingHeaderDecoder simpleOpenFramingHeaderDecoder,
        final MessageHeaderDecoder messageHeaderDecoder)
    {
        nodeStateHeaderDecoder.wrap(
            buffer, 0, NodeStateHeaderDecoder.BLOCK_LENGTH, NodeStateHeaderDecoder.SCHEMA_VERSION);
        nodeStateHeaderEncoder.wrap(buffer, 0);

        final int version = nodeStateHeaderDecoder.version();
        if (ClusterMarkFile.MAJOR_VERSION != SemanticVersion.major(version))
        {
            throw new ClusterException(
                "mark file major version " + SemanticVersion.major(version) +
                " does not match software: " + ClusterMarkFile.MAJOR_VERSION);
        }

        final int footerOffset = scanForMessageTypeOffset(
            nodeStateHeaderEncoder.sbeBlockLength(),
            NodeStateFooterDecoder.TEMPLATE_ID,
            buffer,
            simpleOpenFramingHeaderDecoder,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == footerOffset)
        {
            throw new IllegalStateException("failed to find NodeStateFooter entry, file corrupt?");
        }

        final int candidateTermOffset = scanForMessageTypeOffset(
            nodeStateHeaderEncoder.sbeBlockLength(),
            CandidateTermDecoder.TEMPLATE_ID,
            buffer,
            simpleOpenFramingHeaderDecoder,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == candidateTermOffset)
        {
            throw new IllegalStateException("failed to find CandidateTerm entry");
        }

        candidateTermDecoder.wrapAndApplyHeader(buffer, candidateTermOffset, messageHeaderDecoder);
        candidateTermEncoder.wrap(buffer, candidateTermOffset);

        final int clusterMembersOffset = scanForMessageTypeOffset(
            nodeStateHeaderEncoder.sbeBlockLength(),
            ClusterMembersDecoder.TEMPLATE_ID,
            buffer,
            simpleOpenFramingHeaderDecoder,
            messageHeaderDecoder);

        if (Aeron.NULL_VALUE == clusterMembersOffset)
        {
            throw new IllegalStateException("failed to find ClusterMembers entry");
        }

        clusterMembersDecoder.wrapAndApplyHeader(buffer, clusterMembersOffset, messageHeaderDecoder);
        clusterMembersEncoder.wrap(buffer, clusterMembersOffset);
    }

    private static int scanForMessageTypeOffset(
        final int startPosition,
        final int templateId,
        final DirectBuffer buffer,
        final FramingHeaderDecoder simpleOpenFramingHeaderDecoder,
        final MessageHeaderDecoder messageHeaderDecoder)
    {
        verifyAlignment(startPosition);
        int position = startPosition;

        while (position < buffer.capacity())
        {
            simpleOpenFramingHeaderDecoder.wrap(
                buffer,
                position,
                FramingHeaderDecoder.BLOCK_LENGTH,
                FramingHeaderDecoder.SCHEMA_VERSION);

            final int messageLength = simpleOpenFramingHeaderDecoder.messageLength();
            final int messagePosition = position + simpleOpenFramingHeaderDecoder.sbeBlockLength();

            messageHeaderDecoder.wrap(buffer, messagePosition);
            if (templateId == messageHeaderDecoder.templateId())
            {
                return messagePosition;
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
        final NodeStateHeaderEncoder nodeStateHeaderEncoder,
        final FramingHeaderDecoder simpleOpenFramingHeaderDecoder,
        final FramingHeaderEncoder simpleOpenFramingHeaderEncoder,
        final MessageHeaderDecoder messageHeaderDecoder,
        final MessageHeaderEncoder messageHeaderEncoder,
        final CandidateTermDecoder candidateTermDecoder,
        final CandidateTermEncoder candidateTermEncoder,
        final ClusterMembersDecoder clusterMembersDecoder,
        final ClusterMembersEncoder clusterMembersEncoder)
    {
        nodeStateHeaderEncoder.wrap(buffer, 0);
        nodeStateHeaderDecoder.wrap(
            buffer, 0, NodeStateHeaderDecoder.BLOCK_LENGTH, NodeStateHeaderDecoder.SCHEMA_VERSION);

        nodeStateHeaderEncoder.version(ClusterMarkFile.SEMANTIC_VERSION);

        final int candidateTermFrameOffset = nodeStateHeaderEncoder.sbeBlockLength();

        // Set candidateTermId
        simpleOpenFramingHeaderEncoder.wrap(buffer, candidateTermFrameOffset);
        simpleOpenFramingHeaderDecoder.wrap(
            buffer,
            candidateTermFrameOffset,
            FramingHeaderDecoder.BLOCK_LENGTH,
            FramingHeaderDecoder.SCHEMA_VERSION);

        simpleOpenFramingHeaderEncoder.encodingType(CandidateTermDecoder.SCHEMA_ID);

        candidateTermEncoder.wrapAndApplyHeader(
            buffer, candidateTermFrameOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderEncoder);
        candidateTermDecoder.wrapAndApplyHeader(
            buffer, candidateTermFrameOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderDecoder);

        verifyAlignment(candidateTermDecoder.offset());

        simpleOpenFramingHeaderEncoder.messageLength(
            FramingHeaderEncoder.BLOCK_LENGTH +
            MessageHeaderEncoder.ENCODED_LENGTH +
            candidateTermEncoder.encodedLength());

        final int clusterMembersFrameOffset = align(
            candidateTermFrameOffset + simpleOpenFramingHeaderDecoder.messageLength(), ALIGNMENT);

        simpleOpenFramingHeaderEncoder.wrap(buffer, clusterMembersFrameOffset);
        simpleOpenFramingHeaderDecoder.wrap(
            buffer,
            clusterMembersFrameOffset,
            FramingHeaderDecoder.BLOCK_LENGTH,
            FramingHeaderDecoder.SCHEMA_VERSION);
        clusterMembersEncoder.wrapAndApplyHeader(
            buffer, clusterMembersFrameOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderEncoder);
        clusterMembersDecoder.wrapAndApplyHeader(
            buffer, clusterMembersFrameOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderDecoder);
        clusterMembersEncoder
            .leadershipTermId(Aeron.NULL_VALUE)
            .memberId(Aeron.NULL_VALUE)
            .highMemberId(Aeron.NULL_VALUE)
            .clusterMembers("");

        simpleOpenFramingHeaderEncoder.messageLength(
            FramingHeaderEncoder.BLOCK_LENGTH +
            MessageHeaderEncoder.ENCODED_LENGTH +
            clusterMembersEncoder.encodedLength());

        final int footerOffset = align(
            clusterMembersFrameOffset + simpleOpenFramingHeaderDecoder.messageLength(), ALIGNMENT);
        simpleOpenFramingHeaderEncoder.wrap(buffer, footerOffset);
        simpleOpenFramingHeaderDecoder.wrap(
            buffer,
            footerOffset,
            NodeStateFooterDecoder.BLOCK_LENGTH,
            NodeStateFooterDecoder.SCHEMA_VERSION);
        messageHeaderEncoder.wrap(buffer, footerOffset + simpleOpenFramingHeaderEncoder.encodedLength());
        messageHeaderEncoder
            .blockLength(NodeStateFooterEncoder.BLOCK_LENGTH)
            .templateId(NodeStateFooterEncoder.TEMPLATE_ID)
            .schemaId(NodeStateFooterEncoder.SCHEMA_ID)
            .version(NodeStateFooterEncoder.SCHEMA_VERSION);
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
        candidateTermEncoder
            .logPosition(logPosition)
            .timestamp(timestampMs);
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

    public void updateClusterMembers(
        final long leadershipTermId,
        final int memberId,
        final int highClusterMemberId,
        final String clusterMembers)
    {
        final MutableDirectBuffer tempBuffer = new UnsafeBuffer(new byte[MINIMUM_FILE_LENGTH]);
        framingHeaderEncoder.wrap(tempBuffer, 0);
        clusterMembersEncoder.wrapAndApplyHeader(
            tempBuffer, framingHeaderEncoder.encodedLength(), messageHeaderEncoder);
        clusterMembersEncoder
            .leadershipTermId(leadershipTermId)
            .memberId(memberId)
            .highMemberId(highClusterMemberId)
            .clusterMembers(clusterMembers);
        final int newFrameLength = framingHeaderEncoder.encodedLength() + messageHeaderEncoder.encodedLength() +
            clusterMembersEncoder.encodedLength();
        framingHeaderEncoder.messageLength(newFrameLength);

        final int clusterMembersOffset = clusterMembersDecoder.offset();
        final int frameOffset = clusterMembersOffset -
            (FramingHeaderDecoder.BLOCK_LENGTH + MessageHeaderDecoder.ENCODED_LENGTH);
        framingHeaderDecoder.wrap(
            buffer, frameOffset, FramingHeaderDecoder.BLOCK_LENGTH, FramingHeaderDecoder.SCHEMA_VERSION);
        final int existingFrameLengthAligned = align(framingHeaderDecoder.messageLength(), ALIGNMENT);
        final int nextMessageOffset = frameOffset + existingFrameLengthAligned;
        final int amountToMoveTrailingRecords = align(newFrameLength, ALIGNMENT) - existingFrameLengthAligned;
        final int newNextMessageOffset = nextMessageOffset + amountToMoveTrailingRecords;

        final int footerOffset = scanForMessageTypeOffset(
            nodeStateHeaderDecoder.encodedLength(),
            NodeStateFooterDecoder.TEMPLATE_ID,
            buffer,
            framingHeaderDecoder,
            messageHeaderDecoder);
        if (Aeron.NULL_VALUE == footerOffset)
        {
            throw new IllegalStateException("footer");
        }

        final int lengthOfTrailingRecords = (footerOffset + messageHeaderDecoder.encodedLength()) - nextMessageOffset;
        moveTrailingRecords(buffer, nextMessageOffset, lengthOfTrailingRecords, newNextMessageOffset);

        buffer.putBytes(frameOffset, tempBuffer, 0, newFrameLength);
        loadInitialState(
            buffer,
            nodeStateHeaderDecoder,
            nodeStateHeaderEncoder,
            candidateTermDecoder,
            candidateTermEncoder,
            clusterMembersDecoder,
            clusterMembersEncoder,
            framingHeaderDecoder,
            messageHeaderDecoder);

        syncFile(mappedFile);
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
        public int memberId()
        {
            return clusterMembersDecoder.memberId();
        }

        public int highClusterMemberId()
        {
            return clusterMembersDecoder.highMemberId();
        }

        public long leadershipTermId()
        {
            return clusterMembersDecoder.leadershipTermId();
        }

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
