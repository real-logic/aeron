/*
 * Copyright 2014-2024 Real Logic Limited.
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
 * An extensible list of information relating to a specific cluster node. Used to track persistent state that is node
 * specific and shouldn't be present in the snapshot. E.g. candidateTermId.
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
 *      Entry. All records must be laid out so that the body has an 8-byte alignment. Fields that require volatile
 *      accesses must be aligned to an 8-byte boundary. The message header is 8-bytes long to aid with this.
 *      Records are padded to a 8-byte boundary before the next record.
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
    /**
     * File name.
     */
    public static final String FILENAME = "node-state.dat";
    private static final int MINIMUM_FILE_LENGTH = 1 << 20;
    private final CandidateTerm candidateTerm = new CandidateTerm();
    private final MappedByteBuffer mappedFile;
    private final int fileSyncLevel;
    private final NodeStateHeaderDecoder nodeStateHeaderDecoder = new NodeStateHeaderDecoder();
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

                initialiseDecodersOnCreation(
                    buffer,
                    nodeStateHeaderDecoder,
                    messageHeaderDecoder,
                    candidateTermDecoder);

                candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
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

    private int calculateAndVerifyCandidateTermIdOffset()
    {
        final int candidateTermIdOffset;
        candidateTermIdOffset = candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();
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
    }

    private static void initialiseDecodersOnCreation(
        final MutableDirectBuffer buffer,
        final NodeStateHeaderDecoder nodeStateHeaderDecoder,
        final MessageHeaderDecoder messageHeaderDecoder,
        final CandidateTermDecoder candidateTermDecoder)
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

        final int footerOffset = candidateTermFrameOffset + align(messageHeaderDecoder.frameLength(), ALIGNMENT);
        final NodeStateFooterEncoder nodeStateFooterEncoder = new NodeStateFooterEncoder();
        nodeStateFooterEncoder.wrapAndApplyHeader(buffer, footerOffset, messageHeaderEncoder);
        messageHeaderEncoder
            .frameLength(MessageHeaderEncoder.ENCODED_LENGTH + nodeStateFooterEncoder.encodedLength());
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        IoUtil.unmap(mappedFile);
    }

    /**
     * Set the current candidate term id with associated information.
     *
     * @param candidateTermId current candidate term id.
     * @param logPosition    log position where the term id change occurred.
     * @param timestampMs    timestamp of the candidate term id change.
     */
    public void updateCandidateTermId(final long candidateTermId, final long logPosition, final long timestampMs)
    {
        buffer.putLong(candidateTermDecoder.offset() + CandidateTermDecoder.logPositionEncodingOffset(), logPosition);
        buffer.putLong(candidateTermDecoder.offset() + CandidateTermDecoder.timestampEncodingOffset(), timestampMs);
        buffer.putLongVolatile(candidateTermIdOffset, candidateTermId);
        syncFile(mappedFile);
    }

    /**
     * Set the current candidate term id with associated information.
     *
     * @param candidateTermId current candidate term id.
     * @param logPosition     log position where the term id change occurred.
     * @param timestampMs     timestamp of the candidate term id change.
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

    private void loadDecodersAndOffsets(final UnsafeBuffer buffer)
    {
        loadInitialState(
            buffer,
            nodeStateHeaderDecoder,
            candidateTermDecoder,
            messageHeaderDecoder);

        candidateTermIdOffset = calculateAndVerifyCandidateTermIdOffset();
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
         *
         * @return the candidateTermId.
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
         * Get the log position of the latest candidateTermId update.
         *
         * @return log position.
         */
        public long logPosition()
        {
            return candidateTermDecoder.logPosition();
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
