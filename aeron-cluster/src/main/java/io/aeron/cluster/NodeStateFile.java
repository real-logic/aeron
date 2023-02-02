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
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

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
 *      Entry
 *  </p>
 *  <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                Standard Open Framing Header                   |
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
    private static final int MINIMUM_FILE_LENGTH =
        NodeStateHeaderDecoder.BLOCK_LENGTH +
        SimpleOpenFramingHeaderDecoder.BLOCK_LENGTH +
        MessageHeaderDecoder.ENCODED_LENGTH +
        CandidateTermDecoder.BLOCK_LENGTH;
    private final CandidateTerm candidateTerm = new CandidateTerm();
    private final MappedByteBuffer mappedFile;
    private final File clusterDir;
    private int fileSyncLevel;
    private final NodeStateHeaderDecoder nodeStateHeaderDecoder = new NodeStateHeaderDecoder();
    private final NodeStateHeaderEncoder nodeStateHeaderEncoder = new NodeStateHeaderEncoder();
    private final SimpleOpenFramingHeaderDecoder simpleOpenFramingHeaderDecoder = new SimpleOpenFramingHeaderDecoder();
    private final SimpleOpenFramingHeaderEncoder simpleOpenFramingHeaderEncoder = new SimpleOpenFramingHeaderEncoder();
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

                initialiseEncodersAndDecodersOnCreation(
                    buffer,
                    nodeStateHeaderDecoder,
                    nodeStateHeaderEncoder,
                    simpleOpenFramingHeaderDecoder,
                    simpleOpenFramingHeaderEncoder,
                    messageHeaderDecoder,
                    messageHeaderEncoder,
                    candidateTermDecoder,
                    candidateTermEncoder);

                candidateTermIdOffset =
                    candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();

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
                    simpleOpenFramingHeaderDecoder,
                    messageHeaderDecoder);

                candidateTermIdOffset =
                    candidateTermDecoder.offset() + CandidateTermDecoder.candidateTermIdEncodingOffset();
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

    private static void loadInitialState(
        final MutableDirectBuffer buffer,
        final NodeStateHeaderDecoder nodeStateHeaderDecoder,
        final NodeStateHeaderEncoder nodeStateHeaderEncoder,
        final CandidateTermDecoder candidateTermDecoder,
        final CandidateTermEncoder candidateTermEncoder,
        final SimpleOpenFramingHeaderDecoder simpleOpenFramingHeaderDecoder,
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

        final int nodeStateOffset = scanForMessageTypeOffset(
            nodeStateHeaderEncoder.sbeBlockLength(),
            CandidateTermDecoder.TEMPLATE_ID,
            buffer,
            simpleOpenFramingHeaderDecoder,
            messageHeaderDecoder);
        if (Aeron.NULL_VALUE == nodeStateOffset)
        {
            throw new IllegalStateException("failed to find CandidateTerm entry");
        }

        candidateTermDecoder.wrapAndApplyHeader(buffer, nodeStateOffset, messageHeaderDecoder);
        candidateTermEncoder.wrap(buffer, nodeStateOffset);
    }

    private static int scanForMessageTypeOffset(
        final int startPosition,
        final int templateId,
        final DirectBuffer buffer,
        final SimpleOpenFramingHeaderDecoder simpleOpenFramingHeaderDecoder,
        final MessageHeaderDecoder messageHeaderDecoder)
    {
        int position = startPosition;

        while (position < buffer.capacity())
        {
            simpleOpenFramingHeaderDecoder.wrap(
                buffer,
                position,
                SimpleOpenFramingHeaderDecoder.BLOCK_LENGTH,
                SimpleOpenFramingHeaderDecoder.SCHEMA_VERSION);

            final int messageLength = (int)simpleOpenFramingHeaderDecoder.messageLength();
            final int messagePosition = position + simpleOpenFramingHeaderDecoder.sbeBlockLength();

            messageHeaderDecoder.wrap(buffer, messagePosition);
            if (templateId == messageHeaderDecoder.templateId())
            {
                return messagePosition;
            }

            position += messageLength;
        }

        return Aeron.NULL_VALUE;
    }

    private static void initialiseEncodersAndDecodersOnCreation(
        final MutableDirectBuffer buffer,
        final NodeStateHeaderDecoder nodeStateHeaderDecoder,
        final NodeStateHeaderEncoder nodeStateHeaderEncoder,
        final SimpleOpenFramingHeaderDecoder simpleOpenFramingHeaderDecoder,
        final SimpleOpenFramingHeaderEncoder simpleOpenFramingHeaderEncoder,
        final MessageHeaderDecoder messageHeaderDecoder,
        final MessageHeaderEncoder messageHeaderEncoder,
        final CandidateTermDecoder candidateTermDecoder,
        final CandidateTermEncoder candidateTermEncoder)
    {
        nodeStateHeaderEncoder.wrap(buffer, 0);
        nodeStateHeaderDecoder.wrap(
            buffer, 0, NodeStateHeaderDecoder.BLOCK_LENGTH, NodeStateHeaderDecoder.SCHEMA_VERSION);

        nodeStateHeaderEncoder.version(ClusterMarkFile.SEMANTIC_VERSION);

        final int firstFramingHeaderOffset = nodeStateHeaderEncoder.sbeBlockLength();
        // Set candidateTermId
        simpleOpenFramingHeaderEncoder.wrap(buffer, firstFramingHeaderOffset);
        simpleOpenFramingHeaderDecoder.wrap(
            buffer,
            firstFramingHeaderOffset,
            SimpleOpenFramingHeaderDecoder.BLOCK_LENGTH,
            SimpleOpenFramingHeaderDecoder.SCHEMA_VERSION);

        simpleOpenFramingHeaderEncoder.encodingType(CandidateTermDecoder.SCHEMA_ID);

        candidateTermEncoder.wrapAndApplyHeader(
            buffer, firstFramingHeaderOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderEncoder);
        candidateTermDecoder.wrapAndApplyHeader(
            buffer, firstFramingHeaderOffset + simpleOpenFramingHeaderEncoder.sbeBlockLength(), messageHeaderDecoder);

        simpleOpenFramingHeaderEncoder.messageLength(
            SimpleOpenFramingHeaderEncoder.BLOCK_LENGTH +
            MessageHeaderEncoder.ENCODED_LENGTH +
            candidateTermEncoder.encodedLength());
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

    private void syncFile(final MappedByteBuffer mappedFile)
    {
        if (0 < fileSyncLevel)
        {
            mappedFile.force();
        }
    }
}
