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
import org.agrona.IoUtil;
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
 * </p>
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
    private final File archiveDir;
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

    /**
     * Construct the NodeStateFile.
     *
     * @param archiveDir    directory containing the NodeStateFile.
     * @param createNew     whether a new file should be created if one does not already exist.
     * @param fileSyncLevel whether the mapped byte buffer should be synchronised with the underlying filesystem on
     *                      change.
     * @throws IOException if there is an error creating the file or <code>createNew == false</code> and the file does
     * not already exist.
     */
    public NodeStateFile(final File archiveDir, final boolean createNew, final int fileSyncLevel) throws IOException
    {
        this.archiveDir = archiveDir;
        this.fileSyncLevel = fileSyncLevel;
        final File nodeStateFile = new File(archiveDir, NodeStateFile.FILENAME);
        if (!nodeStateFile.exists())
        {
            if (!createNew)
            {
                throw new IOException("NodeStateFile does not exist and createNew=false");
            }

            this.mappedFile = IoUtil.mapNewFile(nodeStateFile, MINIMUM_FILE_LENGTH);
            this.buffer = new UnsafeBuffer(mappedFile, 0, mappedFile.capacity());
            setInitialState();
            syncFile();
        }
        else
        {
            this.mappedFile = IoUtil.mapExistingFile(nodeStateFile, "NodeState");
            this.buffer = new UnsafeBuffer(mappedFile, 0, mappedFile.capacity());
            loadInitialState();
        }
    }

    private void loadInitialState()
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
            nodeStateHeaderEncoder.sbeBlockLength(), CandidateTermDecoder.TEMPLATE_ID);
        if (Aeron.NULL_VALUE == nodeStateOffset)
        {
            throw new IllegalStateException("failed to find CandidateTerm entry");
        }

        candidateTermDecoder.wrapAndApplyHeader(buffer, nodeStateOffset, messageHeaderDecoder);
        candidateTermEncoder.wrap(buffer, nodeStateOffset);
    }

    private int scanForMessageTypeOffset(final int startPosition, final int templateId)
    {
        int position = startPosition;

        while (position < buffer.capacity())
        {
            simpleOpenFramingHeaderDecoder.wrap(
                buffer,
                position,
                SimpleOpenFramingHeaderDecoder.BLOCK_LENGTH,
                SimpleOpenFramingHeaderDecoder.SCHEMA_VERSION);

            final long messageLength = simpleOpenFramingHeaderDecoder.messageLength();
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

    private void setInitialState()
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

        updateCandidateTermId(Aeron.NULL_VALUE, Aeron.NULL_VALUE, Aeron.NULL_VALUE);

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
            .candidateTermId(candidateTermId)
            .logPosition(logPosition)
            .timestamp(timestampMs);
        syncFile();
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
            return candidateTermDecoder.candidateTermId();
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

    private void syncFile()
    {
        if (0 < fileSyncLevel)
        {
            mappedFile.force();
        }
    }
}
