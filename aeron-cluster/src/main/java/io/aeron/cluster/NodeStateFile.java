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

import java.io.File;
import java.io.IOException;

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
public class NodeStateFile
{
    public static final String FILENAME = "node-state.dat";

    private File archiveDir;

    public NodeStateFile(final File archiveDir, final boolean createNew) throws IOException
    {
        this.archiveDir = archiveDir;
        final File nodeStateFile = new File(archiveDir, NodeStateFile.FILENAME);
        if (!createNew && !nodeStateFile.exists())
        {
            throw new IOException("NodeStateFile does not existing and createNew=false");
        }
    }
}
