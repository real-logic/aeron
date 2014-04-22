/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.mediadriver.buffer;

import uk.co.real_logic.aeron.mediadriver.UdpDestination;

import java.nio.ByteBuffer;

/**
 * Interface for encapsulating the strategy of allocating ByteBuffers for Session, Channel, and Term
 */
public interface BufferManagementStrategy
{
    BufferRotator addSenderTerm(final UdpDestination destination,
                                final long sessionId,
                                final long channelId) throws Exception;

    // TODO: figure out if we need to actually remove an individual term
    /*void removeSenderTerm(final UdpDestination destination,
                          final long sessionId,
                          final long channelId,
                          final long termId);*/

    void removeSenderChannel(final UdpDestination destination, final long sessionId, final long channelId);

    ByteBuffer addReceiverTerm(final UdpDestination destination,
                               final long sessionId,
                               final long channelId,
                               final long termId) throws Exception;

    ByteBuffer lookupReceiverTerm(final UdpDestination destination,
                                  final long sessionId,
                                  final long channelId,
                                  final long termId);

    int countChannels(final long sessionId);

    int countSessions(final UdpDestination destination);

    int countChannels(final UdpDestination destination, final long sessionId);

    int countTerms(final UdpDestination destination, final long sessionId, final long channelId);

}
