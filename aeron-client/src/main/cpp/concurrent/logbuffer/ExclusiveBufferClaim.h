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

#ifndef AERON_EXCLUSIVEBUFFERCLAIM_H
#define AERON_EXCLUSIVEBUFFERCLAIM_H

#include <util/Index.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <concurrent/logbuffer/DataFrameHeader.h>

namespace aeron { namespace concurrent { namespace logbuffer {

/**
 * Represents a claimed range in a buffer to be used for recording a message without copy semantics for later commit.
 * <p>
 * The claimed space is in {@link #buffer()} between {@link #offset()} and {@link #offset()} + {@link #length()}.
 * When the buffer is filled with message data, use {@link #commit()} to make it available to subscribers.
 * @deprecated use BufferClaim instead.
 */
class ExclusiveBufferClaim : public BufferClaim
{
public:
    typedef ExclusiveBufferClaim this_t;


};

}}}

#endif
