/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/DataFrameHeader.h>
#include <concurrent/logbuffer/FrameDescriptor.h>

#include "ReceiveChannelEndpoint.h"

using namespace aeron::driver::media;

std::int32_t ReceiveChannelEndpoint::pollForData()
{
    std::int32_t bytesReceived = 0;
    std::int32_t bytesRead = 0;

    InetAddress* srcAddress = receive(&bytesReceived);

    if (nullptr != srcAddress)
    {
        if (isValidFrame(receiveBuffer(), bytesRead))
        {
            bytesReceived = dispatch(receiveBuffer(), bytesRead, *srcAddress);
        }
    }

    return bytesReceived;
}

std::int32_t ReceiveChannelEndpoint::dispatch(
    concurrent::AtomicBuffer &buffer, std::int32_t length, InetAddress& address)
{
    std::int32_t bytesReceived = 0;
    switch (concurrent::logbuffer::FrameDescriptor::frameType(buffer, 0))
    {
        case concurrent::logbuffer::DataFrameHeader::HDR_TYPE_PAD:
        case concurrent::logbuffer::DataFrameHeader::HDR_TYPE_DATA:
            break;
            // dispatch
        case concurrent::logbuffer::DataFrameHeader::HDR_TYPE_SETUP:
            break;
    }

    return bytesReceived;
}
