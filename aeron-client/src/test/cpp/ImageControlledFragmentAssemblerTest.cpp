/*
 * Copyright 2014-2025 Real Logic Limited.
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

#include "concurrent/logbuffer/FrameDescriptor.h"
#include "ImageControlledFragmentAssemblerTestFixture.h"

extern "C"
{
#include "aeron_image.h"
}

void initHeader(std::uint8_t *buffer, size_t length, Header **header)
{
    *header = new Header{INITIAL_TERM_ID, POSITION_BITS_TO_SHIFT, (void*)"test context"};

    AtomicBuffer temp{buffer, length};
    (*header)->buffer(temp);
}

void freeHeader(Header *header)
{
    delete header;
}

void fillHeader(Header &header, std::int32_t termOffset, std::uint8_t flags, std::int32_t payloadLength)
{
    auto &frame(header.buffer().overlayStruct<DataFrameHeader::DataFrameHeaderDefn>(0));

    frame.frameLength = DataFrameHeader::LENGTH + payloadLength;
    frame.version = DataFrameHeader::CURRENT_VERSION;
    frame.flags = flags;
    frame.type = DataFrameHeader::HDR_TYPE_DATA;
    frame.termOffset = termOffset;
    frame.sessionId = SESSION_ID;
    frame.streamId = STREAM_ID;
    frame.termId = ACTIVE_TERM_ID;
}

INSTANTIATE_TEST_SUITE_P(
    ImageControlledFragmentAssemblerParameterisedTest,
    ImageControlledFragmentAssemblerParameterisedTest,
    testing::Values(std::make_tuple(initHeader, freeHeader, fillHeader)));