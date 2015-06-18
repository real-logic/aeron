/*
 * Copyright 2015 Real Logic Ltd.
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

#ifndef AERON_DATAFRAMEHEADER_H
#define AERON_DATAFRAMEHEADER_H

#include <stddef.h>

namespace aeron { namespace concurrent { namespace logbuffer {

namespace DataFrameHeader {

#pragma pack(push)
#pragma pack(4)
struct DataFrameHeaderDefn
{
    std::int32_t frameLength;
    std::int8_t version;
    std::int8_t flags;
    std::int16_t type;
    std::int32_t termOffset;
    std::int32_t sessionId;
    std::int32_t streamId;
    std::int32_t termId;
};
#pragma pack(pop)

static const util::index_t FRAME_LENGTH_OFFSET = offsetof(DataFrameHeaderDefn, frameLength);
static const util::index_t VERSION_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, version);
static const util::index_t FLAGS_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, flags);
static const util::index_t TYPE_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, type);
static const util::index_t TERM_OFFSET_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, termOffset);
static const util::index_t SESSION_ID_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, sessionId);
static const util::index_t STREAM_ID_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, streamId);
static const util::index_t TERM_ID_FIELD_OFFSET = offsetof(DataFrameHeaderDefn, termId);
static const util::index_t DATA_OFFSET = sizeof(DataFrameHeaderDefn);

static const util::index_t LENGTH = DATA_OFFSET;

static const std::int16_t HDR_TYPE_PAD = 0x00;
static const std::int16_t HDR_TYPE_DATA = 0x01;

}

}}}

#endif //AERON_DATAFRAMEHEADER_H
