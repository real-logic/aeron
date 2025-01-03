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

#ifndef AERON_CONCURRENT_DATA_FRAME_HEADER_H
#define AERON_CONCURRENT_DATA_FRAME_HEADER_H

#include <cstddef>

#include "util/Index.h"
#include "aeronc.h"

namespace aeron { namespace concurrent { namespace logbuffer {

namespace DataFrameHeader {

static constexpr util::index_t FRAME_LENGTH_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, frame_length);
static constexpr util::index_t VERSION_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, version);
static constexpr util::index_t FLAGS_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, flags);
static constexpr util::index_t TYPE_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, type);
static constexpr util::index_t TERM_OFFSET_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, term_offset);
static constexpr util::index_t SESSION_ID_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, session_id);
static constexpr util::index_t STREAM_ID_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, stream_id);
static constexpr util::index_t TERM_ID_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, term_id);
static constexpr util::index_t RESERVED_VALUE_FIELD_OFFSET = offsetof(aeron_header_values_frame_t, reserved_value);
static constexpr util::index_t DATA_OFFSET = sizeof(aeron_header_values_frame_t);

static constexpr util::index_t LENGTH = DATA_OFFSET;

static constexpr std::uint16_t HDR_TYPE_PAD = 0x00;
static constexpr std::uint16_t HDR_TYPE_DATA = 0x01;
static constexpr std::uint16_t HDR_TYPE_NAK = 0x02;
static constexpr std::uint16_t HDR_TYPE_SM = 0x03;
static constexpr std::uint16_t HDR_TYPE_ERR = 0x04;
static constexpr std::uint16_t HDR_TYPE_SETUP = 0x05;
static constexpr std::uint16_t HDR_TYPE_EXT = 0xFFFF;

static constexpr std::int8_t CURRENT_VERSION = 0x0;

}

}}}

#endif
