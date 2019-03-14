/*
 * Copyright 2014-2019 Real Logic Ltd.
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
#ifndef AERON_ARCHIVE_CONFIGURATION_H
#define AERON_ARCHIVE_CONFIGURATION_H

#include <cstdint>

#include "Aeron.h"
#include "ChannelUri.h"
#include "util/MacroUtil.h"

namespace aeron {
namespace archive {
namespace client {

constexpr const std::int64_t NULL_TIMESTAMP = aeron::NULL_VALUE;
constexpr const std::int64_t NULL_POSITION = aeron::NULL_VALUE;
constexpr const std::int64_t NULL_LENGTH = aeron::NULL_VALUE;

namespace Configuration
{
constexpr const std::uint8_t ARCHIVE_MAJOR_VERSION = 0;
constexpr const std::uint8_t ARCHIVE_MINOR_VERSION = 1;
constexpr const std::uint8_t ARCHIVE_PATCH_VERSION = 1;
constexpr const std::int32_t ARCHIVE_SEMANTIC_VERSION = aeron::util::semanticVersionCompose(
    ARCHIVE_MAJOR_VERSION, ARCHIVE_MINOR_VERSION, ARCHIVE_PATCH_VERSION);

constexpr const long long MESSAGE_TIMEOUT_NS_DEFAULT = 5 * 1000 * 1000 * 1000L;

constexpr const char CONTROL_REQUEST_CHANNEL_DEFAULT[] = "aeron:udp?endpoint=localhost:8010";
constexpr const std::int32_t CONTROL_REQUEST_STREAM_ID_DEFAULT = 10;

constexpr const char LOCAL_CONTROL_REQUEST_CHANNEL_DEFAULT[] = "aeron:ipc";
constexpr const std::int32_t LOCAL_CONTTROL_REQUEST_STREAM_ID_DEFAULT = 11;

constexpr const char CONTROL_RESPONSE_CHANNEL_DEFAULT[] = "aeron:udp?endpoint=localhost:8020";
constexpr const std::int32_t CONTROL_RESPONSE_STREAM_ID_DEFAULT = 20;

constexpr const char RECORDING_EVENTS_CHANNEL_DEFAULT[] = "aeron:udp?endpoint=localhost:8030";
constexpr const std::int32_t RECORDING_EVENTS_STREAM_ID_DEFAULT = 30;

constexpr const bool CONTROL_TERM_BUFFER_SPARSE_DEFAULT = true;
constexpr const std::int32_t CONTROL_TERM_BUFFER_LENGTH_DEFAULT = 64 * 1024;
constexpr const std::int32_t CONTROL_MTU_LENGTH_DEFAULT = 1408;

}

class Context
{
public:
    using this_t = Context;

    void conclude()
    {
        if (!m_aeron)
        {
            aeron::Context ctx;

            ctx.aeronDir(m_aeronDirectoryName);
            m_aeron = Aeron::connect(ctx);
            m_ownsAeronClient = true;
        }

        std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(m_controlRequestChannel);
        channelUri->put(TERM_LENGTH_PARAM_NAME, std::to_string(m_controlTermBufferLength));
        channelUri->put(MTU_LENGTH_PARAM_NAME, std::to_string(m_controlMtuLength));
        channelUri->put(SPARSE_PARAM_NAME, (m_controlTermBufferSparse ? "true" : "false"));
        m_controlRequestChannel = channelUri->toString();
    }

    inline std::shared_ptr<Aeron> aeron()
    {
        return m_aeron;
    }

    inline this_t& aeron(std::shared_ptr<Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        return *this;
    }

    inline long long messageTimeoutNs()
    {
        return m_messageTimeoutNs;
    }

    inline this_t& messageTimeoutNs(long long timeoutNs)
    {
        m_messageTimeoutNs = timeoutNs;
        return *this;
    }

    inline std::string recordingEventsChannel()
    {
        return m_recordingEventsChannel;
    }

    inline this_t& recordingEventsChannel(const std::string& recordingEventsChannel)
    {
        m_recordingEventsChannel = recordingEventsChannel;
        return *this;
    }

    inline std::int32_t recordingEventsStreamId()
    {
        return m_recordingEventsStreamId;
    }

    inline this_t& recordingEventsStreamId(std::int32_t recordingEventsStreamId)
    {
        m_recordingEventsStreamId = recordingEventsStreamId;
        return *this;
    }

    inline std::string controlResponseChannel()
    {
        return m_controlResponseChannel;
    }

    inline this_t& controlResponseChannel(const std::string& channel)
    {
        m_controlResponseChannel = channel;
        return *this;
    }

    inline std::int32_t controlResponseStreamId()
    {
        return m_controlResponseStreamId;
    }

    inline this_t& controlResponseStreamId(std::int32_t streamId)
    {
        m_controlResponseStreamId = streamId;
        return *this;
    }

    inline std::string controlRequestChannel()
    {
        return m_controlRequestChannel;
    }

    inline this_t& controlRequestChannel(const std::string& channel)
    {
        m_controlRequestChannel = channel;
        return *this;
    }

    inline std::int32_t controlRequestStreamId()
    {
        return m_controlRequestStreamId;
    }

    inline this_t& controlRequestStreamId(std::int32_t streamId)
    {
        m_controlRequestStreamId = streamId;
        return *this;
    }

    inline bool controlTermBufferSparse()
    {
        return m_controlTermBufferSparse;
    }

    inline this_t& controlTermBufferSparse(bool controlTermBufferSparse)
    {
        m_controlTermBufferSparse = controlTermBufferSparse;
        return *this;
    }

    inline std::int32_t controlTermBufferLength()
    {
        return m_controlTermBufferLength;
    }

    inline this_t& controlTermBufferLength(std::int32_t controlTermBufferLength)
    {
        m_controlTermBufferLength = controlTermBufferLength;
        return *this;
    }

    inline std::int32_t controlMtuLength()
    {
        return m_controlMtuLength;
    }

    inline this_t& controlMtuLength(std::int32_t controlMtuLength)
    {
        m_controlMtuLength = controlMtuLength;
        return *this;
    }

    inline std::string aeronDirectoryName()
    {
        return m_aeronDirectoryName;
    }

    inline this_t& aeronDirectoryName(const std::string& aeronDirectoryName)
    {
        m_aeronDirectoryName = aeronDirectoryName;
        return *this;
    }

    inline bool ownsAeronClient()
    {
        return m_ownsAeronClient;
    }

    inline this_t& ownsAeronClient(bool ownsAeronClient)
    {
        m_ownsAeronClient = ownsAeronClient;
        return *this;
    }

    inline exception_handler_t errorHandler()
    {
        return m_errorHandler;
    }

    inline this_t& errorHandler(const exception_handler_t& errorHandler)
    {
        m_errorHandler = errorHandler;
        return *this;
    }

private:
    std::shared_ptr<Aeron> m_aeron;
    std::string m_aeronDirectoryName = aeron::Context::defaultAeronPath();
    long long m_messageTimeoutNs = Configuration::MESSAGE_TIMEOUT_NS_DEFAULT;

    std::string m_controlResponseChannel = Configuration::CONTROL_RESPONSE_CHANNEL_DEFAULT;
    std::int32_t m_controlResponseStreamId = Configuration::CONTROL_RESPONSE_STREAM_ID_DEFAULT;

    std::string m_controlRequestChannel = Configuration::CONTROL_REQUEST_CHANNEL_DEFAULT;
    std::int32_t m_controlRequestStreamId = Configuration::CONTROL_REQUEST_STREAM_ID_DEFAULT;

    std::string m_recordingEventsChannel = Configuration::RECORDING_EVENTS_CHANNEL_DEFAULT;
    std::int32_t m_recordingEventsStreamId = Configuration::RECORDING_EVENTS_STREAM_ID_DEFAULT;

    bool m_controlTermBufferSparse = Configuration::CONTROL_TERM_BUFFER_SPARSE_DEFAULT;
    std::int32_t m_controlTermBufferLength = Configuration::CONTROL_TERM_BUFFER_LENGTH_DEFAULT;
    std::int32_t m_controlMtuLength = Configuration::CONTROL_MTU_LENGTH_DEFAULT;

    bool m_ownsAeronClient = false;

    exception_handler_t m_errorHandler = nullptr;
};

}}}
#endif //AERON_ARCHIVE_CONFIGURATION_H
