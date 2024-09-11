/*
 * Copyright 2014-2024 Real Logic Limited.
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
#ifndef AERON_ARCHIVE_WRAPPER_REPLAY_PARAMS_H
#define AERON_ARCHIVE_WRAPPER_REPLAY_PARAMS_H

#include "AeronArchive.h"

namespace aeron { namespace archive { namespace client
{

class ReplayParams
{
public:
    friend class AeronArchive;

    ReplayParams()
    {
        aeron_archive_replay_params_init(&m_params);
    }

    std::int32_t boundingLimitCounterId() const
    {
        return m_params.bounding_limit_counter_id;
    }

    ReplayParams &boundingLimitCounterId(std::int32_t boundingLimitCounterId)
    {
        m_params.bounding_limit_counter_id = boundingLimitCounterId;
        return *this;
    }

    std::int32_t fileIoMaxLength() const
    {
        return m_params.file_io_max_length;
    }

    ReplayParams &fileIoMaxLength(std::int32_t fileIoMaxLength)
    {
        m_params.file_io_max_length = fileIoMaxLength;
        return *this;
    }

    std::int64_t position() const
    {
        return m_params.position;
    }

    ReplayParams &position(std::int64_t position)
    {
        m_params.position = position;
        return *this;
    }

    std::int64_t length() const
    {
        return m_params.length;
    }

    ReplayParams &length(std::int64_t length)
    {
        m_params.length = length;
        return *this;
    }

    bool isBounded() const
    {
        return NULL_VALUE != boundingLimitCounterId();
    }

    ReplayParams &replayToken(std::int64_t replayToken)
    {
        m_params.replay_token = replayToken;
        return *this;
    }

    std::int64_t replayToken() const
    {
        return m_params.replay_token;
    }

    ReplayParams &subscriptionRegistrationId(std::int64_t registrationId)
    {
        m_params.subscription_registration_id = registrationId;
        return *this;
    }

    std::int64_t subscriptionRegistrationId() const
    {
        return m_params.subscription_registration_id;
    }

private:
    aeron_archive_replay_params_t m_params;

};

}}}

#endif //AERON_ARCHIVE_WRAPPER_REPLAY_PARAMS_H
