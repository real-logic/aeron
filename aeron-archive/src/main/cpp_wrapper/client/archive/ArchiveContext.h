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
#ifndef AERON_ARCHIVE_WRAPPER_CONTEXT_H
#define AERON_ARCHIVE_WRAPPER_CONTEXT_H

#include "AeronArchive.h"
#include "CredentialsSupplier.h"
#include "concurrent/YieldingIdleStrategy.h"

#include "Context.h"

namespace aeron { namespace archive { namespace client
{

struct RecordingSignal
{
    enum Value
    {
        START = INT32_C(0),
        STOP = INT32_C(1),
        EXTEND = INT32_C(2),
        REPLICATE = INT32_C(3),
        MERGE = INT32_C(4),
        SYNC = INT32_C(5),
        DELETE = INT32_C(6),
        REPLICATE_END = INT32_C(7),
        NULL_VALUE = INT32_MIN
    };

    RecordingSignal(
        std::int64_t controlSessionId,
        std::int64_t recordingId,
        std::int64_t subscriptionId,
        std::int64_t position,
        std::int32_t recordingSignalCode) :
        m_controlSessionId(controlSessionId),
        m_recordingId(recordingId),
        m_subscriptionId(subscriptionId),
        m_position(position),
        m_recordingSignalCode(recordingSignalCode)
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_recordingId;
    std::int64_t m_subscriptionId;
    std::int64_t m_position;
    std::int32_t m_recordingSignalCode;
};

typedef  std::function<void(RecordingSignal &recordingSignal)> recording_signal_consumer_t;

class Context
{
    friend class AeronArchive;

public:
    Context()
    {
        if (aeron_archive_context_init(&m_aeron_archive_ctx_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        setupContext();
    }

    ~Context()
    {
        aeron_archive_context_close(m_aeron_archive_ctx_t);
        m_aeron_archive_ctx_t = nullptr;
    }

    inline std::string controlRequestChannel() const
    {
        return aeron_archive_context_get_control_request_channel(m_aeron_archive_ctx_t);
    }

    inline Context &controlRequestChannel(const std::string &channel)
    {
        aeron_archive_context_set_control_request_channel(m_aeron_archive_ctx_t, channel.c_str());
        return *this;
    }

    /*
    inline on_recording_signal_t recordingSignalConsumer() const
    {
        return m_onRecordingSignal;
    }
     */

    inline Context &recordingSignalConsumer(const recording_signal_consumer_t &consumer)
    {
        m_recordingSignalConsumer = consumer;
        return *this;
    }

    std::string aeronDirectoryName()
    {
        return { aeron_archive_context_get_aeron_directory_name(m_aeron_archive_ctx_t) };
    }

    std::shared_ptr<Aeron> aeron()
    {
        return m_aeronW;
    }

    void setAeron(std::shared_ptr<Aeron> aeron)
    {
        if (aeron_archive_context_set_aeron(m_aeron_archive_ctx_t,aeron->aeron()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_aeronW = std::move(aeron);
    }

    void credentialsSupplier(const CredentialsSupplier &credentialsSupplier)
    {
        m_credentialsSupplier.m_encodedCredentials = credentialsSupplier.m_encodedCredentials;
        m_credentialsSupplier.m_onChallenge = credentialsSupplier.m_onChallenge;
        m_credentialsSupplier.m_onFree = credentialsSupplier.m_onFree;
    }

    template<typename IdleStrategy>
    void idleStrategy(IdleStrategy &idleStrategy)
    {
        m_idleFunc = [&idleStrategy](int work_count){ idleStrategy.idle(work_count); };
    }

    inline std::int32_t controlRequestStreamId() const
    {
        return aeron_archive_context_get_control_request_stream_id(m_aeron_archive_ctx_t);
    }

private:
    aeron_archive_context_t *m_aeron_archive_ctx_t = nullptr; // backing C struct

    CredentialsSupplier m_credentialsSupplier;
    aeron_archive_encoded_credentials_t m_lastEncodedCredentials = {};

    std::function<void(int work_count)> m_idleFunc;
    YieldingIdleStrategy m_defaultIdleStrategy;

    std::shared_ptr<Aeron> m_aeronW = nullptr;

    recording_signal_consumer_t m_recordingSignalConsumer = nullptr;

    // This Context is created after connect succeeds and wraps around a context_t created in the C layer
    explicit Context(
        aeron_archive_context_t *archive_context) :
        m_aeron_archive_ctx_t(archive_context)
    {
        setupContext();
    }

    void setupContext()
    {
        if (aeron_archive_context_set_credentials_supplier(
            m_aeron_archive_ctx_t,
            encodedCredentialsFunc,
            onChallengeFunc,
            onFreeFunc,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_idle_strategy(
            m_aeron_archive_ctx_t,
            idle,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_recording_signal_consumer(
            m_aeron_archive_ctx_t,
            recording_signal_consumer_func,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        this->idleStrategy(m_defaultIdleStrategy);
    }

    static aeron_archive_encoded_credentials_t *encodedCredentialsFunc(void *clientd)
    {
        auto ctx = (Context *)clientd;

        auto credentials = ctx->m_credentialsSupplier.m_encodedCredentials();

        ctx->m_lastEncodedCredentials = { credentials.first, credentials.second };

        return &ctx->m_lastEncodedCredentials;
    }

    static aeron_archive_encoded_credentials_t *onChallengeFunc(aeron_archive_encoded_credentials_t *encodedChallenge, void *clientd)
    {
        auto ctx = (Context *)clientd;

        auto credentials = ctx->m_credentialsSupplier.m_onChallenge({ encodedChallenge->data, encodedChallenge->length});

        ctx->m_lastEncodedCredentials = { credentials.first, credentials.second };

        return &ctx->m_lastEncodedCredentials;
    }

    static void onFreeFunc(aeron_archive_encoded_credentials_t *credentials, void *clientd)
    {
        auto ctx = (Context *)clientd;

        ctx->m_credentialsSupplier.m_onFree({ credentials->data, credentials->length});
    }

    static void idle(void *clientd, int work_count)
    {
        auto ctx = (Context *)clientd;

        ctx->m_idleFunc(work_count);
    }

    static void recording_signal_consumer_func(
        aeron_archive_recording_signal_t *recording_signal,
        void *clientd)
    {
        auto ctx = (Context *)clientd;

        if (nullptr != ctx->m_recordingSignalConsumer)
        {
            RecordingSignal signal(
                recording_signal->control_session_id,
                recording_signal->recording_id,
                recording_signal->subscription_id,
                recording_signal->position,
                recording_signal->recording_signal_code);

            ctx->m_recordingSignalConsumer(signal);
        }
    }

};

}}}

#endif //AERON_ARCHIVE_WRAPPER_CONTEXT_H
