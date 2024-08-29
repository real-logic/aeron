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

class Context
{
    friend class AeronArchive;

public:
    Context()
    {
        if (aeron_archive_context_init(&m_ctx) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        setupContext();
    }

    ~Context()
    {
        aeron_archive_context_close(m_ctx);
        m_ctx = nullptr;
    }

    std::string aeronDirectoryName()
    {
        return { aeron_archive_context_get_aeron_directory_name(m_ctx) };
    }

    void setAeronContext(const aeron::Context &ctx)
    {
        // TODO should probably hang on to the wrapper context as well

        if (aeron_archive_context_set_aeron_context(m_ctx, ctx.context()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
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

    std::shared_ptr<Aeron> aeron()
    {
        return m_aeron;
    }

    void setAeron(std::shared_ptr<Aeron> aeron)
    {
        m_aeron = std::move(aeron);
    }

private:
    aeron_archive_context_t *m_ctx = nullptr; // backing C struct

    CredentialsSupplier m_credentialsSupplier;
    aeron_archive_encoded_credentials_t m_lastEncodedCredentials = {};

    std::function<void(int work_count)> m_idleFunc;
    YieldingIdleStrategy m_defaultIdleStrategy;

    std::shared_ptr<Aeron> m_aeron = nullptr;

    // This Context is created after connect succeeds and wraps around a context_t created in the C layer
    explicit Context(
        aeron_archive_context_t *archive_context) :
        m_ctx(archive_context)
    {
        setupContext();
    }

    void setupContext()
    {
        if (aeron_archive_context_set_credentials_supplier(
            m_ctx,
            encodedCredentialsFunc,
            onChallengeFunc,
            onFreeFunc,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_idle_strategy(
            m_ctx,
            idle,
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

};

}}}

#endif //AERON_ARCHIVE_WRAPPER_CONTEXT_H
