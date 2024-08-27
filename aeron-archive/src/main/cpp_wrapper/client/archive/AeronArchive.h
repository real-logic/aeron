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
#ifndef AERON_ARCHIVE_WRAPPER_H
#define AERON_ARCHIVE_WRAPPER_H

#include "client/aeron_archive.h"

#include "Aeron.h"
#include "client/util/ArchiveExceptions.h"

#include "ArchiveContext.h"

namespace aeron { namespace archive { namespace client
{

using namespace aeron::util;

class AeronArchive
{

public:
    using Context_t = aeron::archive::client::Context;

    class AsyncConnect
    {
        friend class AeronArchive;

    public:
        std::shared_ptr<AeronArchive> poll()
        {
            aeron_archive_t *aeron_archive = nullptr;

            if (aeron_archive_async_connect_poll(&aeron_archive, m_async) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            if (nullptr == aeron_archive)
            {
                return {};
            }

            m_async = nullptr; // _poll() just free'd this up

            return std::shared_ptr<AeronArchive>(new AeronArchive(aeron_archive));
        }

    private:
        explicit AsyncConnect(const Context_t &ctx)
        {
            if (aeron_archive_async_connect(&m_async, ctx.m_ctx) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        aeron_archive_async_connect_t *m_async = nullptr;
    };

    static std::shared_ptr<AsyncConnect> asyncConnect(const Context &ctx)
    {
        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    static std::shared_ptr<AeronArchive> connect(const Context &ctx)
    {
        aeron_archive_t *aeron_archive = nullptr;

        if (aeron_archive_connect(&aeron_archive, ctx.m_ctx) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::shared_ptr<AeronArchive>(new AeronArchive(aeron_archive));
    }

    ~AeronArchive()
    {
        aeron_archive_close(m_aeron_archive);
    }

    std::shared_ptr<Subscription> controlResponseSubscription()
    {
        if (nullptr == m_controlResponseSubscription)
        {
            m_controlResponseSubscription = std::make_shared<Subscription>(
                aeron_archive_get_aeron(m_aeron_archive),
                aeron_archive_get_control_response_subscription(m_aeron_archive),
                nullptr);
        }

        return m_controlResponseSubscription;
    }

    std::int64_t archiveId()
    {
        return aeron_archive_get_archive_id(m_aeron_archive);
    }

private:
    explicit AeronArchive(
        aeron_archive_t *aeron_archive) :
        m_aeron_archive(aeron_archive)
    {
    }

    aeron_archive_t *m_aeron_archive = nullptr;

    std::shared_ptr<Subscription> m_controlResponseSubscription = nullptr;
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_H
