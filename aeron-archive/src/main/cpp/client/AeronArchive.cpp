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

#include "AeronArchive.h"
#include "ArchiveException.h"

using namespace aeron::archive::client;

AeronArchive::AsyncConnect::AsyncConnect(
    Context_t& context,
    std::shared_ptr<Aeron> aeron,
    std::int64_t subscriptionId,
    std::int64_t publicationId) :
    m_ctx(std::unique_ptr<Context_t>(new Context_t(context))),
    m_aeron(std::move(aeron)),
    m_subscriptionId(subscriptionId),
    m_publicationId(publicationId)
{
}

std::shared_ptr<AeronArchive> AeronArchive::AsyncConnect::poll()
{
    if (!m_subscription)
    {
        m_subscription = m_aeron->findSubscription(m_subscriptionId);
    }

    if (!m_controlResponsePoller && m_subscription)
    {
        m_controlResponsePoller = std::unique_ptr<ControlResponsePoller>(new ControlResponsePoller(m_subscription));
    }

    if (!m_publication)
    {
        m_publication = m_aeron->findExclusivePublication(m_publicationId);
    }

    if (!m_archiveProxy && m_publication)
    {
        m_archiveProxy = std::unique_ptr<ArchiveProxy>(new ArchiveProxy(
            m_publication, aeron::systemNanoClock, m_ctx->messageTimeoutNs()));
    }

    if (0 == m_step && m_archiveProxy)
    {
        if (m_archiveProxy->publication()->isConnected())
        {
            return std::shared_ptr<AeronArchive>();
        }

        m_step = 1;
    }

    if (1 == m_step)
    {
        m_connectCorrelationId = m_aeron->nextCorrelationId();

        m_step = 2;
    }

    if (2 == m_step)
    {
        if (!m_archiveProxy->tryConnect(
            m_ctx->controlResponseChannel(), m_ctx->controlResponseStreamId(), m_connectCorrelationId))
        {
            return std::shared_ptr<AeronArchive>();
        }

        m_step = 3;
    }

    if (3 == m_step && m_controlResponsePoller)
    {
        if (!m_controlResponsePoller->subscription()->isConnected())
        {
            return std::shared_ptr<AeronArchive>();
        }

        m_step = 4;
    }

    if (m_controlResponsePoller)
    {
        m_controlResponsePoller->poll();

        if (m_controlResponsePoller->isPollComplete() &&
            m_controlResponsePoller->correlationId() == m_connectCorrelationId &&
            m_controlResponsePoller->isControlResponse())
        {
            if (!m_controlResponsePoller->isCodeOk())
            {
                if (m_controlResponsePoller->isCodeError())
                {
                    throw ArchiveException(
                        static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                        "error: " + m_controlResponsePoller->errorMessage(),
                        SOURCEINFO);
                }

                throw ArchiveException(
                    "unexpected response: code=" + std::to_string(m_controlResponsePoller->codeValue()), SOURCEINFO);
            }

            //const std::int64_t sessionId = m_controlResponsePoller->controlSessionId();

            // TODO: finish by creating AeronArchive and returning shared_ptr to it.
        }
    }

    return std::shared_ptr<AeronArchive>();
}

std::shared_ptr<AeronArchive::AsyncConnect> AeronArchive::asyncConnect(AeronArchive::Context_t &ctx)
{
    ctx.conclude();

    std::shared_ptr<Aeron> aeron = ctx.aeron();

    const std::int64_t subscriptionId = aeron->addSubscription(
        ctx.controlResponseChannel(), ctx.controlResponseStreamId());

    const std::int64_t publicationId = aeron->addExclusivePublication(
        ctx.controlRequestChannel(), ctx.controlRequestStreamId());

    return std::make_shared<AeronArchive::AsyncConnect>(ctx, aeron, subscriptionId, publicationId);
}