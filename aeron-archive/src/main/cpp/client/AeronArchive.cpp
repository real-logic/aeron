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
        m_archiveProxy = std::unique_ptr<ArchiveProxy>(new ArchiveProxy(m_publication));
    }

    if (0 == m_step && m_archiveProxy)
    {
        if (!m_archiveProxy->publication()->isConnected())
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

            const std::int64_t sessionId = m_controlResponsePoller->controlSessionId();

            return std::make_shared<AeronArchive>(
                std::move(m_ctx),
                std::move(m_archiveProxy),
                std::move(m_controlResponsePoller),
                m_aeron,
                sessionId);
        }
    }

    return std::shared_ptr<AeronArchive>();
}

AeronArchive::AeronArchive(
    std::unique_ptr<Context_t> ctx,
    std::unique_ptr<ArchiveProxy> archiveProxy,
    std::unique_ptr<ControlResponsePoller> controlResponsePoller,
    std::shared_ptr<Aeron> aeron,
    std::int64_t controlSessionId)
    :
    m_ctx(std::move(ctx)),
    m_archiveProxy(std::move(archiveProxy)),
    m_controlResponsePoller(std::move(controlResponsePoller)),
    m_aeron(std::move(aeron)),
    m_nanoClock(systemNanoClock),
    m_controlSessionId(controlSessionId),
    m_messageTimeoutNs(m_ctx->messageTimeoutNs())
{
}

AeronArchive::~AeronArchive()
{
    m_archiveProxy->closeSession(m_controlSessionId);
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

template<typename IdleStrategy>
void AeronArchive::pollNextResponse(std::int64_t correlationId, long long deadlineNs, ControlResponsePoller& poller)
{
    IdleStrategy idle;

    while (true)
    {
        const int fragments = poller.poll();

        if (poller.isPollComplete())
        {
            break;
        }

        if (fragments > 0)
        {
            continue;
        }

        if (!poller.subscription()->isConnected())
        {
            throw ArchiveException("subscription to archive is not connected", SOURCEINFO);
        }

        checkDeadline(deadlineNs, "awaiting response", correlationId);
        idle.idle();
        invokeAeronClient();
    }
}

template<typename IdleStrategy>
std::int64_t AeronArchive::pollForResponse(std::int64_t correlationId)
{
    const long long deadlineNs = m_nanoClock() + m_messageTimeoutNs;

    while (true)
    {
        pollNextResponse<IdleStrategy>(correlationId, deadlineNs, *m_controlResponsePoller);

        if (m_controlResponsePoller->controlSessionId() != controlSessionId() ||
            !m_controlResponsePoller->isControlResponse())
        {
            invokeAeronClient();
            continue;
        }

        if (m_controlResponsePoller->isCodeError())
        {
            if (m_controlResponsePoller->correlationId() == correlationId)
            {
                throw ArchiveException(
                    static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                    "response for correlationId=" + std::to_string(correlationId)
                    + ", error: " + m_controlResponsePoller->errorMessage(),
                    SOURCEINFO);
            }
            else if (m_ctx->errorHandler() != nullptr)
            {
                ArchiveException ex(
                    static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                    "response for correlationId=" + std::to_string(correlationId)
                    + ", error: " + m_controlResponsePoller->errorMessage(),
                    SOURCEINFO);
                m_ctx->errorHandler()(ex);
            }
        }
        else if (m_controlResponsePoller->correlationId() == correlationId)
        {
            if (!m_controlResponsePoller->isCodeOk())
            {
                throw ArchiveException(
                    "unexpected response code: " + std::to_string(m_controlResponsePoller->codeValue()), SOURCEINFO);
            }

            return m_controlResponsePoller->relevantId();
        }
    }
}
