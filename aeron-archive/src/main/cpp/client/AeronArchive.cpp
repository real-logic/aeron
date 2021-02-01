/*
 * Copyright 2014-2021 Real Logic Limited.
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

#include "AeronArchive.h"

using namespace aeron;
using namespace aeron::archive::client;

AeronArchive::AsyncConnect::AsyncConnect(
    Context_t &context,
    std::shared_ptr<Aeron> aeron,
    std::int64_t subscriptionId,
    std::int64_t publicationId,
    const long long deadlineNs) :
    m_nanoClock(systemNanoClock),
    m_ctx(std::unique_ptr<Context_t>(new Context_t(context))),
    m_aeron(std::move(aeron)),
    m_subscriptionId(subscriptionId),
    m_publicationId(publicationId),
    m_deadlineNs(deadlineNs)
{
}

std::shared_ptr<AeronArchive> AeronArchive::AsyncConnect::poll()
{
    if (m_nanoClock() > m_deadlineNs)
    {
        throw TimeoutException(
            "Archive connect timeout: correlationId=" + std::to_string(m_correlationId) +
            " step=" + std::to_string(m_step),
            SOURCEINFO);
    }

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
        std::string controlResponseChannel = m_subscription->tryResolveChannelEndpointPort();
        if (controlResponseChannel.empty())
        {
            return std::shared_ptr<AeronArchive>();
        }

        auto encodedCredentials = m_ctx->credentialsSupplier().m_encodedCredentials();
        m_correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->tryConnect(
            controlResponseChannel,m_ctx->controlResponseStreamId(), encodedCredentials, m_correlationId))
        {
            m_ctx->credentialsSupplier().m_onFree(encodedCredentials);
            return std::shared_ptr<AeronArchive>();
        }

        m_ctx->credentialsSupplier().m_onFree(encodedCredentials);
        m_step = 2;
    }

    if (2 == m_step && m_controlResponsePoller)
    {
        if (!m_subscription->isConnected())
        {
            return std::shared_ptr<AeronArchive>();
        }

        m_step = 3;
    }

    if (5 == m_step && m_controlResponsePoller)
    {
        if (!m_archiveProxy->tryChallengeResponse(
            m_encodedCredentialsFromChallenge, m_correlationId, m_challengeControlSessionId))
        {
            return std::shared_ptr<AeronArchive>();
        }

        m_ctx->credentialsSupplier().m_onFree(m_encodedCredentialsFromChallenge);
        m_encodedCredentialsFromChallenge.first = nullptr;
        m_encodedCredentialsFromChallenge.second = 0;
        m_step = 6;
    }

    if (m_controlResponsePoller)
    {
        m_controlResponsePoller->poll();

        if (m_controlResponsePoller->isPollComplete() && m_controlResponsePoller->correlationId() == m_correlationId)
        {
            const std::int64_t sessionId = m_controlResponsePoller->controlSessionId();

            if (m_controlResponsePoller->wasChallenged())
            {
                m_encodedCredentialsFromChallenge = m_ctx->credentialsSupplier().m_onChallenge(
                    m_controlResponsePoller->encodedChallenge());

                m_correlationId = m_aeron->nextCorrelationId();
                m_challengeControlSessionId = sessionId;
                m_step = 5;
            }
            else
            {
                if (!m_controlResponsePoller->isCodeOk())
                {
                    if (m_controlResponsePoller->isCodeError())
                    {
                        m_archiveProxy->closeSession(sessionId);

                        throw ArchiveException(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            "error: " + m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                    }

                    throw ArchiveException(
                        "unexpected response: code=" + std::to_string(m_controlResponsePoller->codeValue()),
                        SOURCEINFO);
                }

                m_archiveProxy->keepAlive(aeron::NULL_VALUE, sessionId);

                std::unique_ptr<RecordingDescriptorPoller> recordingDescriptorPoller(
                    new RecordingDescriptorPoller(m_subscription, m_ctx->errorHandler(), sessionId));
                std::unique_ptr<RecordingSubscriptionDescriptorPoller> recordingSubscriptionDescriptorPoller(
                    new RecordingSubscriptionDescriptorPoller(m_subscription, m_ctx->errorHandler(), sessionId));

                return std::make_shared<AeronArchive>(
                    std::move(m_ctx),
                    std::move(m_archiveProxy),
                    std::move(m_controlResponsePoller),
                    std::move(recordingDescriptorPoller),
                    std::move(recordingSubscriptionDescriptorPoller),
                    m_aeron,
                    sessionId);
            }
        }
    }

    return std::shared_ptr<AeronArchive>();
}

AeronArchive::AeronArchive(
    std::unique_ptr<Context_t> ctx,
    std::unique_ptr<ArchiveProxy> archiveProxy,
    std::unique_ptr<ControlResponsePoller> controlResponsePoller,
    std::unique_ptr<RecordingDescriptorPoller> recordingDescriptorPoller,
    std::unique_ptr<RecordingSubscriptionDescriptorPoller> recordingSubscriptionDescriptorPoller,
    std::shared_ptr<Aeron> aeron,
    std::int64_t controlSessionId) :
    m_ctx(std::move(ctx)),
    m_archiveProxy(std::move(archiveProxy)),
    m_controlResponsePoller(std::move(controlResponsePoller)),
    m_recordingDescriptorPoller(std::move(recordingDescriptorPoller)),
    m_recordingSubscriptionDescriptorPoller(std::move(recordingSubscriptionDescriptorPoller)),
    m_aeron(std::move(aeron)),
    m_nanoClock(systemNanoClock),
    m_controlSessionId(controlSessionId),
    m_messageTimeoutNs(m_ctx->messageTimeoutNs())
{
}

AeronArchive::~AeronArchive()
{
    if (m_archiveProxy->publication()->isConnected())
    {
        m_archiveProxy->closeSession(m_controlSessionId);
    }
}

std::shared_ptr<AeronArchive::AsyncConnect> AeronArchive::asyncConnect(AeronArchive::Context_t &ctx)
{
    ctx.conclude();

    std::shared_ptr<Aeron> aeron = ctx.aeron();
    const std::int64_t subscriptionId = aeron->addSubscription(
        ctx.controlResponseChannel(), ctx.controlResponseStreamId());
    const std::int64_t publicationId = aeron->addExclusivePublication(
        ctx.controlRequestChannel(), ctx.controlRequestStreamId());
    const long long deadlineNs = systemNanoClock() + ctx.messageTimeoutNs();

    return std::make_shared<AeronArchive::AsyncConnect>(ctx, aeron, subscriptionId, publicationId, deadlineNs);
}

std::string AeronArchive::version()
{
    return std::string("aeron version " AERON_VERSION_TXT " built " __DATE__ " " __TIME__);
}
