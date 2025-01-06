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

#include "AeronArchive.h"

using namespace aeron;
using namespace aeron::archive::client;

constexpr const std::int32_t AERON_ARCHIVE_PROTOCOL_VERSION_WITH_ARCHIVE_ID = aeron::util::semanticVersionCompose(
    1, 11, 0);

AeronArchive::AsyncConnect::AsyncConnect(
    Context_t &context,
    std::shared_ptr<Aeron> aeron,
    std::int64_t subscriptionId,
    std::int64_t publicationId,
    long long deadlineNs) :
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
            "Archive connect timeout: step=" + std::to_string(m_state) +
            (m_state < State::AWAIT_SUBSCRIPTION_CONNECTED ?
                " publication.uri=" + (m_publication ? m_publication->channel() : "<publication find pending>") :
                " subscription.uri=" + (m_subscription ? m_subscription->channel() : "<subscription find pending>")),
            SOURCEINFO);
    }

    if (State::ADD_PUBLICATION == m_state)
    {
        if (!m_publication)
        {
            m_publication = m_aeron->findExclusivePublication(m_publicationId);
        }

        if (!m_archiveProxy && m_publication)
        {
            m_archiveProxy = std::unique_ptr<ArchiveProxy>(new ArchiveProxy(m_publication));
        }

        if (!m_subscription)
        {
            m_subscription = m_aeron->findSubscription(m_subscriptionId);
        }

        if (!m_controlResponsePoller && m_subscription)
        {
            m_controlResponsePoller = std::unique_ptr<ControlResponsePoller>(new ControlResponsePoller(m_subscription));
        }

        if (m_archiveProxy && m_controlResponsePoller)
        {
            m_state = State::AWAIT_PUBLICATION_CONNECTED;
        }
    }

    if (State::AWAIT_PUBLICATION_CONNECTED == m_state)
    {
        if (!m_archiveProxy->publication()->isConnected())
        {
            return {};
        }

        m_state = State::SEND_CONNECT_REQUEST;
    }

    if (State::SEND_CONNECT_REQUEST == m_state)
    {
        std::string controlResponseChannel = m_subscription->tryResolveChannelEndpointPort();
        if (controlResponseChannel.empty())
        {
            return {};
        }

        auto encodedCredentials = m_ctx->credentialsSupplier().m_encodedCredentials();
        m_correlationId = m_aeron->nextCorrelationId();

        if (!m_archiveProxy->tryConnect(
            controlResponseChannel,m_ctx->controlResponseStreamId(), encodedCredentials, m_correlationId))
        {
            m_ctx->credentialsSupplier().m_onFree(encodedCredentials);
            return {};
        }

        m_ctx->credentialsSupplier().m_onFree(encodedCredentials);
        m_state = State::AWAIT_SUBSCRIPTION_CONNECTED;
    }

    if (State::AWAIT_SUBSCRIPTION_CONNECTED == m_state)
    {
        if (!m_subscription->isConnected())
        {
            return {};
        }

        m_state = State::AWAIT_CONNECT_RESPONSE;
    }

    if (State::SEND_ARCHIVE_ID_REQUEST == m_state)
    {
        if (!m_archiveProxy->archiveId(m_correlationId, m_controlSessionId))
        {
            return {};
        }

        m_state = State::AWAIT_ARCHIVE_ID_RESPONSE;
    }

    if (State::SEND_CHALLENGE_RESPONSE == m_state)
    {
        if (!m_archiveProxy->tryChallengeResponse(
            m_encodedCredentialsFromChallenge, m_correlationId, m_controlSessionId))
        {
            return {};
        }

        m_ctx->credentialsSupplier().m_onFree(m_encodedCredentialsFromChallenge);
        m_encodedCredentialsFromChallenge.first = nullptr;
        m_encodedCredentialsFromChallenge.second = 0;
        m_state = State::AWAIT_CHALLENGE_RESPONSE;
    }

    if (m_controlResponsePoller)
    {
        m_controlResponsePoller->poll();

        if (m_controlResponsePoller->isPollComplete() && m_controlResponsePoller->correlationId() == m_correlationId)
        {
            m_controlSessionId = m_controlResponsePoller->controlSessionId();
            if (m_controlResponsePoller->wasChallenged())
            {
                m_encodedCredentialsFromChallenge = m_ctx->credentialsSupplier().m_onChallenge(
                    m_controlResponsePoller->encodedChallenge());

                m_correlationId = m_aeron->nextCorrelationId();
                m_state = State::SEND_CHALLENGE_RESPONSE;
            }
            else
            {
                if (!m_controlResponsePoller->isCodeOk())
                {
                    if (m_controlResponsePoller->isCodeError())
                    {
                        m_archiveProxy->closeSession(m_controlSessionId);
                        throw ArchiveException(
                            static_cast<std::int32_t>(m_controlResponsePoller->relevantId()),
                            m_correlationId,
                            m_controlResponsePoller->errorMessage(),
                            SOURCEINFO);
                    }

                    m_archiveProxy->closeSession(m_controlSessionId);
                    throw ArchiveException(
                        ARCHIVE_ERROR_CODE_GENERIC,
                        m_correlationId,
                        "unexpected response: code=" + std::to_string(m_controlResponsePoller->codeValue()),
                        SOURCEINFO);
                }

                if (State::AWAIT_ARCHIVE_ID_RESPONSE == m_state)
                {
                    std::int64_t archiveId = m_controlResponsePoller->relevantId();
                    return transitionToDone(archiveId);
                }
                else
                {
                    std::int32_t archiveProtocolVersion = m_controlResponsePoller->version();
                    if (archiveProtocolVersion < AERON_ARCHIVE_PROTOCOL_VERSION_WITH_ARCHIVE_ID)
                    {
                        return transitionToDone(aeron::NULL_VALUE);
                    }
                    else
                    {
                        m_correlationId = m_aeron->nextCorrelationId();
                        m_state = State::SEND_ARCHIVE_ID_REQUEST;
                    }
                }
            }
        }
    }

    return {};
}

AeronArchive::AeronArchive(
    std::unique_ptr<Context_t> ctx,
    std::unique_ptr<ArchiveProxy> archiveProxy,
    std::unique_ptr<ControlResponsePoller> controlResponsePoller,
    std::unique_ptr<RecordingDescriptorPoller> recordingDescriptorPoller,
    std::unique_ptr<RecordingSubscriptionDescriptorPoller> recordingSubscriptionDescriptorPoller,
    std::shared_ptr<Aeron> aeron,
    std::int64_t controlSessionId,
    std::int64_t archiveId) :
    m_ctx(std::move(ctx)),
    m_archiveProxy(std::move(archiveProxy)),
    m_controlResponsePoller(std::move(controlResponsePoller)),
    m_recordingDescriptorPoller(std::move(recordingDescriptorPoller)),
    m_recordingSubscriptionDescriptorPoller(std::move(recordingSubscriptionDescriptorPoller)),
    m_aeron(std::move(aeron)),
    m_nanoClock(systemNanoClock),
    m_controlSessionId(controlSessionId),
    m_archiveId(archiveId),
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

void AeronArchive::checkAndSetupResponseChannel(AeronArchive::Context_t &ctx, std::int64_t subscriptionId)
{
    const std::shared_ptr<ChannelUri> responseChannel = ChannelUri::parse(ctx.controlResponseChannel());

    if (responseChannel->hasControlModeResponse())
    {
        std::string correlationId = std::to_string(subscriptionId);
        const std::shared_ptr<ChannelUri> requestChannel = ChannelUri::parse(ctx.controlRequestChannel());
        requestChannel->put(RESPONSE_CORRELATION_ID_PARAM_NAME, correlationId);
        ctx.controlRequestChannel(requestChannel->toString());
    }
}

std::shared_ptr<AeronArchive::AsyncConnect> AeronArchive::asyncConnect(AeronArchive::Context_t &ctx)
{
    ctx.conclude();

    std::shared_ptr<Aeron> aeron = ctx.aeron();
    const std::int64_t subscriptionId = aeron->addSubscription(
        ctx.controlResponseChannel(), ctx.controlResponseStreamId());
    checkAndSetupResponseChannel(ctx, subscriptionId);
    const std::int64_t publicationId = aeron->addExclusivePublication(
        ctx.controlRequestChannel(), ctx.controlRequestStreamId());
    const long long deadlineNs = systemNanoClock() + ctx.messageTimeoutNs();

    return std::make_shared<AeronArchive::AsyncConnect>(ctx, aeron, subscriptionId, publicationId, deadlineNs);
}

std::string AeronArchive::version()
{
    return { "aeron version=" AERON_VERSION_TXT " commit=" AERON_VERSION_GITSHA };
}
