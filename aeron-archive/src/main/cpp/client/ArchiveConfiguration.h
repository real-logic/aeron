/*
 * Copyright 2014-2020 Real Logic Limited.
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
#ifndef AERON_ARCHIVE_CONFIGURATION_H
#define AERON_ARCHIVE_CONFIGURATION_H

#include <cstdint>

#include "Aeron.h"
#include "ChannelUri.h"
#include "util/MacroUtil.h"

namespace aeron { namespace archive { namespace client
{

/**
 * Represents a timestamp that has not been set. Can be used when the time is not known.
 */
constexpr const std::int64_t NULL_TIMESTAMP = aeron::NULL_VALUE;

/**
 * Represents a position that has not been set. Can be used when the position is not known.
 */
constexpr const std::int64_t NULL_POSITION = aeron::NULL_VALUE;

/**
 * Represents a length that has not been set. If null length is provided then replay the whole recorded stream.
 */
constexpr const std::int64_t NULL_LENGTH = aeron::NULL_VALUE;

/**
 * Callback to return encoded credentials.
 *
 * @return encoded credentials to send on connect request.
 */
typedef std::function<std::pair<const char *, std::uint32_t>()> credentials_encoded_credentials_supplier_t;

inline std::pair<const char *, std::uint32_t> defaultCredentialsEncodedCredentials()
{
    return { nullptr, 0 };
}

/**
 * Callback to return encoded credentials given specific encoded challenge
 *
 * @param encodedChallenge to use to generate the encoded credentials.
 * @return encoded credentials to send in challenge response.
 */
typedef std::function<std::pair<const char *, std::uint32_t>(
    std::pair<const char *, std::uint32_t> encodedChallenge)> credentials_challenge_supplier_t;

inline std::pair<const char *, std::uint32_t> defaultCredentialsOnChallenge(
    std::pair<const char *, std::uint32_t> encodedChallenge)
{
    return { nullptr, 0 };
}

/**
 * Callback to return encoded credentials so they may be reused or free'd.
 *
 * @param encodedCredentials to re-use or free.
 */
typedef std::function<void(std::pair<const char *, std::uint32_t> encodedCredentials)> credentials_free_t;

inline void defaultCredentialsOnFree(std::pair<const char *, std::uint32_t> credentials)
{
    delete[] credentials.first;
}

/**
 * Structure to hold credential callbacks.
 */
struct CredentialsSupplier
{
    credentials_encoded_credentials_supplier_t m_encodedCredentials = defaultCredentialsEncodedCredentials;
    credentials_challenge_supplier_t m_onChallenge = defaultCredentialsOnChallenge;
    credentials_free_t m_onFree = defaultCredentialsOnFree;

    explicit CredentialsSupplier(
        credentials_encoded_credentials_supplier_t encodedCredentials = defaultCredentialsEncodedCredentials,
        credentials_challenge_supplier_t onChallenge = defaultCredentialsOnChallenge,
        credentials_free_t onFree = defaultCredentialsOnFree) :
        m_encodedCredentials(std::move(encodedCredentials)),
        m_onChallenge(std::move(onChallenge)),
        m_onFree(std::move(onFree))
    {
    }
};

namespace Configuration
{
constexpr const std::uint8_t ARCHIVE_MAJOR_VERSION = 1;
constexpr const std::uint8_t ARCHIVE_MINOR_VERSION = 5;
constexpr const std::uint8_t ARCHIVE_PATCH_VERSION = 0;
constexpr const std::int32_t ARCHIVE_SEMANTIC_VERSION = aeron::util::semanticVersionCompose(
    ARCHIVE_MAJOR_VERSION, ARCHIVE_MINOR_VERSION, ARCHIVE_PATCH_VERSION);

/// Timeout when waiting on a message to be sent or received.
constexpr const long long MESSAGE_TIMEOUT_NS_DEFAULT = 5 * 1000 * 1000 * 1000LL;

/// Channel for sending control messages to an archive.
constexpr const char CONTROL_REQUEST_CHANNEL_DEFAULT[] = "aeron:udp?endpoint=localhost:8010";
/// Stream id within a channel for sending control messages to an archive.
constexpr const std::int32_t CONTROL_REQUEST_STREAM_ID_DEFAULT = 10;

/// Channel for sending control messages to a driver local archive. Default to IPC.
constexpr const char LOCAL_CONTROL_REQUEST_CHANNEL_DEFAULT[] = "aeron:ipc";
/// Stream id within a channel for sending control messages to a driver local archive.
constexpr const std::int32_t LOCAL_CONTROL_REQUEST_STREAM_ID_DEFAULT = 11;

/// Channel for receiving control response messages from an archive.
constexpr const char CONTROL_RESPONSE_CHANNEL_DEFAULT[] = "aeron:udp?endpoint=localhost:8020";
/// Stream id within a channel for receiving control messages from an archive.
constexpr const std::int32_t CONTROL_RESPONSE_STREAM_ID_DEFAULT = 20;

/**
 * Channel for receiving progress events of recordings from an archive.
 * For production it is recommended that multicast or dynamic multi-destination-cast (MDC) is used to allow
 * for dynamic subscribers, an endpoint can be added to the subscription side for controlling port usage.
 */
constexpr const char RECORDING_EVENTS_CHANNEL_DEFAULT[] = "aeron:udp?control-mode=dynamic|control=localhost:8030";
/// Stream id within a channel for receiving progress of recordings from an archive.
constexpr const std::int32_t RECORDING_EVENTS_STREAM_ID_DEFAULT = 30;

/**
 * Overrides term buffer sparse file configuration for if term buffer files
 * are sparse on the control channel.
 */
constexpr const bool CONTROL_TERM_BUFFER_SPARSE_DEFAULT = true;

/// Low term length for control channel reflects expected low bandwidth usage.
constexpr const std::int32_t CONTROL_TERM_BUFFER_LENGTH_DEFAULT = 64 * 1024;
/// MTU to reflect default for the control streams.
constexpr const std::int32_t CONTROL_MTU_LENGTH_DEFAULT = 1408;

}

/**
 * Specialised configuration options for communicating with an Aeron Archive.
 * <p>
 * The context will be copied after a successful
 * AeronArchive#connect(Context) or AeronArchive::asyncConnect(Context).
 */
class Context
{
public:
    using this_t = Context;

    /**
     * Conclude configuration by setting up defaults when specifics are not provided.
     */
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
        channelUri->put(SPARSE_PARAM_NAME, m_controlTermBufferSparse ? "true" : "false");
        m_controlRequestChannel = channelUri->toString();
    }

    /**
     * Aeron client for communicating with the local Media Driver.
     * <p>
     * If not provided then a default will be established during #conclude by calling
     * Aeron#connect.
     *
     * @return client for communicating with the local Media Driver.
     */
    inline std::shared_ptr<Aeron> aeron()
    {
        return m_aeron;
    }

    /**
     * Aeron client for communicating with the local Media Driver.
     * <p>
     * This client will be closed when the AeronArchive goes out of scope if
     * #ownsAeronClient is true.
     *
     * @param aeron client for communicating with the local Media Driver.
     * @return this for a fluent API.
     * @see Aeron#connect()
     */
    inline this_t &aeron(std::shared_ptr<Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        return *this;
    }

    /**
     * The message timeout in nanoseconds to wait for sending or receiving a message.
     *
     * @return the message timeout in nanoseconds to wait for sending or receiving a message.
     * @see MESSAGE_TIMEOUT_NS_DEFAULT
     */
    inline long long messageTimeoutNs()
    {
        return m_messageTimeoutNs;
    }

    /**
     * Set the message timeout in nanoseconds to wait for sending or receiving a message.
     *
     * @param messageTimeoutNs to wait for sending or receiving a message.
     * @return this for a fluent API.
     * @see MESSAGE_TIMEOUT_NS_DEFAULT
     */
    inline this_t &messageTimeoutNs(long long timeoutNs)
    {
        m_messageTimeoutNs = timeoutNs;
        return *this;
    }

    /**
     * Get the channel URI on which the recording events publication will publish.
     *
     * @return the channel URI on which the recording events publication will publish.
     */
    inline std::string recordingEventsChannel()
    {
        return m_recordingEventsChannel;
    }

    /**
     * Set the channel URI on which the recording events publication will publish.
     * <p>
     * To support dynamic subscribers then this can be set to multicast or MDC (Multi-Destination-Cast) if
     * multicast cannot be supported for on the available the network infrastructure.
     *
     * @param recordingEventsChannel channel URI on which the recording events publication will publish.
     * @return this for a fluent API.
     * @see RECORDING_EVENTS_CHANNEL_DEFAULT
     */
    inline this_t &recordingEventsChannel(const std::string &recordingEventsChannel)
    {
        m_recordingEventsChannel = recordingEventsChannel;
        return *this;
    }

    /**
     * Get the stream id on which the recording events publication will publish.
     *
     * @return the stream id on which the recording events publication will publish.
     */
    inline std::int32_t recordingEventsStreamId()
    {
        return m_recordingEventsStreamId;
    }

    /**
     * Set the stream id on which the recording events publication will publish.
     *
     * @param recordingEventsStreamId stream id on which the recording events publication will publish.
     * @return this for a fluent API.
     */
    inline this_t &recordingEventsStreamId(std::int32_t recordingEventsStreamId)
    {
        m_recordingEventsStreamId = recordingEventsStreamId;
        return *this;
    }

    /**
     * Get the channel parameter for the control response channel.
     *
     * @return the channel parameter for the control response channel.
     */
    inline std::string controlResponseChannel()
    {
        return m_controlResponseChannel;
    }

    /**
     * Set the channel parameter for the control response channel.
     *
     * @param channel parameter for the control response channel.
     * @return this for a fluent API.
     */
    inline this_t &controlResponseChannel(const std::string &channel)
    {
        m_controlResponseChannel = channel;
        return *this;
    }

    /**
     * Get the stream id for the control response channel.
     *
     * @return the stream id for the control response channel.
     */
    inline std::int32_t controlResponseStreamId()
    {
        return m_controlResponseStreamId;
    }

    /**
     * Set the stream id for the control response channel.
     *
     * @param streamId for the control response channel.
     * @return this for a fluent API
     */
    inline this_t &controlResponseStreamId(std::int32_t streamId)
    {
        m_controlResponseStreamId = streamId;
        return *this;
    }

    /**
     * Get the channel parameter for the control request channel.
     *
     * @return the channel parameter for the control request channel.
     */
    inline std::string controlRequestChannel()
    {
        return m_controlRequestChannel;
    }

    /**
     * Set the channel parameter for the control request channel.
     *
     * @param channel parameter for the control request channel.
     * @return this for a fluent API.
     */
    inline this_t &controlRequestChannel(const std::string &channel)
    {
        m_controlRequestChannel = channel;
        return *this;
    }

    /**
     * Get the stream id for the control request channel.
     *
     * @return the stream id for the control request channel.
     */
    inline std::int32_t controlRequestStreamId()
    {
        return m_controlRequestStreamId;
    }

    /**
     * Set the stream id for the control request channel.
     *
     * @param streamId for the control request channel.
     * @return this for a fluent API
     */
    inline this_t &controlRequestStreamId(std::int32_t streamId)
    {
        m_controlRequestStreamId = streamId;
        return *this;
    }

    /**
     * Should the control streams use sparse file term buffers.
     *
     * @return true if the control stream should use sparse file term buffers.
     */
    inline bool controlTermBufferSparse()
    {
        return m_controlTermBufferSparse;
    }

    /**
     * Should the control streams use sparse file term buffers.
     *
     * @param controlTermBufferSparse for the control stream.
     * @return this for a fluent API.
     */
    inline this_t &controlTermBufferSparse(bool controlTermBufferSparse)
    {
        m_controlTermBufferSparse = controlTermBufferSparse;
        return *this;
    }

    /**
     * Get the term buffer length for the control streams.
     *
     * @return the term buffer length for the control streams.
     */
    inline std::int32_t controlTermBufferLength()
    {
        return m_controlTermBufferLength;
    }

    /**
     * Set the term buffer length for the control streams.
     *
     * @param controlTermBufferLength for the control streams.
     * @return this for a fluent API.
     */
    inline this_t &controlTermBufferLength(std::int32_t controlTermBufferLength)
    {
        m_controlTermBufferLength = controlTermBufferLength;
        return *this;
    }

    /**
     * Get the MTU length for the control streams.
     *
     * @return the MTU length for the control streams.
     */
    inline std::int32_t controlMtuLength()
    {
        return m_controlMtuLength;
    }

    /**
     * Set the MTU length for the control streams.
     *
     * @param controlMtuLength for the control streams.
     * @return this for a fluent API.
     */
    inline this_t &controlMtuLength(std::int32_t controlMtuLength)
    {
        m_controlMtuLength = controlMtuLength;
        return *this;
    }

    /**
     * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
     *
     * @return The top level Aeron directory.
     */
    inline std::string aeronDirectoryName()
    {
        return m_aeronDirectoryName;
    }

    /**
     * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
     *
     * @param aeronDirectoryName the top level Aeron directory.
     * @return this for a fluent API.
     */
    inline this_t &aeronDirectoryName(const std::string &aeronDirectoryName)
    {
        m_aeronDirectoryName = aeronDirectoryName;
        return *this;
    }

    /**
     * Does this context own the Aeron client and thus takes responsibility for closing it?
     *
     * @return does this context own the Aeron client and thus takes responsibility for closing it?
     */
    inline bool ownsAeronClient()
    {
        return m_ownsAeronClient;
    }

    /**
     * Does this context own the Aeron client and thus takes responsibility for closing it?
     *
     * @param ownsAeronClient does this context own the Aeron client?
     * @return this for a fluent API.
     */
    inline this_t &ownsAeronClient(bool ownsAeronClient)
    {
        m_ownsAeronClient = ownsAeronClient;
        return *this;
    }

    /**
     * Get the error handler that will be called for asynchronous errors.
     *
     * @return the error handler that will be called for asynchronous errors.
     */
    inline exception_handler_t errorHandler()
    {
        return m_errorHandler;
    }

    /**
     * Handle errors returned asynchronously from the archive for a control session.
     *
     * @param errorHandler method to handle objects of type std::exception.
     * @return this for a fluent API.
     */
    inline this_t &errorHandler(const exception_handler_t &errorHandler)
    {
        m_errorHandler = errorHandler;
        return *this;
    }

    /**
     * Get the credential supplier that will be called for generating encoded credentials.
     *
     * @return the credential supplier that will be called for generating encoded credentials.
     */
    inline CredentialsSupplier &credentialsSupplier()
    {
        return m_credentialsSupplier;
    }

    /**
     * Set the CredentialSupplier functions to be called as connect requests are handled.
     *
     * @param supplier that holds functions to be called.
     * @return this for a fluent API.
     */
    inline this_t &credentialsSupplier(const CredentialsSupplier &supplier)
    {
        m_credentialsSupplier.m_encodedCredentials = supplier.m_encodedCredentials;
        m_credentialsSupplier.m_onChallenge = supplier.m_onChallenge;
        m_credentialsSupplier.m_onFree = supplier.m_onFree;
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

    CredentialsSupplier m_credentialsSupplier;
};

}}}

#endif //AERON_ARCHIVE_CONFIGURATION_H
