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

#ifndef AERON_CONTEXT_H
#define AERON_CONTEXT_H

#include <memory>
#include <iostream>

#include "util/Exceptions.h"
#include "concurrent/AgentRunner.h"
#include "concurrent/CountersReader.h"
#include "CncFileDescriptor.h"
#include "status/PublicationErrorFrame.h"

#include "aeronc.h"

namespace aeron
{

using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent;

class Image;

/**
 * Used to represent a null value for when some value is not yet set.
 */
static constexpr std::int32_t NULL_VALUE = -1;

/**
 * Function called by Aeron to deliver notification of an available image.
 *
 * The Image passed may not be the image used internally, but may be copied or moved freely.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param image that has become available.
 */
typedef std::function<void(Image &image)> on_available_image_t;

/**
 * Function called by Aeron to deliver notification that an Image has become unavailable for polling.
 *
 * The Image passed is not guaranteed to be valid after the callback.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param image that has become unavailable
 */
typedef std::function<void(Image &image)> on_unavailable_image_t;

/**
 * Function called by Aeron to deliver notification that the media driver has added a Publication successfully.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param channel of the Publication
 * @param streamId within the channel of the Publication
 * @param sessionId of the Publication
 * @param correlationId used by the Publication for adding. Aka the registrationId returned by Aeron::addPublication
 */
typedef std::function<void(
    const std::string &channel,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t correlationId)> on_new_publication_t;

/**
 * Function called by Aeron to deliver notification that the media driver has added a Subscription successfully.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param channel of the Subscription
 * @param streamId within the channel of the Subscription
 * @param correlationId used by the Subscription for adding. Aka the registrationId returned by Aeron::addSubscription
 */
typedef std::function<void(
    const std::string &channel,
    std::int32_t streamId,
    std::int64_t correlationId)> on_new_subscription_t;

/**
 * Function called by Aeron to deliver notification of a Counter being available.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param countersReader for more detail on the counter.
 * @param registrationId for the counter.
 * @param counterId      that is available.
 */

typedef std::function<void(
    CountersReader &countersReader,
    std::int64_t registrationId,
    std::int32_t counterId)> on_available_counter_t;

/**
 * Function called by Aeron to deliver notification of counter being removed.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing
 * and should not make a reentrant call back into the Aeron instance.
 *
 * @param countersReader for more counter details.
 * @param registrationId for the counter.
 * @param counterId      that is unavailable.
 */
typedef std::function<void(
    CountersReader &countersReader,
    std::int64_t registrationId,
    std::int32_t counterId)> on_unavailable_counter_t;

/**
 * Function called when the Aeron client is closed to notify that the client or any of it associated resources
 * should not be used after this event.
 */
typedef std::function<void()> on_close_client_t;

typedef std::function<void(aeron::status::PublicationErrorFrame &errorFrame)> on_publication_error_frame_t;

const static long NULL_TIMEOUT = -1;
const static long DEFAULT_MEDIA_DRIVER_TIMEOUT_MS = 10000;
const static long DEFAULT_RESOURCE_LINGER_MS = 5000;
const static int MAX_CLIENT_NAME_LENGTH = 100;

/**
 * The Default handler for Aeron runtime exceptions.
 *
 * When a DriverTimeoutException is encountered, this handler will exit the program.
 *
 * The error handler can be overridden by supplying an {@link Context} with a custom handler.
 *
 * @see Context#errorHandler
 */
inline void defaultErrorHandler(const std::exception &exception)
{
    std::cerr << "ERROR: " << exception.what();

    try
    {
        const auto &sourcedException = dynamic_cast<const SourcedException &>(exception);
        std::cerr << " : " << sourcedException.where();
    }
    catch (const std::bad_cast &)
    {
        // ignore
    }

    std::cerr << std::endl;
    ::exit(-1);
}

inline void defaultOnNewPublicationHandler(const std::string &, std::int32_t, std::int32_t, std::int64_t)
{
}

inline void defaultOnAvailableImageHandler(Image &)
{
}

inline void defaultOnNewSubscriptionHandler(const std::string &, std::int32_t, std::int64_t)
{
}

inline void defaultOnUnavailableImageHandler(Image &)
{
}

inline void defaultOnAvailableCounterHandler(CountersReader &, std::int64_t, std::int32_t)
{
}

inline void defaultOnUnavailableCounterHandler(CountersReader &, std::int64_t, std::int32_t)
{
}

inline void defaultOnCloseClientHandler()
{
}

inline void defaultOnErrorFrameHandler(aeron::status::PublicationErrorFrame &)
{
}

/**
 * This class provides configuration for the {@link Aeron} class via the {@link Aeron::Aeron} or {@link Aeron::connect}
 * methods and its overloads. It gives applications some control over the interactions with the Aeron Media Driver.
 * It can also set up error handling as well as application callbacks for connection information from the
 * Media Driver.
 */
class Context
{
    friend class Aeron;

public:
    using this_t = Context;

    Context()
    {
        aeron_context_init(&m_context);
    }

    /// @cond HIDDEN_SYMBOLS
    explicit Context(aeron_context_t *context)
    {
        m_context = context;
    }
    /// @endcond

    Context(Context &&other) noexcept :
        m_context(other.m_context),
        m_onAvailableImageHandler(other.m_onAvailableImageHandler),
        m_onUnavailableImageHandler(other.m_onUnavailableImageHandler),
        m_exceptionHandler(other.m_exceptionHandler),
        m_onNewPublicationHandler(other.m_onNewPublicationHandler),
        m_isOnNewExclusivePublicationHandlerSet(other.m_isOnNewExclusivePublicationHandlerSet),
        m_onNewExclusivePublicationHandler(other.m_onNewExclusivePublicationHandler),
        m_onNewSubscriptionHandler(other.m_onNewSubscriptionHandler),
        m_onAvailableCounterHandler(other.m_onAvailableCounterHandler),
        m_onUnavailableCounterHandler(other.m_onUnavailableCounterHandler),
        m_onCloseClientHandler(other.m_onCloseClientHandler),
        m_onErrorFrameHandler(other.m_onErrorFrameHandler)
    {
        other.m_context = nullptr;
    }

    ~Context()
    {
        aeron_context_close(m_context);
    }

    /// @cond HIDDEN_SYMBOLS
    this_t &conclude()
    {
        if (clientName().length() > MAX_CLIENT_NAME_LENGTH)
        {
            throw util::IllegalArgumentException("clientName length must <= 100", SOURCEINFO);
        }

        if (!m_isOnNewExclusivePublicationHandlerSet)
        {
            newExclusivePublicationHandler(m_onNewPublicationHandler);
        }
        return *this;
    }
    /// @endcond

    /**
     * Set the directory that the Aeron client will use to communicate with the media driver.
     *
     * @param directory to use
     * @return reference to this Context instance
     */
    inline this_t &aeronDir(const std::string &directory)
    {
        if (aeron_context_set_dir(m_context, directory.c_str()) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Set the name for this Aeron client.
     *
     * @param clientName to set.
     * @return reference to this Context instance.
     */
    inline this_t &clientName(const std::string &clientName)
    {
        if (aeron_context_set_client_name(m_context, clientName.c_str()) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Get the name of this Aeron client.
     *
     * @return client name or empty string.
     */
    inline std::string clientName()
    {
        return {aeron_context_get_client_name(m_context)};
    }

    /**
     * Return the path to the CnC file used by the Aeron client for communication with the media driver.
     *
     * @return path of the CnC file
     */
    inline std::string cncFileName() const
    {
        const std::string dir = std::string(aeron_context_get_dir(m_context));
        return dir + std::string(1, AERON_FILE_SEP) + CncFileDescriptor::CNC_FILE;
    }

    /**
     * Set the handler for exceptions from the Aeron client.
     *
     * @param handler called when exceptions arise
     * @return reference to this Context instance
     *
     * @see defaultErrorHandler for how the default behavior is handled
     */
    inline this_t &errorHandler(const exception_handler_t &handler)
    {
        m_exceptionHandler = handler;
        return *this;
    }

    /**
     * Set the handler for successful Aeron::addPublication notifications.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    inline this_t &newPublicationHandler(const on_new_publication_t &handler)
    {
        m_onNewPublicationHandler = handler;
        return *this;
    }

    /**
     * Set the handler for successful Aeron::addExclusivePublication notifications.
     *
     * If not set, then will use newPublicationHandler instead.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    inline this_t &newExclusivePublicationHandler(const on_new_publication_t &handler)
    {
        m_onNewExclusivePublicationHandler = handler;
        m_isOnNewExclusivePublicationHandlerSet = true;
        return *this;
    }

    /**
     * Set the handler for successful Aeron::addSubscription notifications.
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    inline this_t &newSubscriptionHandler(const on_new_subscription_t &handler)
    {
        m_onNewSubscriptionHandler = handler;
        return *this;
    }

    /**
     * Set the handler for available image notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t &availableImageHandler(const on_available_image_t &handler)
    {
        m_onAvailableImageHandler = handler;
        return *this;
    }

    /**
     * Set the handler for inactive image notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t &unavailableImageHandler(const on_unavailable_image_t &handler)
    {
        m_onUnavailableImageHandler = handler;
        return *this;
    }

    /**
     * Set the handler for available counter notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t &availableCounterHandler(const on_available_counter_t &handler)
    {
        m_onAvailableCounterHandler = handler;
        return *this;
    }

    /**
     * Set the handler for inactive counter notifications.
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t &unavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        m_onUnavailableCounterHandler = handler;
        return *this;
    }

    /**
     * Set the handler to be called when the Aeron client is closed and not longer active.
     *
     * @param handler to be called when the Aeron client is closed.
     * @return reference to this Context instance.
     */
    inline this_t &closeClientHandler(const on_close_client_t &handler)
    {
        m_onCloseClientHandler = handler;
        return *this;
    }

    /**
     * Set the amount of time, in milliseconds, that this client will wait until it determines the
     * Media Driver is unavailable. When this happens a DriverTimeoutException will be generated for the error handler.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     * @see errorHandler
     */
    inline this_t &mediaDriverTimeout(long value)
    {
        if (value < 0)
        {
            throw IllegalArgumentException("timeout less than 0", SOURCEINFO);
        }

        if (aeron_context_set_driver_timeout_ms(m_context, static_cast<std::uint64_t>(value)) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Get the amount of time, in milliseconds, that this client will wait until it determines the
     * Media Driver is unavailable. When this happens a DriverTimeoutException will be generated for the error handler.
     *
     * @return value in number of milliseconds.
     * @see errorHandler
     */
    long mediaDriverTimeout() const
    {
        return static_cast<long>(aeron_context_get_driver_timeout_ms(m_context));
    }

    /**
     * Set the amount of time, in milliseconds, that this client will to linger inactive connections and internal
     * arrays before they are freed.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     */
    inline this_t &resourceLingerTimeout(long value)
    {
        if (value < 0)
        {
            throw IllegalArgumentException("timeout less than 0", SOURCEINFO);
        }
        std::uint64_t duration_ns = static_cast<std::uint64_t>(value) * 1000000;
        if (aeron_context_set_resource_linger_duration_ns(m_context, duration_ns) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Get the amount of time, in milliseconds, that the client conductor will sleep when idle.
     *
     * @return value in number of milliseconds.
     * @see errorHandler
     */
    long idleSleepDuration() const
    {
        return static_cast<long>(aeron_context_get_idle_sleep_duration_ns(m_context) / 1000000);
    }

    /**
     * Set the amount of time, in milliseconds, that the client conductor will sleep when idle.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     */
    inline this_t &idleSleepDuration(long value)
    {
        if (value < 0)
        {
            throw IllegalArgumentException("idle sleep less than 0", SOURCEINFO);
        }

        std::uint64_t duration_ns = static_cast<std::uint64_t>(value) * 1000000;
        if (aeron_context_set_idle_sleep_duration_ns(m_context, duration_ns) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Set whether to use an invoker to control the conductor agent or spawn a thread.
     *
     * @param useConductorAgentInvoker to use an invoker or not.
     * @return reference to this Context instance
     */
    inline this_t &useConductorAgentInvoker(bool useConductorAgentInvoker)
    {
        if (aeron_context_set_use_conductor_agent_invoker(m_context, useConductorAgentInvoker) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
        return *this;
    }

    /**
     * Set whether memory mapped files should be pre-touched so they are pre-loaded to avoid later page faults.
     *
     * @param preTouchMappedMemory true to pre-touch memory otherwise false.
     * @return reference to this Context instance
     */
    inline this_t &preTouchMappedMemory(bool preTouchMappedMemory)
    {
        aeron_context_set_pre_touch_mapped_memory(m_context, preTouchMappedMemory);
        return *this;
    }

    /**
     * Set handler to receive notifications when a publication error is received for a publication that this client
     * is interested in.
     *
     * @param handler
     * @return reference to this Context instance.
     */
    inline this_t &errorFrameHandler(on_publication_error_frame_t &handler)
    {
        m_onErrorFrameHandler = handler;
        return *this;
    }

    static bool requestDriverTermination(
        const std::string &directory, const std::uint8_t *tokenBuffer, std::size_t tokenLength)
    {
        int result = aeron_context_request_driver_termination(directory.c_str(), tokenBuffer, tokenLength);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return 1 == result;
    }

    static std::string defaultAeronPath()
    {
        char path[1024];
        std::size_t length = sizeof(path);
        int result = aeron_default_path(path, length);

        if (result < 0)
        {
            std::string errMsg = std::string("Failed to get default path, result: ") += std::to_string(result);
            throw IllegalStateException(errMsg, SOURCEINFO);
        }
        else if (length <= static_cast<std::size_t>(result))
        {
            std::string errMsg = std::string("Path information was truncated, buffer length: ");
            errMsg += std::to_string(length);
            errMsg += ", path length: ";
            errMsg += std::to_string(result);
            errMsg += ", path: ";
            errMsg += std::string(path, 0, length);

            throw IllegalStateException(errMsg, SOURCEINFO);
        }

        return { path, 0, length };
    }

private:
    aeron_context_t *m_context = nullptr;
    on_available_image_t m_onAvailableImageHandler = defaultOnAvailableImageHandler;
    on_unavailable_image_t m_onUnavailableImageHandler = defaultOnUnavailableImageHandler;
    exception_handler_t m_exceptionHandler = defaultErrorHandler;
    on_new_publication_t m_onNewPublicationHandler = defaultOnNewPublicationHandler;
    bool m_isOnNewExclusivePublicationHandlerSet = false;
    on_new_publication_t m_onNewExclusivePublicationHandler = defaultOnNewPublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler = defaultOnNewSubscriptionHandler;
    on_available_counter_t m_onAvailableCounterHandler = defaultOnAvailableCounterHandler;
    on_unavailable_counter_t m_onUnavailableCounterHandler = defaultOnUnavailableCounterHandler;
    on_close_client_t m_onCloseClientHandler = defaultOnCloseClientHandler;
    on_publication_error_frame_t m_onErrorFrameHandler = defaultOnErrorFrameHandler;

    void attachCallbacksToContext()
    {
        if (aeron_context_set_error_handler(
            m_context,
            errorHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_exceptionHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_new_publication(
            m_context,
            newPublicationHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onNewPublicationHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_new_exclusive_publication(
            m_context,
            newPublicationHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onNewExclusivePublicationHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_available_counter(
            m_context,
            availableCounterHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onAvailableCounterHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_unavailable_counter(
            m_context,
            availableCounterHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onUnavailableCounterHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_close_client(
            m_context,
            closeClientHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onCloseClientHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_on_new_subscription(
            m_context,
            newSubscriptionHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onNewSubscriptionHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }

        if (aeron_context_set_publication_error_frame_handler(
            m_context,
            errorFrameHandlerCallback,
            const_cast<void *>(reinterpret_cast<const void *>(&m_onErrorFrameHandler))) < 0)
        {
            throw IllegalArgumentException(std::string(aeron_errmsg()), SOURCEINFO);
        }
    }

    static void errorHandlerCallback(void *clientd, int errcode, const char *message)
    {
        try
        {
            AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW(errcode, message);
        }
        catch (SourcedException &exception)
        {
            exception_handler_t &handler = *reinterpret_cast<exception_handler_t *>(clientd);
            handler(exception);
        }
    }

    static void newPublicationHandlerCallback(
        void *clientd,
        aeron_async_add_publication_t * /* async */,
        const char *channel,
        std::int32_t stream_id,
        std::int32_t session_id,
        std::int64_t correlation_id)
    {
        on_new_publication_t &handler = *reinterpret_cast<on_new_publication_t *>(clientd);
        handler(std::string(channel), stream_id, session_id, correlation_id);
    }

    static void newSubscriptionHandlerCallback(
        void *clientd,
        aeron_async_add_subscription_t * /* async */,
        const char *channel,
        std::int32_t stream_id,
        std::int64_t correlation_id)
    {
        on_new_subscription_t &handler = *reinterpret_cast<on_new_subscription_t *>(clientd);
        handler(std::string(channel), stream_id, correlation_id);
    }

    static void availableCounterHandlerCallback(
        void *clientd, aeron_counters_reader_t *counters_reader, std::int64_t registration_id, std::int32_t counter_id)
    {
        on_available_counter_t &handler = *reinterpret_cast<on_available_counter_t *>(clientd);
        CountersReader countersReader(counters_reader);
        handler(countersReader, registration_id, counter_id);
    }

    static void closeClientHandlerCallback(void *clientd)
    {
        on_close_client_t &handler = *reinterpret_cast<on_close_client_t *>(clientd);
        handler();
    }

    static void errorFrameHandlerCallback(void *clientd, aeron_publication_error_values_t *error_frame)
    {
        on_publication_error_frame_t &handler = *reinterpret_cast<on_publication_error_frame_t *>(clientd);
        aeron::status::PublicationErrorFrame errorFrame{error_frame};
        handler(errorFrame);
    }
};

}

#endif
