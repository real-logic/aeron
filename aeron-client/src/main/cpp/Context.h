/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_CONTEXT__
#define INCLUDED_AERON_CONTEXT__

#include <memory>
#include <util/Exceptions.h>
#include <concurrent/AgentRunner.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <CncFileDescriptor.h>
#include <iostream>

namespace aeron {

using namespace aeron::concurrent::ringbuffer;
using namespace aeron::concurrent::logbuffer;
using namespace aeron::concurrent::broadcast;

class Image;

/**
 * Function called by Aeron to deliver notification of an available image
 *
 * The Image passed may not be the image used internally, but may be copied or moved freely.
 *
 * @param image           that has become available.
 */
typedef std::function<void(Image& image)> on_available_image_t;

/**
 * Function called by Aeron to deliver notification that an Image has become unavailable for polling.
 *
 * The Image passed is not guaranteed to be valid after the callback.
 *
 * @param image     that has become unavailable
 */
typedef std::function<void(Image& image)> on_unavailable_image_t;

/**
 * Function called by Aeron to deliver notification that the media driver has added a Publication successfully
 *
 * @param channel of the Publication
 * @param streamId within the channel of the Publication
 * @param sessionId of the Publication
 * @param correlationId used by the Publication for adding. Aka the registrationId returned by Aeron::addPublication
 */
typedef std::function<void(
    const std::string& channel,
    std::int32_t streamId,
    std::int32_t sessionId,
    std::int64_t correlationId)> on_new_publication_t;

/**
 * Function called by Aeron to deliver notification that the media driver has added a Subscription successfully
 *
 * @param channel of the Subscription
 * @param streamId within the channel of the Subscription
 * @param correlationId used by the Subscription for adding. Aka the registrationId returned by Aeron::addSubscription
 */
typedef std::function<void(
    const std::string& channel,
    std::int32_t streamId,
    std::int64_t correlationId)> on_new_subscription_t;

const static long NULL_TIMEOUT = -1;
const static long DEFAULT_MEDIA_DRIVER_TIMEOUT_MS = 10000;
const static long DEFAULT_RESOURCE_LINGER_MS = 5000;

/**
 * The Default handler for Aeron runtime exceptions.
 * When a DriverTimeoutException is encountered, this handler will exit the program.
 *
 * The error handler can be overridden by supplying an {@link Context} with a custom handler.
 *
 * @see Context#errorHandler
 */
inline void defaultErrorHandler(const std::exception& exception)
{
    std::cerr << "ERROR: " << exception.what();

    try
    {
        const SourcedException& sourcedException = dynamic_cast<const SourcedException&>(exception);
        std::cerr << " : " << sourcedException.where();
    }
    catch (std::bad_cast)
    {
        // ignore
    }

    std::cerr << std::endl;
    ::exit(-1);
}

inline void defaultOnNewPublicationHandler(const std::string&, std::int32_t, std::int32_t, std::int64_t)
{
}

inline void defaultOnAvailableImageHandler(Image &)
{
}

inline void defaultOnNewSubscriptionHandler(const std::string&, std::int32_t, std::int64_t)
{
}

inline void defaultOnUnavailableImageHandler(Image &)
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

    /// @cond HIDDEN_SYMBOLS
    this_t& conclude()
    {
        if (NULL_TIMEOUT == m_mediaDriverTimeout)
        {
            m_mediaDriverTimeout = DEFAULT_MEDIA_DRIVER_TIMEOUT_MS;
        }

        if (NULL_TIMEOUT == m_resourceLingerTimeout)
        {
            m_resourceLingerTimeout = DEFAULT_RESOURCE_LINGER_MS;
        }

        return *this;
    }
    /// @endcond

    /**
     * Set the directory that the Aeron client will use to communicate with the media driver
     *
     * @param directory to use
     * @return reference to this Context instance
     */
    inline this_t& aeronDir(const std::string &directory)
    {
        m_dirName = directory;
        return *this;
    }

    /**
     * Return the path to the CnC file used by the Aeron client for communication with the media driver
     *
     * @return path of the CnC file
     */
    inline const std::string cncFileName()
    {
        return m_dirName + "/" + CncFileDescriptor::CNC_FILE;
    }

    /**
     * Set the handler for exceptions from the Aeron client
     *
     * @param handler called when exceptions arise
     * @return reference to this Context instance
     *
     * @see defaultErrorHandler for how the default behavior is handled
     */
    inline this_t& errorHandler(const exception_handler_t& handler)
    {
        m_exceptionHandler = handler;
        return *this;
    }

    /**
     * Set the handler for successful Aeron::addPublication notifications
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    inline this_t& newPublicationHandler(const on_new_publication_t& handler)
    {
        m_onNewPublicationHandler = handler;
        return *this;
    }

    /**
     * Set the handler for successful Aeron::addSubscription notifications
     *
     * @param handler called when add is completed successfully
     * @return reference to this Context instance
     */
    inline this_t& newSubscriptionHandler(const on_new_subscription_t& handler)
    {
        m_onNewSubscriptionHandler = handler;
        return *this;
    }

    /**
     * Set the handler for available image notifications
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t& availableImageHandler(const on_available_image_t &handler)
    {
        m_onAvailableImageHandler = handler;
        return *this;
    }

    /**
     * Set the handler for inactive image notifications
     *
     * @param handler called when event occurs
     * @return reference to this Context instance
     */
    inline this_t& unavailableImageHandler(const on_unavailable_image_t &handler)
    {
        m_onUnavailableImageHandler = handler;
        return *this;
    }

    /**
     * Set the amount of time, in milliseconds, that this client will wait until it determines the
     * Media Driver is unavailable. When this happens a
     * DriverTimeoutException will be generated for the error handler.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     * @see errorHandler
     */
    inline this_t& mediaDriverTimeout(long value)
    {
        m_mediaDriverTimeout = value;
        return *this;
    }

    /**
     * Set the amount of time, in milliseconds, that this client will to linger inactive connections and internal
     * arrays before they are free'd.
     *
     * @param value Number of milliseconds.
     * @return reference to this Context instance
     */
    inline this_t& resourceLingerTimeout(long value)
    {
        m_resourceLingerTimeout = value;
        return *this;
    }

    /**
     * Set whether to use an invoker to control the conductor agent or spawn a thread.
     *
     * @param useConductorAgentInvoker to use an invoker or not.
     * @return reference to this Context instance
     */
    inline this_t& useConductorAgentInvoker(bool useConductorAgentInvoker)
    {
        m_useConductorAgentInvoker = useConductorAgentInvoker;
        return *this;
    }

    inline static std::string tmpDir()
    {
#if defined(_MSC_VER)
        static char buff[MAX_PATH+1];
        std::string dir = "";

        if (::GetTempPath(MAX_PATH, &buff[0]) > 0)
        {
            dir = buff;
        }

        return dir;
#else
        std::string dir = "/tmp";

        if (::getenv("TMPDIR"))
        {
            dir = ::getenv("TMPDIR");
        }

        return dir;
#endif
    }

    inline static std::string getUserName()
    {
        const char *username = ::getenv("USER");
#if (_MSC_VER)
        if (nullptr == username)
        {
            username = ::getenv("USERNAME");
            if (nullptr == username)
            {
                 username = "default";
            }
        }
#else
        if (nullptr == username)
        {
            username = "default";
        }
#endif
        return username;
    }

    inline static std::string defaultAeronPath()
    {
#if defined(__linux__)
        return "/dev/shm/aeron-" + getUserName();
#elif (_MSC_VER)
        return tmpDir() + "/aeron-" + getUserName();
#else
        return tmpDir() + "/aeron-" + getUserName();
#endif
    }

private:
    std::string m_dirName = defaultAeronPath();
    exception_handler_t m_exceptionHandler = defaultErrorHandler;
    on_new_publication_t m_onNewPublicationHandler = defaultOnNewPublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler = defaultOnNewSubscriptionHandler;
    on_available_image_t m_onAvailableImageHandler = defaultOnAvailableImageHandler;
    on_unavailable_image_t m_onUnavailableImageHandler = defaultOnUnavailableImageHandler;
    long m_mediaDriverTimeout = NULL_TIMEOUT;
    long m_resourceLingerTimeout = NULL_TIMEOUT;
    bool m_useConductorAgentInvoker = false;
};

}

#endif
