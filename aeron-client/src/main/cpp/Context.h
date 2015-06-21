/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
using namespace aeron::concurrent::broadcast;

typedef std::function<void(const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t joiningPosition, const std::string& sourceIdentity)> on_new_connection_t;
typedef std::function<void(const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t position)> on_inactive_connection_t;
typedef std::function<void(const std::string& channel, std::int32_t streamId, std::int32_t sessionId, std::int64_t correlationId)> on_new_publication_t;
typedef std::function<void(const std::string& channel, std::int32_t streamId, std::int64_t correlationId)> on_new_subscription_t;

const static long NULL_TIMEOUT = -1;
const static long DEFAULT_MEDIA_DRIVER_TIMEOUT_MS = 10000;
const static long DEFAULT_RESOURCE_LINGER_MS = 5000;

inline static void defaultErrorHandler(util::SourcedException& exception)
{
    std::cerr << "ERROR: " << exception.what() << " : " << exception.where() << std::endl;
    ::exit(-1);
}

inline static void defaultOnNewPublicationHandler(const std::string&, std::int32_t, std::int32_t, std::int64_t)
{
}

inline static void defaultOnNewConnectionHandler(const std::string&, std::int32_t, std::int32_t, std::int64_t, const std::string&)
{
}

inline static void defaultOnNewSubscriptionHandler(const std::string&, std::int32_t, std::int64_t)
{
}

inline static void defaultOnInactiveConnectionHandler(const std::string&, std::int32_t, std::int32_t, std::int64_t)
{
}

class Context
{
    friend class Aeron;
public:
    typedef Context this_t;

    this_t& conclude()
    {
        if (NULL_TIMEOUT == m_mediaDriverTimeout)
        {
            m_mediaDriverTimeout = DEFAULT_MEDIA_DRIVER_TIMEOUT_MS;
        }

        return *this;
    }

    inline this_t& aeronDir(const std::string &base)
    {
        m_dirName = base;
        return *this;
    }

    inline const std::string cncFileName()
    {
        return m_dirName + "/" + CncFileDescriptor::CNC_FILE;
    }

    inline this_t& newPublicationHandler(const on_new_publication_t& handler)
    {
        m_onNewPublicationHandler = handler;
        return *this;
    }

    inline this_t& newSubscriptionHandler(const on_new_subscription_t& handler)
    {
        m_onNewSubscriptionHandler = handler;
        return *this;
    }

    inline this_t& newConnectionHandler(const on_new_connection_t& handler)
    {
        m_onNewConnectionHandler = handler;
        return *this;
    }

    inline this_t& inactiveConnectionHandler(const on_inactive_connection_t& handler)
    {
        m_onInactiveConnectionHandler = handler;
        return *this;
    }

    inline this_t& mediaDriverTimeout(long value)
    {
        m_mediaDriverTimeout = value;
        return *this;
    }

    inline this_t& resourceLingerTimeout(long value)
    {
        m_resourceLingerTimeout = value;
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

    inline static std::string defaultAeronPath()
    {
#if defined(__linux__)
        return "/dev/shm/aeron";
#else
        return tmpDir() + "/aeron";
#endif
    }

private:
    std::string m_dirName = defaultAeronPath();
    exception_handler_t m_exceptionHandler = defaultErrorHandler;
    on_new_publication_t m_onNewPublicationHandler = defaultOnNewPublicationHandler;
    on_new_subscription_t m_onNewSubscriptionHandler = defaultOnNewSubscriptionHandler;
    on_new_connection_t m_onNewConnectionHandler = defaultOnNewConnectionHandler;
    on_inactive_connection_t m_onInactiveConnectionHandler = defaultOnInactiveConnectionHandler;
    long m_mediaDriverTimeout = DEFAULT_MEDIA_DRIVER_TIMEOUT_MS;
    long m_resourceLingerTimeout = DEFAULT_RESOURCE_LINGER_MS;
};

}

#endif