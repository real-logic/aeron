/*
 * Copyright 2014 Real Logic Ltd.
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

#include <util/Exceptions.h>
#include <common/AgentRunner.h>
#include <concurrent/ringbuffer/ManyToOneRingBuffer.h>
#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <iostream>

namespace aeron {

using namespace aeron::common::common;
using namespace aeron::common::concurrent::ringbuffer;
using namespace aeron::common::concurrent::broadcast;

inline static void defaultErrorHandler(util::SourcedException& exception)
{
    std::cerr << "ERROR: " << exception.what() << " : " << exception.where() << std::endl;
    ::exit(-1);
}

class Context
{
    friend class Aeron;
public:
    typedef Context this_t;

    this_t& conclude()
    {
        return *this;
    }

    this_t& useSharedMemoryOnLinux()
    {
#if defined(__linux__)
        m_dataDirName = "/dev/shm/aeron/data";
        m_adminDirName = "/dev/shm/aeron/conductor";
        m_countersDirName = "/dev/shm/aeron/counters";
#endif
        return *this;
    }

    inline this_t& toDriverBuffer(std::unique_ptr<ManyToOneRingBuffer> toDriverBuffer)
    {
        m_toDriverBuffer = std::move(toDriverBuffer);
        return *this;
    }

    inline std::unique_ptr<ManyToOneRingBuffer> toDriverBuffer()
    {
        return std::move(m_toDriverBuffer);
    }

    inline this_t& toClientsBuffer(std::unique_ptr<CopyBroadcastReceiver> toClientsBuffer)
    {
        m_toClientsBuffer = std::move(toClientsBuffer);
        return *this;
    }

    inline std::unique_ptr<CopyBroadcastReceiver> toClientsBuffer()
    {
        return std::move(m_toClientsBuffer);
    }

    inline void dataDirName(const std::string& name)
    {
        m_dataDirName = name;
    }

    inline const std::string& dataDirName() const
    {
        return m_dataDirName;
    }

    inline void adminDirName(const std::string& name)
    {
        m_adminDirName = name;
    }

    inline const std::string& adminDirName() const
    {
        return m_adminDirName;
    }

    inline void countersDirName(const std::string& name)
    {
        m_countersDirName = name;
    }

    inline const std::string& countersDirName() const
    {
        return m_countersDirName;
    }

    inline const std::string& toDriverFileName()
    {
        m_toDriverFileName = m_adminDirName + "/" + "to-driver";
        return m_toDriverFileName;
    }

    inline const std::string& toClientsFileName()
    {
        m_toClientsFileName = m_adminDirName + "/" + "to-clients";
        return m_toClientsFileName;
    }

private:
    std::unique_ptr<ManyToOneRingBuffer> m_toDriverBuffer;
    std::unique_ptr<CopyBroadcastReceiver> m_toClientsBuffer;
    std::string m_dataDirName = "";
    std::string m_adminDirName = "";
    std::string m_countersDirName = "";
    std::string m_toDriverFileName = "";
    std::string m_toClientsFileName = "";
    exception_handler_t m_exceptionHandler = defaultErrorHandler;
};

}

#endif