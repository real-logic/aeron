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

#ifndef INCLUDED_AERON_SUBSCRIPTION__
#define INCLUDED_AERON_SUBSCRIPTION__

#include <cstdint>
#include <iostream>
#include <atomic>
#include <concurrent/logbuffer/TermReader.h>
#include "Connection.h"

namespace aeron {

using namespace aeron::concurrent::logbuffer;

class ClientConductor;

class Subscription
{
public:
    Subscription(
        ClientConductor& conductor, std::int64_t registrationId, const std::string& channel, std::int32_t streamId);
    virtual ~Subscription();

    inline const std::string& channel() const
    {
        return m_channel;
    }

    inline std::int32_t streamId() const
    {
        return m_streamId;
    }

    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    inline int poll(const fragment_handler_t fragmentHandler, int fragmentLimit)
    {
        int fragmentsRead = 0;
        int length = std::atomic_load(&m_connectionsLength);
        Connection* connections = std::atomic_load(&m_connections);

        if (length > 0)
        {
            int startingIndex = m_roundRobinIndex;
            if (startingIndex >= length)
            {
                m_roundRobinIndex = startingIndex = 0;
            }

            int i = startingIndex;

            do
            {
                fragmentsRead += connections[i].poll(fragmentHandler, fragmentLimit);

                if (++i == length)
                {
                    i = 0;
                }
            }
            while (fragmentsRead < fragmentLimit && i != startingIndex);
        }

        return fragmentsRead;
    }

    bool isConnected(std::int32_t sessionId)
    {
        Connection* connections = std::atomic_load(&m_connections);
        bool isConnected = false;

        for (int i = 0, length = std::atomic_load(&m_connectionsLength); i < length; i++)
        {
            if (connections[i].sessionId() == sessionId)
            {
                isConnected = true;
                break;
            }
        }

        return isConnected;
    }

    Connection* addConnection(Connection& connection)
    {
        Connection* oldArray = std::atomic_load(&m_connections);
        int length = std::atomic_load(&m_connectionsLength);
        Connection* newArray = new Connection[length + 1];

        for (int i = 0; i < length; i++)
        {
            newArray[i] = std::move(oldArray[i]);
        }

        newArray[length] = std::move(connection);

        std::atomic_store(&m_connections, newArray);
        std::atomic_store(&m_connectionsLength, length + 1);

        // oldArray to linger and be deleted by caller (aka client conductor)
        return oldArray;
    }

    Connection* removeConnection(std::int64_t correlationId)
    {
        Connection* oldArray = std::atomic_load(&m_connections);
        int length = std::atomic_load(&m_connectionsLength);
        int index = -1;

        for (int i = 0; i < length; i++)
        {
            if (oldArray[i].correlationId() == correlationId)
            {
                index = i;
                break;
            }
        }

        if (-1 != index)
        {
            Connection* newArray = new Connection[length - 1];

            for (int i = 0, j = 0; i < length; i++)
            {
                if (i != index)
                {
                    newArray[j++] = std::move(oldArray[i]);
                }
            }

            std::atomic_store(&m_connections, newArray);
            std::atomic_store(&m_connectionsLength, length - 1);
        }

        // oldArray to linger and be deleted by caller (aka client conductor)
        return (-1 != index) ? oldArray : nullptr;
    }

private:
    ClientConductor& m_conductor;
    const std::string m_channel;
    int m_roundRobinIndex = 0;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;

    std::atomic<Connection*> m_connections;
    std::atomic<int> m_connectionsLength;
};

}

#endif