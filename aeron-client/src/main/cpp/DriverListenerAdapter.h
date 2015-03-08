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

#ifndef INCLUDED_AERON_DRIVER_LISTENER_ADAPTER__
#define INCLUDED_AERON_DRIVER_LISTENER_ADAPTER__

#include <concurrent/broadcast/CopyBroadcastReceiver.h>
#include <command/ControlProtocolEvents.h>
#include <command/PublicationBuffersReadyFlyweight.h>

namespace aeron {

using namespace aeron::common;
using namespace aeron::common::command;
using namespace aeron::common::concurrent;
using namespace aeron::common::concurrent::broadcast;

template <class DriverListener>
class DriverListenerAdapter
{
public:
    DriverListenerAdapter(CopyBroadcastReceiver& broadcastReceiver, DriverListener& driverListener) :
        m_broadcastReceiver(broadcastReceiver),
        m_driverListener(driverListener)
    {
    }

    int receiveMessages()
    {
        return m_broadcastReceiver.receive(
            [&](std::int32_t msgTypeId, AtomicBuffer& buffer, util::index_t offset, util::index_t length)
            {
                switch (msgTypeId)
                {
                    case ControlProtocolEvents::ON_PUBLICATION_READY:
                    {
                        const PublicationBuffersReadyFlyweight publicationReady(buffer, offset);

                        m_driverListener.onNewPublication(
                            publicationReady.channel(),
                            publicationReady.streamId(),
                            publicationReady.sessionId(),
                            publicationReady.positionIndicatorOffset(),
                            publicationReady.mtuLength(),
                            publicationReady.logFileName(),
                            publicationReady.correlationId());
                    };

                    case ControlProtocolEvents::ON_CONNECTION_READY:
                    {

                    };

                    case ControlProtocolEvents::ON_OPERATION_SUCCESS:
                    {

                    };

                    case ControlProtocolEvents::ON_ERROR:
                    {

                    };

                    default:
                        break;
                }
            });
    }

private:
    CopyBroadcastReceiver& m_broadcastReceiver;
    DriverListener& m_driverListener;
};

}

#endif