/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_UDPCHANNEL_H_
#define INCLUDED_AERON_DRIVER_UDPCHANNEL_H_

#include <memory>
#include <iostream>
#include "util/Exceptions.h"
#include "InetAddress.h"
#include "InterfaceLookup.h"
#include "NetworkInterface.h"

namespace aeron { namespace driver { namespace media {

DECLARE_SOURCED_EXCEPTION (InvalidChannelException);

class UdpChannel
{
public:
    UdpChannel(
        std::unique_ptr<InetAddress>& remoteData,
        std::unique_ptr<InetAddress>& remoteControl,
        std::unique_ptr<NetworkInterface>& localData,
        bool isMulticast)
        : m_remoteControl(std::move(remoteControl)),
          m_remoteData(std::move(remoteData)),
          m_localData(std::move(localData)),
          m_isMulticast(isMulticast)
    {
    }

    const char* canonicalForm();

    inline bool isMulticast() const
    {
        return m_isMulticast;
    }

    inline InetAddress& remoteControl() const
    {
        if (m_remoteControl == nullptr)
        {
            return remoteData();
        }

        return *m_remoteControl;
    }

    inline InetAddress& remoteData() const
    {
        return *m_remoteData;
    }

    inline InetAddress& localData() const
    {
        return m_localData->address();
    }

    inline InetAddress& localControl() const
    {
        return m_localData->address();
    }

    inline NetworkInterface& localInterface() const
    {
        return *m_localData;
    }

    static std::unique_ptr<UdpChannel> parse(
        const char* uri, int familyHint = PF_INET, InterfaceLookup& lookup = BsdInterfaceLookup::get());

private:
    std::unique_ptr<InetAddress> m_remoteControl;
    std::unique_ptr<InetAddress> m_remoteData;
    std::unique_ptr<NetworkInterface> m_localData;
    bool m_isMulticast;
};


inline std::ostream& operator<<(std::ostream& os, const UdpChannel& dt)
{
    os << "LocalControl=" << dt.localControl()
        << ",LocalData=" << dt.localData()
        << ",RemoteControl=" << dt.remoteControl()
        << ",RemoteData=" << dt.remoteData();
    return os;
}

}}}

#endif // INCLUDED_AERON_DRIVER_UDPCHANNEL_H_
