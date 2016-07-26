/*
 * Copyright 2016 Real Logic Ltd.
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

#ifndef AERON_SYSTEMCOUNTERS_H
#define AERON_SYSTEMCOUNTERS_H

#include <unordered_map>

#include <concurrent/AtomicCounter.h>
#include <concurrent/CountersManager.h>
#include <util/Exceptions.h>
#include "SystemCounterDescriptor.h"

namespace aeron { namespace driver { namespace status {

using namespace aeron::concurrent;

class SystemCounters
{
public:
    inline SystemCounters(CountersManager& counterManager)
    {
        std::int32_t i = 0;
        for (auto descriptor : SystemCounterDescriptor::VALUES)
        {
            if (i != descriptor.id())
            {
                throw util::IllegalStateException(
                    util::strPrintf(
                        "Invalid descriptor id for '%s': supplied=%d, expected=%d",
                        descriptor.label(), descriptor.id(), i),
                    SOURCEINFO);
            }

            counts[descriptor.id()] = descriptor.newCounter(counterManager);

            i++;
        }
    }

    virtual ~SystemCounters()
    {
        for (auto counter : counts)
        {
            delete counter;
        }
    }

    inline AtomicCounter* get(SystemCounterDescriptor descriptor)
    {
        return counts[descriptor.id()];
    }

private:
    AtomicCounter* counts[SystemCounterDescriptor::VALUES_SIZE];
    SystemCounters(const SystemCounters& srcMyClass);
    SystemCounters& operator=(const SystemCounters& srcMyClass);};

}}};

#endif //AERON_SYSTEMCOUNTERS_H
