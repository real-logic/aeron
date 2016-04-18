//
// Created by barkerm on 14/04/16.
//

#ifndef AERON_SYSTEMCOUNTERS_H
#define AERON_SYSTEMCOUNTERS_H

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

            counts[i++] = descriptor.newCounter(counterManager);
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
