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

#include "Aeron.h"

using namespace aeron;

Aeron::Aeron(Context& context) :
    m_conductor(createDriverProxy(context)),
    m_idleStrategy(),
    m_conductorRunner(m_conductor, m_idleStrategy, context.m_exceptionHandler),
    m_context(context)
{
    m_conductorRunner.start();
}

Aeron::~Aeron()
{
    m_conductorRunner.close();

    // TODO: do cleanup of anything created
}

DriverProxy& Aeron::createDriverProxy(const Context& context)
{
    ManyToOneRingBuffer* toDriverBuffer = context.toDriverBuffer();

    if (nullptr == toDriverBuffer)
    {
        // TODO: create buffer from mapped file for location specified in context & save for cleanup on delete

        // TODO: make this a function to take std::string and return new object holding
    }

    DriverProxy* driverProxy = new DriverProxy(*toDriverBuffer);  // TODO: save for cleanup on delete

    return *driverProxy;
}
