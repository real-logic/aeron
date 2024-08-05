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

#ifndef AERON_PUBLICATIONERRORFRAME_H
#define AERON_PUBLICATIONERRORFRAME_H

#include "aeronc.h"

namespace aeron { namespace status {

class PublicationErrorFrame
{
public:
    /**
     * Constructs from a supplied C pointer to the aeron_publication_error_values_t and wraps over the top of it.
     * By default it won't manage the underlying memory of the C structure.
     *
     * @param errorValues C structure holding the actual data.
     * @param ownsErrorValuesPtr to indicate if the destructor of this class should free the underlying C memory.
     */
    PublicationErrorFrame(aeron_publication_error_values_t *errorValues, bool ownsErrorValuesPtr = false) :
        m_errorValues(errorValues), m_ownsErrorValuesPtr(ownsErrorValuesPtr)
    {
    }

    PublicationErrorFrame(PublicationErrorFrame &other)
    {
        if (aeron_publication_error_values_copy(&this->m_errorValues, other.m_errorValues) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    PublicationErrorFrame& operator=(PublicationErrorFrame& other)
    {
        if (aeron_publication_error_values_copy(&this->m_errorValues, other.m_errorValues) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_ownsErrorValuesPtr = true;
        return *this;
    }

    PublicationErrorFrame(PublicationErrorFrame &&other)
        : m_errorValues(other.m_errorValues), m_ownsErrorValuesPtr(other.m_ownsErrorValuesPtr)
    {
        other.m_errorValues = nullptr;
        other.m_ownsErrorValuesPtr = false;
    }

    ~PublicationErrorFrame()
    {
        if (m_ownsErrorValuesPtr)
        {
            aeron_publication_error_values_delete(this->m_errorValues);
        }
    }

    std::int64_t registrationId()
    {
        return m_errorValues->registration_id;
    }

    std::int32_t sessionId()
    {
        return m_errorValues->session_id;
    }

    std::int32_t streamId()
    {
        return m_errorValues->stream_id;
    }

    std::int64_t groupTag()
    {
        return m_errorValues->group_tag;
    }

    std::uint16_t sourcePort()
    {
        return m_errorValues->source_port;
    }

    std::uint8_t* sourceAddress()
    {
        return m_errorValues->source_address;
    }

    std::int16_t sourceAddressType()
    {
        return m_errorValues->address_type;
    }

    bool isValid()
    {
        return nullptr != m_errorValues;
    }

private:
    aeron_publication_error_values_t *m_errorValues;
    bool m_ownsErrorValuesPtr;
};

}}

#endif //AERON_PUBLICATIONERRORFRAME_H
