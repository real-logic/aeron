//
// Created by mike on 31/07/24.
//

#ifndef AERON_PUBLICATIONERRORFRAME_H
#define AERON_PUBLICATIONERRORFRAME_H

#include "aeronc.h"

namespace aeron { namespace status {

class PublicationErrorFrame
{
public:
    PublicationErrorFrame(aeron_publication_error_values_t *errorValues) :
        m_errorValues(errorValues), m_ownsErrorValuesPtr(false)
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
