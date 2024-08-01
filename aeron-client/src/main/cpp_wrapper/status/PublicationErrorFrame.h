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
        m_errorValues(errorValues)
    {
    }

private:
    aeron_publication_error_values_t *m_errorValues;
};



}}

#endif //AERON_PUBLICATIONERRORFRAME_H
