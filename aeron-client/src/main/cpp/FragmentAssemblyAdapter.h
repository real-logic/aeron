/*
 * Copyright 2015 Real Logic Ltd.
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

#ifndef AERON_FRAGMENTASSEMBLYADAPTER_H
#define AERON_FRAGMENTASSEMBLYADAPTER_H

#include "Aeron.h"

namespace aeron {

class FragmentAssemblyAdapter
{
public:
    FragmentAssemblyAdapter(const fragment_handler_t& delegate) :
        m_delegate(delegate)
    {
    }

    inline void onFragment(AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
    {
        const std::uint8_t flags = header.flags();

        if ((flags & FrameDescriptor::UNFRAGMENTED) == FrameDescriptor::UNFRAGMENTED)
        {
            m_delegate(buffer, offset, length, header);
        }
        else
        {

        }
    }

    fragment_handler_t handler()
    {
        return [&](AtomicBuffer& buffer, util::index_t offset, util::index_t length, Header& header)
        {
            this->onFragment(buffer, offset, length, header);
        };
    }

private:
    fragment_handler_t m_delegate;
};

}

#endif //AERON_FRAGMENTASSEMBLYADAPTER_H
