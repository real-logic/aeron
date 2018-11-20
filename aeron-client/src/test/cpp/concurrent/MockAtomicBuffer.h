/*
 * Copyright 2014-2018 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_MOCKS__
#define INCLUDED_AERON_DRIVER_MOCKS__

#include <gmock/gmock.h>

#define COND_MOCK 1

#include <concurrent/AtomicBuffer.h>

namespace aeron { namespace concurrent { namespace mock {

class MockAtomicBuffer : public AtomicBuffer
{
public:
    MockAtomicBuffer(std::uint8_t *buffer, size_t length);
    virtual ~MockAtomicBuffer();

    MOCK_METHOD2(putUInt8, void(util::index_t offset, std::uint8_t v));
    MOCK_METHOD2(putUInt16, void(util::index_t offset, std::uint16_t v));
    MOCK_METHOD2(putInt16, void(util::index_t offset, std::int16_t v));
    MOCK_METHOD2(putInt32, void(util::index_t offset, std::int32_t v));
    MOCK_METHOD2(putInt64, void(util::index_t offset, std::int64_t v));
    MOCK_CONST_METHOD1(getInt32, std::int32_t(util::index_t offset));
    MOCK_CONST_METHOD1(getInt32Volatile, std::int32_t(util::index_t offset));
    MOCK_METHOD2(getAndAddInt32, std::int32_t(util::index_t offset, std::int32_t delta));
    MOCK_METHOD2(getAndAddInt64, std::int64_t(util::index_t offset, std::int64_t delta));
    MOCK_METHOD4(putBytes, void(util::index_t index, const concurrent::AtomicBuffer& srcBuffer, util::index_t srcIndex, util::index_t length));
    MOCK_METHOD3(putBytes, void(util::index_t index, const std::uint8_t *srcBuffer, util::index_t length));
    MOCK_METHOD2(putInt32Ordered, void(util::index_t offset, std::int32_t v));
    MOCK_CONST_METHOD1(getUInt16, std::uint16_t(util::index_t offset));
    MOCK_CONST_METHOD1(getInt16, std::int16_t(util::index_t offset));
    MOCK_CONST_METHOD1(getInt64, std::int64_t(util::index_t offset));
    MOCK_METHOD2(putInt64Ordered, void(util::index_t, std::int64_t v));
    MOCK_CONST_METHOD1(getInt64Volatile, std::int64_t(util::index_t));

    MOCK_METHOD2(putStringWithoutLength, std::int32_t(util::index_t, const std::string&));

    void useAsSpy()
    {
        ON_CALL(*this, putInt64(testing::_, testing::_))
            .WillByDefault(testing::Invoke(&m_realBuffer, &AtomicBuffer::putInt64));
        ON_CALL(*this, putInt32Ordered(testing::_, testing::_))
            .WillByDefault(testing::Invoke(&m_realBuffer, &AtomicBuffer::putInt32Ordered));
        ON_CALL(*this, getAndAddInt32(testing::_, testing::_))
            .WillByDefault(testing::Invoke(&m_realBuffer, &AtomicBuffer::getAndAddInt32));
        ON_CALL(*this, putInt64Ordered(testing::_, testing::_))
            .WillByDefault(testing::Invoke(&m_realBuffer, &AtomicBuffer::putInt64Ordered));
        ON_CALL(*this, putStringWithoutLength(testing::_, testing::_))
            .WillByDefault(testing::Invoke(&m_realBuffer, &AtomicBuffer::putStringWithoutLength));
    }

private:
    AtomicBuffer m_realBuffer;
};

}}}

#endif
