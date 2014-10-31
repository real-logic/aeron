#ifndef INCLUDED_AERON_COMMAND_REMOVEMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_REMOVEMESSAGEFLYWEIGHT__

#include <cstdint>
#include <string>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace common { namespace command {

/**
* Control message for removing a Publication or Subscription.
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                            Client ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                    Command Correlation ID                     |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Registration ID                       |
* |                                                               |
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct RemoveMessageDefn
{
    CorrelatedMessageDefn correlatedMessage;
	std::int64_t registrationId;
};
#pragma pack(pop)

class RemoveMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
	inline RemoveMessageFlyweight(concurrent::AtomicBuffer& buffer, size_t offset)
		: CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<RemoveMessageDefn>(0))
	{
	}

	inline std::int64_t registrationId() const
	{
		return m_struct.registrationId;
	}

	inline CorrelatedMessageFlyweight& registrationId(std::int64_t value)
	{
		m_struct.registrationId = value;
		return *this;
	}

	inline static std::int32_t length()
	{
		return (std::int32_t)sizeof(RemoveMessageDefn);
	}

private:
	RemoveMessageDefn& m_struct;
};

}}};
#endif