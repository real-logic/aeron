#ifndef INCLUDED_AERON_COMMAND_SUBSCRIPTIONMESSAGEFLYWEIGHT__
#define INCLUDED_AERON_COMMAND_SUBSCRIPTIONMESSAGEFLYWEIGHT__

#include <cstdint>
#include <string>
#include "CorrelatedMessageFlyweight.h"

namespace aeron { namespace common { namespace command {

/**
* Control message for adding or removing a subscription.
*
* <p>
* 0                   1                   2                   3
* 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
* +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
* |                            Client ID                          |
* |                                                               |
* +---------------------------------------------------------------+
* |                         Correlation ID                        |
* |                                                               |
* +---------------------------------------------------------------+
* |                  Registration Correlation ID                  |
* |                                                               |
* +---------------------------------------------------------------+
* |                           Stream Id                           |
* +---------------------------------------------------------------+
* |      Channel Length         |   Channel                     ...
* |                                                             ...
* +---------------------------------------------------------------+
*/

#pragma pack(push)
#pragma pack(4)
struct SubscriptionMessageDefn : public CorrelatedMessageDefn
{
	std::int64_t registrationCorrelationId;
	std::int32_t streamId;
	struct
	{
		std::int32_t channelLength;
		std::int8_t  channelData[1];
	} channel;
};
#pragma pack(pop)

class SubscriptionMessageFlyweight : public CorrelatedMessageFlyweight
{
public:
	typedef SubscriptionMessageFlyweight this_t;

	inline SubscriptionMessageFlyweight(concurrent::AtomicBuffer& buffer, size_t offset)
		: CorrelatedMessageFlyweight(buffer, offset), m_struct(overlayStruct<SubscriptionMessageDefn>(0))
	{
	}

	inline std::int64_t registrationCorrelationId() const
	{
		return m_struct.registrationCorrelationId;
	}

	inline this_t& registrationCorrelationId(std::int64_t value)
	{
		m_struct.registrationCorrelationId = value;
		return *this;
	}

	inline std::int32_t streamId() const
	{
		return m_struct.streamId;
	}

	inline this_t& streamId(std::int32_t value)
	{
		m_struct.streamId = value;
		return *this;
	}

	inline std::string channel()
	{
		return stringGet(offsetof(ConnectionMessageDefn, channel));
	}

	inline this_t& channel(const std::string& value)
	{
		stringPut(offsetof(ConnectionMessageDefn, channel), value);
		return *this;
	}

	std::size_t length()
	{
		return offsetof(ConnectionMessageDefn, channel.channelData) + m_struct.channel.channelLength;
	}

private:
	SubscriptionMessageDefn& m_struct;
};

}}};
#endif