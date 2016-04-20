#ifndef INCLUDED_AERON_DRIVER_MOCKS__
#define INCLUDED_AERON_DRIVER_MOCKS__

#define COND_MOCK 1

#include <gmock/gmock.h>

#include <media/ReceiveChannelEndpoint.h>

#include <PublicationImage.h>
#include <Receiver.h>
#include <DriverConductorProxy.h>
#include <FeedbackDelayGenerator.h>

namespace aeron { namespace driver { namespace media {

class MockReceiveChannelEndpoint : public ReceiveChannelEndpoint
{
public:
    MockReceiveChannelEndpoint(std::unique_ptr<UdpChannel>&& channel) : ReceiveChannelEndpoint(std::move(channel))
    { }

    virtual ~MockReceiveChannelEndpoint() = default;

    MOCK_METHOD0(pollForData, std::int32_t());
    MOCK_METHOD3(sendSetupElicitingStatusMessage, void(InetAddress &address, std::int32_t sessionId, std::int32_t streamId));
};

}}};

namespace aeron { namespace driver {

class MockPublicationImage : public PublicationImage
{
public:
    MockPublicationImage() : PublicationImage(
        0, 0, 0, 0, 0, 0, 0, 0, 0,
        std::unique_ptr<MappedRawLog>(nullptr),
        std::shared_ptr<InetAddress>(nullptr),
        std::shared_ptr<InetAddress>(nullptr),
        std::shared_ptr<ReceiveChannelEndpoint>(nullptr),
        std::unique_ptr<std::vector<ReadablePosition<UnsafeBufferPosition>>>(nullptr),
        std::unique_ptr<Position<UnsafeBufferPosition>>(nullptr),
        m_delayGenerator,
        mockCurrentTime
    ){}

    virtual ~MockPublicationImage() = default;

    MOCK_METHOD0(sessionId, std::int32_t());
    MOCK_METHOD0(streamId, std::int32_t());
    MOCK_METHOD4(insertPacket, std::int32_t(std::int32_t termId, std::int32_t termOffset, AtomicBuffer& buffer, std::int32_t length));
    MOCK_METHOD0(ifActiveGoInactive, void());
    MOCK_METHOD1(status, void(PublicationImageStatus status));

    static long mockCurrentTime()
    {
        return 0;
    }
private:
    StaticFeedbackDelayGenerator m_delayGenerator{0, true};
};

class MockReceiver : public Receiver
{
public:
    MockReceiver() : Receiver()
    {}

    virtual ~MockReceiver() = default;

    MOCK_METHOD3(addPendingSetupMessage, void(std::int32_t sessionId, std::int32_t streamId, ReceiveChannelEndpoint& receiveChannelEndpoint));
};

class MockDriverConductorProxy : public DriverConductorProxy
{
public:
    MOCK_METHOD10(createPublicationImage, void(std::int32_t sessionId, std::int32_t streamId, std::int32_t initialTermId, std::int32_t activeTermId, std::int32_t termOffset, std::int32_t termLength, std::int32_t mtuLength, InetAddress& controlAddress, InetAddress& srcAddress, ReceiveChannelEndpoint& channelEndpoint));
};

}};

#endif