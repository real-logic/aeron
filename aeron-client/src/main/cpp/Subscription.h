/*
 * Copyright 2014-2025 Real Logic Limited.
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

#ifndef AERON_SUBSCRIPTION_H
#define AERON_SUBSCRIPTION_H

#include <cstdint>
#include <memory>
#include <iterator>
#include "concurrent/AtomicArrayUpdater.h"
#include "concurrent/status/StatusIndicatorReader.h"
#include "Image.h"
#include "util/Export.h"

namespace aeron
{

using namespace aeron::concurrent::logbuffer;

class ClientConductor;

/**
 * Aeron Subscriber API for receiving messages from publishers on a given channel and streamId pair.
 * Subscribers are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link fragment_handler_t}.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an applications responsibility to {@link #poll} the Subscriber for new messages.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @see FragmentAssembler
 */
class CLIENT_EXPORT Subscription
{
public:
    /// @cond HIDDEN_SYMBOLS
    Subscription(
        ClientConductor &conductor,
        std::int64_t registrationId,
        const std::string &channel,
        std::int32_t streamId,
        std::int32_t channelStatusId);

    /// @endcond
    ~Subscription();

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    inline const std::string &channel() const
    {
        return m_channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    inline std::int32_t streamId() const
    {
        return m_streamId;
    }

    /**
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    inline std::int32_t channelStatusId() const
    {
        return m_channelStatusId;
    }

    /**
     * Get the status for the channel of this {@link Subscription}
     *
     * @return status code for this channel
     */
    std::int64_t channelStatus() const;

    /**
     * Fetches the local socket addresses for this subscription. If the channel is not
     * {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then this will return an
     * empty list.
     *
     * The format is as follows:
     * <br>
     * <br>
     * IPv4: <code>ip address:port</code>
     * <br>
     * IPv6: <code>[ip6 address]:port</code>
     * <br>
     * <br>
     * This is to match the formatting used in the Aeron URI
     *
     * @return local socket address for this subscription.
     * @see #channelStatus()
     */
    std::vector<std::string> localSocketAddresses() const;

    /**
     * Resolve channel endpoint and replace it with the port from the ephemeral range when 0 was provided. If there are
     * no addresses, or if there is more than one, returned from {@link #localSocketAddresses()} then the original
     * {@link #channel()} is returned.
     * <p>
     * If the channel is not {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then an
     * empty string will be returned.
     *
     * @return channel URI string with an endpoint being resolved to the allocated port.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    std::string tryResolveChannelEndpointPort() const;

    /**
     * Find the resolved endpoint for the channel. This may be null of MDS is used and no destination is yet added.
     * The result will similar to taking the first element returned from {@link #localSocketAddresses()}. If more than
     * one destination is added then the first found is returned.
     * <p>
     * If the channel is not {@link aeron::concurrent::status::ChannelEndpointStatus::CHANNEL_ENDPOINT_ACTIVE}, then an
     * empty string will be returned.
     *
     * @return The resolved endpoint or an empty string if not found.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    std::string resolvedEndpoint() const;

    /**
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add.
     * @return correlation id for the add command.
     */
    std::int64_t addDestination(const std::string &endpointChannel);

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove.
     * @return correlation id for the remove command.
     */
    std::int64_t removeDestination(const std::string &endpointChannel);

    /**
     * Retrieve the status of the associated add or remove destination operation with the given correlationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the correlationId is unknown, then an exception is thrown.
     * - If the media driver has not answered the add/remove command, then a false is returned.
     * - If the media driver has successfully added or removed the destination then true is returned.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Subscription::addDestination
     * @see Subscription::removeDestination
     *
     * @param correlationId of the add/remove command returned by Subscription::addDestination
     * or Subscription::removeDestination.
     * @return true for added or false if not.
     */
    bool findDestinationResponse(std::int64_t correlationId);

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received.
     *
     * @see fragment_handler_t
     */
    template<typename F>
    inline int poll(F &&fragmentHandler, int fragmentLimit)
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageArray[i]->poll(std::forward<F>(fragmentHandler), fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageArray[i]->poll(std::forward<F>(fragmentHandler), fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll in a controlled manner the Image s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use controlled_poll_fragment_handler_t.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received.
     * @see controlled_poll_fragment_handler_t
     */
    template<typename F>
    inline int controlledPoll(F &&fragmentHandler, int fragmentLimit)
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageArray[i]->controlledPoll(std::forward<F>(fragmentHandler), fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageArray[i]->controlledPoll(std::forward<F>(fragmentHandler), fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param blockHandler     to receive a block of fragments from each Image.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     */
    template<typename F>
    inline long blockPoll(F &&blockHandler, int blockLengthLimit)
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        long bytesConsumed = 0;

        for (std::size_t i = 0; i < length; i++)
        {
            bytesConsumed += imageArray[i]->blockPoll(std::forward<F>(blockHandler), blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Is the subscription connected by having at least one open image available.
     *
     * @return true if the subscription has more than one open image available.
     */
    inline bool isConnected() const
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;

        for (std::size_t i = 0; i < length; i++)
        {
            if (!imageArray[i]->isClosed())
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Count of images associated with this subscription.
     *
     * @return count of images associated with this subscription.
     */
    inline int imageCount() const
    {
        return static_cast<int>(m_imageArray.load().second);
    }

    /**
     * Return the {@link Image} associated with the given sessionId.
     *
     * This method returns a share_ptr to the underlying Image and must be released before the Image may be fully
     * reclaimed.
     *
     * @param sessionId associated with the Image.
     * @return Image associated with the given sessionId or nullptr if no Image exist.
     */
    inline std::shared_ptr<Image> imageBySessionId(std::int32_t sessionId) const
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        int index = -1;

        for (std::size_t i = 0; i < length; i++)
        {
            if (imageArray[i]->sessionId() == sessionId)
            {
                index = static_cast<int>(i);
                break;
            }
        }

        return -1 != index ? imageArray[index] : std::shared_ptr<Image>();
    }

    /**
     * Return the {@link Image} associated with the given index.
     *
     * This method returns a share_ptr to the underlying Image and must be released before the Image may be fully
     * reclaimed.
     *
     * @param index in the array.
     * @return image at given index or exception if out of range.
     */
    inline std::shared_ptr<Image> imageByIndex(std::size_t index) const
    {
        return m_imageArray.load().first[index];
    }

    /**
     * Get the image at the given index from the images array.
     *
     * This is only valid until the image becomes unavailable. This is only provided for backwards compatibility and
     * usage should be replaced with Subscription::imageByIndex instead so that the Image is retained easier.
     *
     * @param index in the array.
     * @return image at given index or exception if out of range.
     * @deprecated use Subscription::imageByIndex instead.
     */
    inline Image &imageAtIndex(std::size_t index) const
    {
        return *m_imageArray.load().first[index];
    }

    /**
     * Get a std::vector of active {@link Image}s that match this subscription.
     *
     * This method will create copies of each Image that are only valid until the Image becomes unavailable. This is
     * only provided for backwards compatibility and usage should be replaced with Subscription::copyOfImageList
     * instead so that the Images are retained easier.
     *
     * @return a std::vector of active {@link Image}s that match this subscription.
     * @deprecated use Subscription::copyOfImageList instead.
     */
    inline std::shared_ptr<std::vector<Image>> images() const
    {
        std::shared_ptr<std::vector<Image>> result(new std::vector<Image>());

        forEachImage(
            [&](Image &image)
            {
                result->push_back(Image(image));
            });

        return result;
    }

    /**
     * Get a std::vector of active std::shared_ptr of {@link Image}s that match this subscription.
     *
     * THis method will create a new std::vector<std::shared_ptr<Image>> populated with the underlying {@link Image}s.
     *
     * @return a std::vector of active std::shared_ptr of {@link Image}s that match this subscription.
     */
    inline std::shared_ptr<std::vector<std::shared_ptr<Image>>> copyOfImageList() const
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        std::shared_ptr<std::vector<std::shared_ptr<Image>>> result(new std::vector<std::shared_ptr<Image>>);

        result->reserve(length);

        for (std::size_t i = 0; i < length; i++)
        {
            result->push_back(imageArray[i]);
        }

        return result;
    }

    /**
     * Iterate over Image list and call passed in function.
     *
     * @return length of Image list.
     */
    template<typename F>
    inline int forEachImage(F &&func) const
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;

        for (std::size_t i = 0; i < length; i++)
        {
            func(*imageArray[i]);
        }

        return static_cast<int>(length);
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return m_isClosed.load(std::memory_order_acquire);
    }

    /// @cond HIDDEN_SYMBOLS
    bool hasImage(std::int64_t correlationId) const
    {
        auto imageArrayPair = m_imageArray.load();
        auto imageArray = imageArrayPair.first;
        const std::size_t length = imageArrayPair.second;
        bool hasImage = false;

        for (std::size_t i = 0; i < length; i++)
        {
            if (imageArray[i]->correlationId() == correlationId)
            {
                hasImage = true;
                break;
            }
        }

        return hasImage;
    }

    Image::array_t addImage(std::shared_ptr<Image> image)
    {
        return m_imageArray.addElement(std::move(image)).first;
    }

    std::pair<Image::array_t, std::size_t> removeImage(std::int64_t correlationId)
    {
        auto result = m_imageArray.removeElement(
            [&](const std::shared_ptr<Image> &image)
            {
                if (image->correlationId() == correlationId)
                {
                    image->close();
                    return true;
                }

                return false;
            });

        return result;
    }

    std::pair<Image::array_t, std::size_t> closeAndRemoveImages()
    {
        if (!m_isClosed.exchange(true))
        {
            return m_imageArray.store(new std::shared_ptr<Image>[0], 0);
        }
        else
        {
            return { nullptr, 0 };
        }
    }
    /// @endcond

private:
    ClientConductor &m_conductor;
    const std::string m_channel;
    std::int32_t m_channelStatusId;
    std::int32_t m_streamId;
    std::int64_t m_registrationId;
    std::atomic<bool> m_isClosed = { false };
    AtomicArrayUpdater<std::shared_ptr<Image>> m_imageArray = {};
    char m_paddingBefore[util::BitUtil::CACHE_LINE_LENGTH] = {};
    std::size_t m_roundRobinIndex = 0;
    char m_paddingAfter[util::BitUtil::CACHE_LINE_LENGTH] = {};
};

}

#endif
