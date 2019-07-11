/*
 * Copyright 2014-2019 Real Logic Ltd.
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

#ifndef AERON_SUBSCRIPTION_H
#define AERON_SUBSCRIPTION_H

#include <cstdint>
#include <iostream>
#include <atomic>
#include <memory>
#include <iterator>
#include <concurrent/logbuffer/TermReader.h>
#include "concurrent/status/StatusIndicatorReader.h"
#include "Image.h"

namespace aeron {

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
class Subscription
{
public:
    /// @cond HIDDEN_SYMBOLS
    Subscription(
        ClientConductor& conductor,
        std::int64_t registrationId,
        const std::string& channel,
        std::int32_t streamId,
        std::int32_t channelStatusId);
    /// @endcond
    ~Subscription();

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    inline const std::string& channel() const
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
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add.
     */
    void addDestination(const std::string& endpointChannel);

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove.
     */
    void removeDestination(const std::string& endpointChannel);

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */
    template <typename F>
    inline int poll(F&& fragmentHandler, int fragmentLimit)
    {
        const Image::list_t& imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList.size();
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageList[i]->poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageList[i]->poll(fragmentHandler, fragmentLimit - fragmentsRead);
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
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    template <typename F>
    inline int controlledPoll(F&& fragmentHandler, int fragmentLimit)
    {
        const Image::list_t& imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList.size();
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageList[i]->controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += imageList[i]->controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
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
    template <typename F>
    inline long blockPoll(F&& blockHandler, int blockLengthLimit)
    {
        const Image::list_t &imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        long bytesConsumed = 0;

        for (const auto& image : imageList)
        {
            bytesConsumed += image->blockPoll(blockHandler, blockLengthLimit);
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
        const Image::list_t& imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);

        for (const auto& image : imageList)
        {
            if (!image->isClosed())
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
        return static_cast<int>(std::atomic_load_explicit(&m_imageList, std::memory_order_acquire)->size());
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
        const Image::list_t &imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList.size();
        int index = -1;

        for (std::size_t i = 0; i < length; i++)
        {
            if (imageList[i]->sessionId() == sessionId)
            {
                index = static_cast<int>(i);
                break;
            }
        }

        return index != -1 ? imageList[index] : std::shared_ptr<Image>();
    }

    /**
     * Return the {@link Image} associated with the given index.
     *
     * This method returns a share_ptr to the underlying Image and must be released before the Image may be fully
     * reclaimed.
     *
     * @param index in the array
     * @return image at given index or exception if out of range.
     */
    inline std::shared_ptr<Image> imageByIndex(size_t index) const
    {
        return std::atomic_load_explicit(&m_imageList, std::memory_order_acquire)->at(index);
    }

    /**
     * Get the image at the given index from the images array.
     *
     * This is only valid until the image becomes unavailable. This is only provided for backwards compatibility and
     * usage should be replaced with Subscription::imageByIndex instead so that the Image is retained easier.
     *
     * @param index in the array
     * @return image at given index or exception if out of range.
     */
    inline Image& imageAtIndex(size_t index) const
    {
        return *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire)->at(index);
    }

    /**
     * Get a std::vector of active {@link Image}s that match this subscription.
     *
     * This method will create copies of each Image that are only valid until the Image becomes unavailable. This is
     * only provided for backwards compatibility and usage should be replaced with Subscription::copyOfImageList
     * instead so that the Images are retained easier.
     *
     * @return a std::vector of active {@link Image}s that match this subscription.
     */
    inline std::shared_ptr<std::vector<Image>> images() const
    {
        std::shared_ptr<std::vector<Image>> result(new std::vector<Image>());

        forEachImage(
            [&](Image& image)
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
     * @return a std::vector of active std::shared_ptr of {@link Image}s that match this subscription
     */
    inline std::shared_ptr<Image::list_t> copyOfImageList() const
    {
        const Image::list_t &imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        std::shared_ptr<Image::list_t> result(new Image::list_t);

        result->reserve(imageList.size());
        std::copy(imageList.begin(), imageList.end(), std::back_inserter(*result));

        return result;
    }

    /**
     * Iterate over Image list and call passed in function.
     *
     * @return length of Image list
     */
    template <typename F>
    inline int forEachImage(F&& func) const
    {
        const Image::list_t &imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);

        for (auto& image : imageList)
        {
            func(*image);
        }

        return static_cast<int>(imageList.size());
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed() const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_acquire);
    }

    /// @cond HIDDEN_SYMBOLS
    bool hasImage(std::int64_t correlationId) const
    {
        const Image::list_t &imageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        bool isConnected = false;

        for (const auto& image : imageList)
        {
            if (image->correlationId() == correlationId)
            {
                isConnected = true;
                break;
            }
        }

        return isConnected;
    }

    Image::list_t *addImage(std::shared_ptr<Image> image)
    {
        auto oldImageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        auto newImageList = new Image::list_t;

        newImageList->reserve(oldImageList->size() + 1);

        std::copy(oldImageList->begin(), oldImageList->end(), std::back_inserter(*newImageList));
        newImageList->push_back(image);

        std::atomic_store_explicit(&m_imageList, newImageList, std::memory_order_release);

        return oldImageList;
    }

    std::pair<Image::list_t *, int> removeImage(std::int64_t correlationId)
    {
        auto& oldImageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        std::size_t length = oldImageList.size();
        int index = -1;

        for (std::size_t i = 0; i < length; i++)
        {
            if (oldImageList[i]->correlationId() == correlationId)
            {
                oldImageList[i]->close();
                index = static_cast<int>(i);
                break;
            }
        }

        if (-1 != index)
        {
            auto& newImageList = *new Image::list_t;

            newImageList.reserve(oldImageList.size() - 1);
            auto it = oldImageList.begin() + index;

            std::copy(oldImageList.begin(), it, std::back_inserter(newImageList));
            std::copy(it + 1, oldImageList.end(), std::back_inserter(newImageList));
            std::atomic_store_explicit(&m_imageList, &newImageList, std::memory_order_release);
        }

        return {-1 != index ? &oldImageList : nullptr, index};
    }

    Image::list_t *removeAndCloseAllImages()
    {
        auto& oldImageList = *std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);

        for (auto& image : oldImageList)
        {
            image->close();
        }

        auto newImageList = new Image::list_t();

        std::atomic_store_explicit(&m_imageList, newImageList, std::memory_order_release);
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_release);

        return &oldImageList;
    }
    /// @endcond

    /**
     * Get the status for the channel of this {@link Subscription}
     *
     * @return status code for this channel
     */
    std::int64_t channelStatus() const;

private:
    ClientConductor& m_conductor;
    const std::string m_channel;
    std::int32_t m_channelStatusId;
    std::size_t m_roundRobinIndex = 0;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;

    std::atomic<Image::list_t*> m_imageList;
    std::atomic<bool> m_isClosed;
};

}

#endif
