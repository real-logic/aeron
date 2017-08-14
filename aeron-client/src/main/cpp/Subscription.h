/*
 * Copyright 2014-2017 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_SUBSCRIPTION__
#define INCLUDED_AERON_SUBSCRIPTION__

#include <cstdint>
#include <iostream>
#include <atomic>
#include <concurrent/logbuffer/TermReader.h>
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
        ClientConductor& conductor, std::int64_t registrationId, const std::string& channel, std::int32_t streamId);
    /// @endcond
    virtual ~Subscription();

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
     * Poll the Image s under the subscription for having reached End of Stream.
     *
     * @param endOfStreamHandler callback for handling end of stream indication.
     * @return number of Image s that have reached End of Stream.
     */
    template <typename F>
    inline int pollEndOfStreams(F&& endOfStreamHandler)
    {
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image *images = imageList->m_images;
        int numEndOfStreams = 0;

        for (std::size_t i = 0; i < length; i++)
        {
            if (images[i].isEndOfStream())
            {
                numEndOfStreams++;
                endOfStreamHandler(images[i]);
            }
        }

        return numEndOfStreams;
    }

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
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image *images = imageList->m_images;
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
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
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image *images = imageList->m_images;
        int fragmentsRead = 0;

        std::size_t startingIndex = m_roundRobinIndex++;
        if (startingIndex >= length)
        {
            m_roundRobinIndex = startingIndex = 0;
        }

        for (std::size_t i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (std::size_t i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
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
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image *images = imageList->m_images;
        long bytesConsumed = 0;

        for (std::size_t i = 0; i < length; i++)
        {
            bytesConsumed += images[i].blockPoll(blockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Count of images connected to this subscription.
     *
     * @return count of images connected to this subscription.
     */
    inline int imageCount() const
    {
        return static_cast<int>(std::atomic_load_explicit(&m_imageList, std::memory_order_acquire)->m_length);
    }

    /**
     * Return the {@link Image} associated with the given sessionId.
     *
     * This method generates a new copy of the Image overlaying the logbuffer.
     * It is up to the application to not use the Image if it becomes unavailable.
     *
     * @param sessionId associated with the Image.
     * @return Image associated with the given sessionId or nullptr if no Image exist.
     */
    inline std::shared_ptr<Image> imageBySessionId(std::int32_t sessionId) const
    {
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image* images = imageList->m_images;
        int index = -1;

        for (int i = 0; i < static_cast<int>(length); i++)
        {
            if (images[i].sessionId() == sessionId)
            {
                index = i;
                break;
            }
        }

        return (index != -1) ? std::shared_ptr<Image>(new Image(images[index])) : std::shared_ptr<Image>();
    }

    /**
     * Get the image at the given index from the images array.
     *
     * This is only valid until the image list changes.
     *
     * @param index in the array
     * @return image at given index or exception if out of range.
     */
    inline Image& imageAtIndex(size_t index) const
    {
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        Image *images = imageList->m_images;

        if (index >= imageList->m_length)
        {
            throw std::out_of_range("image index out of range");
        }

        return images[index];
    }

    /**
     * Get a std::vector of active {@link Image}s that match this subscription.
     *
     * @return a std::vector of active {@link Image}s that match this subscription.
     */
    inline std::shared_ptr<std::vector<Image>> images()
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
     * Iterate over Image list and call passed in function.
     *
     * @return length of Image list
     */
    template <typename F>
    inline int forEachImage(F&& func)
    {
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image* images = imageList->m_images;

        for (std::size_t i = 0; i < length; i++)
        {
            func(images[i]);
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
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_acquire);
    }

    /// @cond HIDDEN_SYMBOLS
    bool hasImage(std::int64_t correlationId) const
    {
        const struct ImageList *imageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        const std::size_t length = imageList->m_length;
        Image *images = imageList->m_images;
        bool isConnected = false;

        for (std::size_t i = 0; i < length; i++)
        {
            if (images[i].correlationId() == correlationId)
            {
                isConnected = true;
                break;
            }
        }

        return isConnected;
    }

    struct ImageList *addImage(Image &image)
    {
        struct ImageList *oldImageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        Image *oldArray = oldImageList->m_images;
        std::size_t length = oldImageList->m_length;
        auto newArray = new Image[length + 1];

        for (std::size_t i = 0; i < length; i++)
        {
            newArray[i] = std::move(oldArray[i]);
        }

        newArray[length] = image; // copy-assign

        auto newImageList = new struct ImageList(newArray, length + 1);

        std::atomic_store_explicit(&m_imageList, newImageList, std::memory_order_release);

        return oldImageList;
    }

    std::pair<struct ImageList *, int> removeImage(std::int64_t correlationId)
    {
        struct ImageList *oldImageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        Image * oldArray = oldImageList->m_images;
        auto length = static_cast<int>(oldImageList->m_length);
        int index = -1;

        for (int i = 0; i < length; i++)
        {
            if (oldArray[i].correlationId() == correlationId)
            {
                index = i;
                break;
            }
        }

        if (-1 != index)
        {
            auto newArray = new Image[length - 1];

            for (int i = 0, j = 0; i < length; i++)
            {
                if (i != index)
                {
                    newArray[j++] = std::move(oldArray[i]);
                }
            }

            auto newImageList = new struct ImageList(newArray, length - 1);

            std::atomic_store_explicit(&m_imageList, newImageList, std::memory_order_release);
        }

        return std::pair<struct ImageList *,int>(
                (-1 != index) ? oldImageList : nullptr,
                index);
    }

    struct ImageList *removeAndCloseAllImages()
    {
        struct ImageList *oldImageList = std::atomic_load_explicit(&m_imageList, std::memory_order_acquire);
        Image *oldArray = oldImageList->m_images;
        std::size_t length = oldImageList->m_length;

        for (std::size_t i = 0; i < length; i++)
        {
            oldArray[i].close();
        }

        auto newImageList = new struct ImageList(new Image[0], 0);

        std::atomic_store_explicit(&m_imageList, newImageList, std::memory_order_release);
        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_release);

        return oldImageList;
    }
    /// @endcond

private:
    ClientConductor& m_conductor;
    const std::string m_channel;
    std::size_t m_roundRobinIndex = 0;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;

    std::atomic<struct ImageList*> m_imageList;
    std::atomic<bool> m_isClosed;
};

}

#endif
