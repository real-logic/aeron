/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll across multiple {@link Image}s.
     * @return the number of fragments received
     *
     * @see FragmentAssembler
     */
    inline int poll(const fragment_handler_t fragmentHandler, int fragmentLimit)
    {
        int fragmentsRead = 0;
        const int length = std::atomic_load(&m_imagesLength);
        Image *images = std::atomic_load(&m_images);

        if (length > 0)
        {
            int startingIndex = m_roundRobinIndex;
            if (startingIndex >= length)
            {
                m_roundRobinIndex = startingIndex = 0;
            }

            int i = startingIndex;

            do
            {
                fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);

                if (++i == length)
                {
                    i = 0;
                }
            }
            while (fragmentsRead < fragmentLimit && i != startingIndex);
        }

        return fragmentsRead;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     *
     * @param blockHandler     to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     */
    inline long blockPoll(const block_handler_t blockHandler, int blockLengthLimit)
    {
        const int length = std::atomic_load(&m_imagesLength);
        Image *images = std::atomic_load(&m_images);
        long bytesConsumed = 0;

        for (int i = 0; i < length; i++)
        {
            bytesConsumed += images[i].blockPoll(blockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    // TODO: add filePoll to return MemoryMappedFile

    /**
     * Count of images connected to this subscription.
     *
     * @return count of images connected to this subscription.
     */
    inline int imageCount() const
    {
        return std::atomic_load(&m_imagesLength);
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
    inline std::shared_ptr<Image> getImage(std::int32_t sessionId)
    {
        const int length = std::atomic_load(&m_imagesLength);
        Image* images = std::atomic_load(&m_images);
        int index = -1;

        for (int i = 0; i < length; i++)
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
     * Get a std::vector of active {@link Image}s that match this subscription.
     *
     * @return a std::vector of active {@link Image}s that match this subscription.
     */
    inline std::shared_ptr<std::vector<Image>> images()
    {
        const int length = std::atomic_load(&m_imagesLength);
        Image *images = std::atomic_load(&m_images);
        std::shared_ptr<std::vector<Image>> result(new std::vector<Image>(length));

        for (int i = 0; i < length; i++)
        {
            (*result)[i] = images[i];
        }

        return result;
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    inline bool isClosed(void) const
    {
        return std::atomic_load_explicit(&m_isClosed, std::memory_order_relaxed);
    }

    /// @cond HIDDEN_SYMBOLS
    bool hasImage(std::int32_t sessionId)
    {
        const int length = std::atomic_load(&m_imagesLength);
        Image *images = std::atomic_load(&m_images);
        bool isConnected = false;

        for (int i = 0; i < length; i++)
        {
            if (images[i].sessionId() == sessionId)
            {
                isConnected = true;
                break;
            }
        }

        return isConnected;
    }

    Image *addImage(Image &image)
    {
        Image * oldArray = std::atomic_load(&m_images);
        int length = std::atomic_load(&m_imagesLength);
        Image * newArray = new Image[length + 1];

        for (int i = 0; i < length; i++)
        {
            newArray[i] = std::move(oldArray[i]);
        }

        newArray[length] = image; // copy-assign

        std::atomic_store(&m_images, newArray);
        std::atomic_store(&m_imagesLength, length + 1); // set length last. Don't go over end of old array on poll

        // oldArray to linger and be deleted by caller (aka client conductor)
        return oldArray;
    }

    std::pair<Image*, int> removeImage(std::int64_t correlationId)
    {
        Image * oldArray = std::atomic_load(&m_images);
        int length = std::atomic_load(&m_imagesLength);
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
            Image * newArray = new Image[length - 1];

            for (int i = 0, j = 0; i < length; i++)
            {
                if (i != index)
                {
                    newArray[j++] = std::move(oldArray[i]);
                }
            }

            std::atomic_store(&m_imagesLength, length - 1);  // set length first. Don't go over end of new array on poll
            std::atomic_store(&m_images, newArray);
        }

        // oldArray to linger and be deleted by caller (aka client conductor)
        return std::pair<Image *, int>(
            (-1 != index) ? oldArray : nullptr,
            index);
    }

    std::pair<Image*, int> removeAndCloseAllImages(void)
    {
        Image * oldArray = std::atomic_load(&m_images);
        int length = std::atomic_load(&m_imagesLength);

        for (int i = 0; i < length; i++)
        {
            oldArray[i].close();
        }

        std::atomic_store(&m_imagesLength, 0);  // set length first. Don't go over end of new array on poll
        std::atomic_store(&m_images, new Image[0]);

        std::atomic_store_explicit(&m_isClosed, true, std::memory_order_relaxed);

        // oldArray to linger and be deleted by caller (aka client conductor)
        return std::pair<Image *, int>(oldArray, length);
    }
    /// @endcond

private:
    ClientConductor& m_conductor;
    const std::string m_channel;
    int m_roundRobinIndex = 0;
    std::int64_t m_registrationId;
    std::int32_t m_streamId;

    std::atomic<Image*> m_images;
    std::atomic<int> m_imagesLength;

    std::atomic<bool> m_isClosed;
};

}

#endif