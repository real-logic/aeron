/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef INCLUDED_AERON_H
#define INCLUDED_AERON_H

#include "util/Exceptions.h"
#include <iostream>
#include <thread>
#include <random>
#include <unordered_map>
#include <mutex>
#include "concurrent/logbuffer/TermReader.h"
#include "util/MemoryMappedFile.h"
#include "concurrent/broadcast/CopyBroadcastReceiver.h"
#include "concurrent/SleepingIdleStrategy.h"
#include "concurrent/AgentRunner.h"
#include "concurrent/AgentInvoker.h"
#include "Publication.h"
#include "ExclusivePublication.h"
#include "Subscription.h"
#include "Context.h"
#include "Counter.h"
#include "util/Export.h"

extern "C"
{
#include "aeronc.h"
}


/// Top namespace for Aeron C++ API
namespace aeron
{

using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::broadcast;

using AsyncAddPublication = aeron_async_add_publication_t;
using AsyncAddExclusivePublication = aeron_async_add_exclusive_publication_t;
using AsyncAddCounter = aeron_async_add_counter_t;

static_assert(sizeof(AsyncAddPublication *) == sizeof(std::int64_t), "int64 must hold space for pointer");
static_assert(sizeof(AsyncAddExclusivePublication *) == sizeof(std::int64_t), "int64 must hold space for pointer");
static_assert(sizeof(AsyncAddSubscription *) == sizeof(std::int64_t), "int64 must hold space for pointer");
static_assert(sizeof(AsyncAddCounter *) == sizeof(std::int64_t), "int64 must hold space for pointer");

/**
 * @example BasicPublisher.cpp
 * An example of a basic publishing application
 *
 * @example BasicSubscriber.cpp
 * An example of a basic subscribing application
 */

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use a {@link Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 */
class CLIENT_EXPORT Aeron
{
public:
    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     */
    explicit Aeron(Context &context);

    ~Aeron();

    /**
     * Indicate if the instance is closed and can not longer be used.
     *
     * @return true is the instance is closed and can no longer be used, otherwise false.
     */
    inline bool isClosed()
    {
        throw UnsupportedOperationException("Need client close on C API", SOURCEINFO);
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @param context for configuration of the client.
     * @return the new Aeron instance connected to the Media Driver.
     */
    inline static std::shared_ptr<Aeron> connect(Context &context)
    {
        return std::make_shared<Aeron>(context);
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new Aeron instance connected to the Media Driver.
     */
    inline static std::shared_ptr<Aeron> connect()
    {
        Context ctx;

        return std::make_shared<Aeron>(ctx);
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return id to resolve the AsyncAddPublication for the publication
     */
    inline std::int64_t addPublication(const std::string &channel, std::int32_t streamId)
    {
        const AsyncAddPublication *addPublication = addPublicationAsync(channel, streamId);
        return reinterpret_cast<std::int64_t>(addPublication);
    }

    /**
     * Retrieve the Publication associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Publication then what is returned is the Publication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addPublication
     *
     * @param registrationId of the Publication returned by Aeron::addPublication
     * @return Publication associated with the registrationId
     */
    inline std::shared_ptr<Publication> findPublication(std::int64_t registrationId)
    {
        return findPublication(reinterpret_cast<AsyncAddPublication *>(registrationId));
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * AsyncAddPublication is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return AsyncAddPublication for the publication.
     */
    inline AsyncAddPublication *addPublicationAsync(const std::string &channel, std::int32_t streamId)
    {
        aeron_async_add_publication_t *addPublication;
        if (aeron_async_add_publication(&addPublication, m_aeron, channel.c_str(), streamId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return addPublication;
    }

    /**
     * Retrieve the Publication associated with the given AsyncAddPublication.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Publication then what is returned is the Publication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addPublicationAsync
     *
     * @param addPublication for the Publication returned by Aeron::addPublicationAsync
     * @return Publication associated with the addPublication
     */
    inline std::shared_ptr<Publication> findPublication(AsyncAddPublication *addPublication)
    {
        aeron_publication_t *publication;
        int result = aeron_async_add_publication_poll(&publication, addPublication);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (result == 0)
        {
            return nullptr;
        }
        else
        {
            return std::make_shared<Publication>(publication);
        }
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return id to resolve AsyncAddExclusivePublication for the publication
     */
    std::int64_t addExclusivePublication(const std::string &channel, std::int32_t streamId)
    {
        const AsyncAddExclusivePublication *addExclusivePublication = addExclusivePublicationAsync(channel, streamId);
        return reinterpret_cast<std::int64_t>(addExclusivePublication);
    }

    /**
     * Retrieve the ExclusivePublication associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addExclusivePublication
     *
     * @param registrationId of the ExclusivePublication returned by Aeron::addExclusivePublication
     * @return ExclusivePublication associated with the registrationId
     */
    inline std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId)
    {
        return findExclusivePublication(reinterpret_cast<AsyncAddExclusivePublication *>(registrationId));
    }

    /**
     * Add a {@link ExclusivePublication} for publishing messages to subscribers
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * AsyncAddExclusivePublication is to be used to determine the status of the command with the media driver.
     *
     * @param channel for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return AsyncAddExclusivePublication for the publication.
     */
    inline AsyncAddExclusivePublication *addExclusivePublicationAsync(const std::string &channel, std::int32_t streamId)
    {
        aeron_async_add_exclusive_publication_t *addPublication;
        if (aeron_async_add_exclusive_publication(&addPublication, m_aeron, channel.c_str(), streamId) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return addPublication;
    }

    /**
     * Retrieve the ExclusivePublication associated with the given AsyncAddPublication.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the ExclusivePublication then what is returned is the ExclusivePublication.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addExclusivePublicationAsync
     *
     * @param addPublication for the ExclusivePublication returned by Aeron::addExclusivePublicationAsync
     * @return ExclusivePublication associated with the addPublication
     */
    inline std::shared_ptr<ExclusivePublication> findExclusivePublication(AsyncAddExclusivePublication *addPublication)
    {
        aeron_exclusive_publication_t *publication;
        int result = aeron_async_add_exclusive_publication_poll(&publication, addPublication);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (result == 0)
        {
            return nullptr;
        }
        else
        {
            return std::make_shared<ExclusivePublication>(publication);
        }
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This function returns immediately and does not wait for the response from the media driver. The returned
     * registration id is to be used to determine the status of the command with the media driver.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return registration id for the subscription
     */
    inline std::int64_t addSubscription(const std::string &channel, std::int32_t streamId)
    {
        return addSubscription(
            channel, streamId, m_context.m_onAvailableImageHandler, m_context.m_onUnavailableImageHandler);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
     * @return id to resolve the AsyncAddSubscription for the subscription
     */
    std::int64_t addSubscription(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler)
    {
        const AsyncAddSubscription *addSubscription = addSubscriptionAsync(
            channel, streamId, onAvailableImageHandler, onUnavailableImageHandler);
        return reinterpret_cast<std::int64_t>(addSubscription);
    }

    /**
     * Retrieve the Subscription associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addSubscription
     *
     * @param registrationId of the Subscription returned by Aeron::addSubscription
     * @return Subscription associated with the registrationId
     */
    inline std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId)
    {
        return findSubscription(reinterpret_cast<AsyncAddSubscription *>(registrationId));
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
     * @return AsyncAddSubscription object to track the addition of the subscription
     */
    inline AsyncAddSubscription *addSubscriptionAsync(
        const std::string &channel,
        std::int32_t streamId,
        const on_available_image_t &onAvailableImageHandler,
        const on_unavailable_image_t &onUnavailableImageHandler)
    {
        AsyncAddSubscription *addSubscription = new AsyncAddSubscription(
            onAvailableImageHandler, onUnavailableImageHandler);
        void *availableClientd = const_cast<void *>(reinterpret_cast<const void *>(&addSubscription->m_onAvailableImage));
        void *unavailableClientd = const_cast<void *>(reinterpret_cast<const void *>(&addSubscription->m_onUnavailableImage));

        if (aeron_async_add_subscription(
            &addSubscription->m_async,
            m_aeron,
            channel.c_str(),
            streamId,
            onAvailableImageCallback, availableClientd,
            onUnavailableImageCallback, unavailableClientd) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return addSubscription;
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * This method will override the default handlers from the {@link Context}.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @return AsyncAddSubscription object to track the addition of the subscription
     */
    inline AsyncAddSubscription *addSubscriptionAsync(
        const std::string &channel,
        std::int32_t streamId)
    {
        return addSubscriptionAsync(
            channel, streamId, m_context.m_onAvailableImageHandler, m_context.m_onUnavailableImageHandler);
    }

    /**
     * Retrieve the Subscription associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addSubscription
     *
     * @param registrationId of the Subscription returned by Aeron::addSubscription
     * @return Subscription associated with the registrationId
     */
    inline std::shared_ptr<Subscription> findSubscription(AsyncAddSubscription *addSubscription)
    {
        aeron_subscription_t *subscription;
        int result = aeron_async_add_subscription_poll(&subscription, addSubscription->m_async);
        if (result < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        else if (result == 0)
        {
            return nullptr;
        }
        else
        {
            return std::make_shared<Subscription>(subscription, addSubscription, m_countersReader);
        }
    }

    /**
     * Generate the next correlation id that is unique for the connected Media Driver.
     *
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     *
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    inline std::int64_t nextCorrelationId()
    {
        return aeron_next_correlation_id(m_aeron);
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyLength   of the key in the keyBuffer.
     * @param label       for the counter.
     * @return id to resolve the AsyncAddCounter for the Counter
     */
    std::int64_t addCounter(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label)
    {
        const AsyncAddCounter *addCounter = addCounterAsync(typeId, keyBuffer, keyLength, label);
        return reinterpret_cast<std::int64_t>(addCounter);
    }

    /**
     * Retrieve the Counter associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Counter then what is returned is the Counter.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addCounter
     *
     * @param registrationId of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registrationId
     */
    inline std::shared_ptr<Counter> findCounter(std::int64_t registrationId)
    {
        return findCounter(reinterpret_cast<AsyncAddCounter *>(registrationId));
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyLength   of the key in the keyBuffer.
     * @param label       for the counter.
     * @return AsyncAddCounter to find the counter
     */
    inline AsyncAddCounter *addCounterAsync(
        std::int32_t typeId,
        const std::uint8_t *keyBuffer,
        std::size_t keyLength,
        const std::string &label)
    {
        aeron_async_add_counter_t *addCounter;
        if (aeron_async_add_counter(
            &addCounter, m_aeron, typeId, keyBuffer, keyLength, label.c_str(), label.length()) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return addCounter;
    }

    /**
     * Retrieve the Counter associated with the given registrationId.
     *
     * This method is non-blocking.
     *
     * The value returned is dependent on what has occurred with respect to the media driver:
     *
     * - If the registrationId is unknown, then a nullptr is returned.
     * - If the media driver has not answered the add command, then a nullptr is returned.
     * - If the media driver has successfully added the Counter then what is returned is the Counter.
     * - If the media driver has returned an error, this method will throw the error returned.
     *
     * @see Aeron::addCounter
     *
     * @param registrationId of the Counter returned by Aeron::addCounter
     * @return Counter associated with the registrationId
     */
    inline std::shared_ptr<Counter> findCounter(AsyncAddCounter *addCounter)
    {
        aeron_counter_t *counter;
        if (aeron_async_add_counter_poll(&counter, addCounter) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return std::make_shared<Counter>(counter, m_countersReader);
    }

    /**
     * Add a handler to the list to be called when a counter becomes available.
     *
     * @param handler to be added to the available counters list.
     */
    inline void addAvailableCounterHandler(const on_available_counter_t &handler)
    {
        aeron_on_available_counter_pair_t counterPair;
        counterPair.handler = onAvailableCounterCallback;
        counterPair.clientd = (void *)&handler;

        if (aeron_add_available_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Remove a handler from the list to be called when a counter becomes available.
     *
     * @param handler to be removed from the available counters list.
     */
    inline void removeAvailableCounterHandler(const on_available_counter_t &handler)
    {
        aeron_on_available_counter_pair_t counterPair;
        counterPair.handler = onAvailableCounterCallback;
        counterPair.clientd = (void *)&handler;

        if (aeron_add_available_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Add a handler to the list to be called when a counter becomes unavailable.
     *
     * @param handler to be added to the unavailable counters list.
     */
    inline void addUnavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        aeron_on_unavailable_counter_pair_t counterPair;
        counterPair.handler = onUnavailableCounterCallback;
        counterPair.clientd = (void *)&handler;

        if (aeron_add_unavailable_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Remove a handler from the list to be called when a counter becomes unavailable.
     *
     * @param handler to be removed from the unavailable counters list.
     */
    inline void removeUnavailableCounterHandler(const on_unavailable_counter_t &handler)
    {
        aeron_on_unavailable_counter_pair_t counterPair;
        counterPair.handler = onUnavailableCounterCallback;
        counterPair.clientd = (void *)&handler;

        if (aeron_add_unavailable_counter_handler(m_aeron, &counterPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Add a handler to the list to be called when the client is closed.
     *
     * @param handler to be added to the close client handlers list.
     */
    inline void addCloseClientHandler(const on_close_client_t &handler)
    {
        aeron_on_close_client_pair_t closeClientPair;
        closeClientPair.handler = onCloseClientCallback;
        closeClientPair.clientd = (void *)&handler;

        if (aeron_add_close_handler(m_aeron, &closeClientPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Remove a handler from the list to be called when the client is closed.
     *
     * @param handler to be removed from the close client handlers list.
     */
    inline void removeCloseClientHandler(const on_close_client_t &handler)
    {
        aeron_on_close_client_pair_t closeClientPair;
        closeClientPair.handler = onCloseClientCallback;
        closeClientPair.clientd = (void *)&handler;

        if (aeron_remove_close_handler(m_aeron, &closeClientPair) < 0)
        {
            AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Return the AgentInvoker for the client conductor.
     *
     * @return AgentInvoker for the conductor.
     */
    inline AgentInvoker<ClientConductor> &conductorAgentInvoker()
    {
        throw new UnsupportedOperationException("How to manage this??", SOURCEINFO);
    }

    /**
     * Return whether the AgentInvoker is used or not.
     *
     * @return true if AgentInvoker used or false if not.
     */
    inline bool usesAgentInvoker() const
    {
        return m_context.m_useConductorAgentInvoker;
    }

    /**
     * Get the CountersReader for the Aeron media driver counters.
     *
     * @return CountersReader for the Aeron media driver in use.
     */
    inline CountersReader &countersReader()
    {
        return m_countersReader;
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    inline std::int64_t clientId() const
    {
        return aeron_client_id(m_aeron);
    }

    /**
     * Get the Aeron Context object used in construction of the Aeron instance.
     *
     * @return Context instance in use.
     */
    inline Context &context()
    {
        return m_context;
    }

    inline const Context &context() const
    {
        return m_context;
    }

    /**
     * Return the static version and build string for the binary library.
     *
     * @return static version and build string for the binary library.
     */
    static std::string version();

private:
    Context m_context;
    aeron_t* m_aeron;
    CountersReader m_countersReader;

    static void onAvailableImageCallback(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        on_available_image_t& callback = *static_cast<on_available_image_t *>(clientd);
        Image imageWrapper(subscription, image);
        callback(imageWrapper);
    }

    static void onUnavailableImageCallback(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
    {
        on_unavailable_image_t & callback = *static_cast<on_unavailable_image_t *>(clientd);
        Image imageWrapper(subscription, image);
        callback(imageWrapper);
    }

    static void onAvailableCounterCallback(
        void *clientd, aeron_counters_reader_t *counters_reader, int64_t registration_id, int32_t counter_id)
    {
        CountersReader reader = CountersReader(counters_reader);
        on_available_counter_t& callback = *static_cast<on_available_counter_t *>(clientd);
        callback(reader, registration_id, counter_id);
    }

    static void onUnavailableCounterCallback(
        void *clientd, aeron_counters_reader_t *counters_reader, int64_t registration_id, int32_t counter_id)
    {
        CountersReader reader = CountersReader(counters_reader);
        on_unavailable_counter_t& callback = *static_cast<on_unavailable_counter_t *>(clientd);
        callback(reader, registration_id, counter_id);
    }

    static void onCloseClientCallback(void *clientd)
    {
        on_close_client_t& callback = *static_cast<on_close_client_t *>(clientd);
        callback();
    }
};

}

#endif
