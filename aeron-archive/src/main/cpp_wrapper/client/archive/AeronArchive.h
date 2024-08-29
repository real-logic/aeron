/*
 * Copyright 2014-2024 Real Logic Limited.
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
#ifndef AERON_ARCHIVE_WRAPPER_H
#define AERON_ARCHIVE_WRAPPER_H

#include "client/aeron_archive.h"

#include "Aeron.h"
#include "client/util/ArchiveExceptions.h"

#include "ArchiveContext.h"

namespace aeron { namespace archive { namespace client
{

typedef std::function<void(
    std::int64_t controlSessionId,
    std::int64_t correlationId,
    std::int64_t recordingId,
    std::int64_t startTimestamp,
    std::int64_t stopTimestamp,
    std::int64_t startPosition,
    std::int64_t stopPosition,
    std::int32_t initialTermId,
    std::int32_t segmentFileLength,
    std::int32_t termBufferLength,
    std::int32_t mtuLength,
    std::int32_t sessionId,
    std::int32_t streamId,
    const std::string &strippedChannel,
    const std::string &originalChannel,
    const std::string &sourceIdentity)> recording_descriptor_consumer_t;

using namespace aeron::util;

class AeronArchive
{

public:
    using Context_t = aeron::archive::client::Context;

    /// Location of the source with respect to the archive.
    enum SourceLocation : int
    {
        /// Source is local to the archive and will be recorded using a spy Subscription.
        LOCAL = 0,

        /// Source is remote to the archive and will be recorded using a network Subscription.
        REMOTE = 1
    };


    class AsyncConnect
    {
        friend class AeronArchive;

    public:
        std::shared_ptr<AeronArchive> poll()
        {
            if (nullptr == m_async)
            {
                // TODO log an error?  Or throw an exception;
                return {};
            }

            aeron_archive_t *aeron_archive = nullptr;

            if (aeron_archive_async_connect_poll(&aeron_archive, m_async) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            if (nullptr == aeron_archive)
            {
                return {};
            }

            m_async = nullptr; // _poll() just free'd this up

            // C always creates a new aeron_archive_context_t, so lets get it and wrap it up in a C++ Context
            auto *duplicatedCtx = new Context( aeron_archive_get_and_own_archive_context(aeron_archive));

            return std::shared_ptr<AeronArchive>(new AeronArchive(aeron_archive, duplicatedCtx, m_aeron));
        }

    private:
        explicit AsyncConnect(
            Context &ctx) :
            m_async(nullptr),
            m_aeron(ctx.aeron())
        {
            if (aeron_archive_async_connect(&m_async, ctx.m_ctx) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        aeron_archive_async_connect_t *m_async;
        std::shared_ptr<Aeron> m_aeron;
    };

    static std::shared_ptr<AsyncConnect> asyncConnect(Context &ctx)
    {
        // TODO remove this - let the C layer do all this work, and then add constructors for C++ wrapper objects
        // to allow us to just slide in the underlying C structs after they've been constructed
        //ensureAeronClient(ctx);

        // TODO if there's an Aeron and an aeron::Context attached to the passed in archive::Context, we need to grab and store
        // those C++ wrapper objects - after the connect completes, the newly created archive::Context needs the Aeron and aeron::Context
        // to be re-attached.

        // TODO if there is NOT an Aeron here, that's fine - we just need to make sure that we build a new one
        // once the connect completes.  In that case, that Aeron should NOT own the underlying aeron_t - i.e. that
        // aeron_t will be owned by the AeronArchive.  If/when someone calls ownsAeron(false), we'll have to simultaneously
        // set some sort of 'owns_aeron_t(true) on the Aeron object.

        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    static std::shared_ptr<AeronArchive> connect(Context &ctx)
    {
        //ensureAeronClient(ctx);

        aeron_archive_t *aeron_archive = nullptr;

        if (aeron_archive_connect(&aeron_archive, ctx.m_ctx) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        auto *duplicatedCtx = new Context( aeron_archive_get_and_own_archive_context(aeron_archive));

        return std::shared_ptr<AeronArchive>(new AeronArchive(aeron_archive, duplicatedCtx, ctx.aeron()));
    }

    ~AeronArchive()
    {
        // TODO make sure we clean things up in the correct order

        m_controlResponseSubscription = nullptr;

        // TODO release the Aeron? (if we have one?  We should always have one)

        aeron_archive_close(m_aeron_archive);
    }

    Subscription &controlResponseSubscription()
    {
        // TODO continue to do this lazily?  Or always do this once connect completes?
        if (nullptr == m_controlResponseSubscription)
        {
            m_controlResponseSubscription = std::make_unique<Subscription>(
                aeron_archive_get_aeron(m_aeron_archive),
                aeron_archive_get_and_own_control_response_subscription(m_aeron_archive),
                nullptr);
        }

        return *m_controlResponseSubscription;
    }

    Context &context()
    {
        return *m_ctx;
    }

    std::int64_t archiveId()
    {
        return aeron_archive_get_archive_id(m_aeron_archive);
    }

    inline std::int64_t startRecording(
        const std::string &channel,
        std::int32_t streamId,
        SourceLocation sourceLocation,
        bool autoStop = false)
    {
        int64_t subscription_id;

        if (aeron_archive_start_recording(
            &subscription_id,
            m_aeron_archive,
            channel.c_str(),
            streamId,
            sourceLocation == SourceLocation::LOCAL ?
                AERON_ARCHIVE_SOURCE_LOCATION_LOCAL :
                AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
            autoStop) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return subscription_id;
    }

    inline std::int64_t getRecordingPosition(std::int64_t recordingId)
    {
        int64_t recording_position;

        if (aeron_archive_get_recording_position(&recording_position, m_aeron_archive, recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_position;
    }

    inline std::int64_t getStopPosition(std::int64_t recordingId)
    {
        int64_t stop_position;

        if (aeron_archive_get_stop_position(&stop_position, m_aeron_archive, recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stop_position;
    }

    inline std::int64_t getMaxRecordedPosition(std::int64_t recordingId)
    {
        int64_t max_recorded_position;

        if (aeron_archive_get_max_recorded_position(&max_recorded_position, m_aeron_archive, recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return max_recorded_position;
    }

    inline void stopRecording(std::int64_t subscriptionId)
    {
        if (aeron_archive_stop_recording_subscription(m_aeron_archive, subscriptionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline std::int64_t findLastMatchingRecording(
        std::int64_t minRecordingId,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId)
    {
        int64_t recording_id;

        if (aeron_archive_find_last_matching_recording(
            &recording_id,
            m_aeron_archive,
            minRecordingId,
            channelFragment.c_str(),
            streamId,
            sessionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_id;
    }

    inline std::int32_t listRecording(std::int64_t recordingId, const recording_descriptor_consumer_t &consumer)
    {
        int32_t count;

        if (aeron_archive_list_recording(
            &count,
            m_aeron_archive,
            recordingId,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

private:
    explicit AeronArchive(
        aeron_archive_t *aeron_archive,
        Context *ctx,
        const std::shared_ptr<Aeron> originalAeron) :
        m_aeron_archive(aeron_archive),
        m_ctx(std::unique_ptr<Context>(ctx))
    {
        if (nullptr == originalAeron)
        {
            // TODO create a new one using the underlying aeron_t
            aeron_t *aeron = getAeronT();
            // TODO need to find an aeron::Context
            //m_aeron = new Aeron(nullptr, aeron);
        }
        else
        {
            m_aeron = originalAeron;
        }

    }

    aeron_t *getAeronT()
    {
        return aeron_archive_get_aeron(m_aeron_archive);
    }

    aeron_archive_t *m_aeron_archive = nullptr;

    std::shared_ptr<Aeron> m_aeron = nullptr;

    std::unique_ptr<Context> m_ctx = nullptr;

    std::unique_ptr<Subscription> m_controlResponseSubscription = nullptr;

    /*
    static void ensureAeronClient(Context &ctx)
    {
        if (nullptr == ctx.aeron())
        {
            auto aeronContext = aeron::Context();
            aeronContext.aeronDir(ctx.aeronDirectoryName());
            ctx.setAeron(std::make_shared<Aeron>(aeronContext));
        }
    }
     */

    static void recording_descriptor_consumer_func(
        aeron_archive_recording_descriptor_t *recording_descriptor,
        void *clientd)
    {
        recording_descriptor_consumer_t &consumer = *reinterpret_cast<recording_descriptor_consumer_t *>(clientd);

        consumer(
            recording_descriptor->control_session_id,
            recording_descriptor->correlation_id,
            recording_descriptor->recording_id,
            recording_descriptor->start_timestamp,
            recording_descriptor->stop_timestamp,
            recording_descriptor->start_position,
            recording_descriptor->stop_position,
            recording_descriptor->initial_term_id,
            recording_descriptor->segment_file_length,
            recording_descriptor->term_buffer_length,
            recording_descriptor->mtu_length,
            recording_descriptor->session_id,
            recording_descriptor->stream_id,
            recording_descriptor->stripped_channel,
            recording_descriptor->original_channel,
            recording_descriptor->source_identity);
    }
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_H
