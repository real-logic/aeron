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

#include <utility>

#include "client/aeron_archive.h"

#include "Aeron.h"
#include "client/util/ArchiveExceptions.h"

#include "ArchiveContext.h"
#include "ReplayParams.h"
#include "ReplicationParams.h"

namespace aeron { namespace archive { namespace client
{

constexpr const std::int64_t NULL_POSITION = aeron::NULL_VALUE;

constexpr const std::int64_t NULL_LENGTH = aeron::NULL_VALUE;

struct RecordingDescriptor
{
    RecordingDescriptor(
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
        std::string strippedChannel,
        std::string originalChannel,
        std::string sourceIdentity) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_recordingId(recordingId),
        m_startTimestamp(startTimestamp),
        m_stopTimestamp(stopTimestamp),
        m_startPosition(startPosition),
        m_stopPosition(stopPosition),
        m_initialTermId(initialTermId),
        m_segmentFileLength(segmentFileLength),
        m_termBufferLength(termBufferLength),
        m_mtuLength(mtuLength),
        m_sessionId(sessionId),
        m_streamId(streamId),
        m_strippedChannel(std::move(strippedChannel)),
        m_originalChannel(std::move(originalChannel)),
        m_sourceIdentity(std::move(sourceIdentity))
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_correlationId;
    std::int64_t m_recordingId;
    std::int64_t m_startTimestamp;
    std::int64_t m_stopTimestamp;
    std::int64_t m_startPosition;
    std::int64_t m_stopPosition;
    std::int32_t m_initialTermId;
    std::int32_t m_segmentFileLength;
    std::int32_t m_termBufferLength;
    std::int32_t m_mtuLength;
    std::int32_t m_sessionId;
    std::int32_t m_streamId;
    const std::string m_strippedChannel;
    const std::string m_originalChannel;
    const std::string m_sourceIdentity;
};

typedef std::function<void(RecordingDescriptor &recordingDescriptor)> recording_descriptor_consumer_t;

struct RecordingSubscriptionDescriptor
{
    RecordingSubscriptionDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t subscriptionId,
        std::int32_t streamId,
        std::string strippedChannel) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_subscriptionId(subscriptionId),
        m_streamId(streamId),
        m_strippedChannel(std::move(strippedChannel))
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_correlationId;
    std::int64_t m_subscriptionId;
    std::int32_t m_streamId;
    const std::string m_strippedChannel;
};

typedef std::function<void(RecordingSubscriptionDescriptor &recordingSubscriptionDescriptor)> recording_subscription_descriptor_consumer_t;

using namespace aeron::util;

class AeronArchive
{

    friend class ReplayMerge;

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

            return std::shared_ptr<AeronArchive>(
                new AeronArchive(aeron_archive, m_aeronW, m_recordingSignalConsumer));
        }

    private:
        explicit AsyncConnect(
            Context &ctx) :
            m_async(nullptr),
            m_aeronW(ctx.aeron()),
            m_recordingSignalConsumer(ctx.m_recordingSignalConsumer)
        {
            // async_connect makes a copy of the underlying aeron_archive_context_t
            if (aeron_archive_async_connect(&m_async, ctx.m_aeron_archive_ctx_t) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        aeron_archive_async_connect_t *m_async;
        std::shared_ptr<Aeron> m_aeronW;
        const recording_signal_consumer_t m_recordingSignalConsumer;
    };

    static std::shared_ptr<AsyncConnect> asyncConnect(Context &ctx)
    {
        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    static std::shared_ptr<AeronArchive> connect(Context &ctx)
    {
        aeron_archive_t *aeron_archive = nullptr;

        if (aeron_archive_connect(&aeron_archive, ctx.m_aeron_archive_ctx_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::shared_ptr<AeronArchive>(
            new AeronArchive(aeron_archive, ctx.aeron(), ctx.m_recordingSignalConsumer));
    }

    ~AeronArchive()
    {
        // make sure to clean things up in the correct order
        m_controlResponseSubscription = nullptr;

        aeron_archive_close(m_aeron_archive_t);
    }

    Subscription &controlResponseSubscription()
    {
        return *m_controlResponseSubscription;
    }

    Context &context()
    {
        return m_archiveCtxW;
    }

    std::int64_t archiveId()
    {
        return aeron_archive_get_archive_id(m_aeron_archive_t);
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
            m_aeron_archive_t,
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

        if (aeron_archive_get_recording_position(
            &recording_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_position;
    }

    inline std::int64_t getStopPosition(std::int64_t recordingId)
    {
        int64_t stop_position;

        if (aeron_archive_get_stop_position(
            &stop_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stop_position;
    }

    inline std::int64_t getMaxRecordedPosition(std::int64_t recordingId)
    {
        int64_t max_recorded_position;

        if (aeron_archive_get_max_recorded_position(
            &max_recorded_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return max_recorded_position;
    }

    inline void stopRecording(std::int64_t subscriptionId)
    {
        if (aeron_archive_stop_recording_subscription(
            m_aeron_archive_t,
            subscriptionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline bool tryStopRecording(std::int64_t subscriptionId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_subscription(
            &stopped,
            m_aeron_archive_t,
            subscriptionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    inline void stopAllReplays(std::int64_t recordingId)
    {
        if (aeron_archive_stop_all_replays(
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline bool tryStopRecordingByIdentity(std::int64_t recordingId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_by_identity(
            &stopped,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    inline void stopRecording(const std::shared_ptr<Publication> publication)
    {
        if (aeron_archive_stop_recording_publication(
            m_aeron_archive_t,
            publication->publication()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline void stopRecording(const std::shared_ptr<ExclusivePublication> exclusivePublication)
    {
        if (aeron_archive_stop_recording_exclusive_publication(
            m_aeron_archive_t,
            exclusivePublication->publication()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline std::int64_t getStartPosition(std::int64_t recordingId)
    {
        int64_t startPosition;

        if (aeron_archive_get_start_position(
            &startPosition,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return startPosition;
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
            m_aeron_archive_t,
            minRecordingId,
            channelFragment.c_str(),
            streamId,
            sessionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_id;
    }

    inline std::int32_t listRecording(
        std::int64_t recordingId,
        const recording_descriptor_consumer_t &consumer)
    {
        int32_t count;

        if (aeron_archive_list_recording(
            &count,
            m_aeron_archive_t,
            recordingId,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int32_t listRecordings(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const recording_descriptor_consumer_t &consumer)
    {
        int32_t count;

        if (aeron_archive_list_recordings(
            &count,
            m_aeron_archive_t,
            fromRecordingId,
            recordCount,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int32_t listRecordingsForUri(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        const recording_descriptor_consumer_t &consumer)
    {
        int32_t count;

        if (aeron_archive_list_recordings_for_uri(
            &count,
            m_aeron_archive_t,
            fromRecordingId,
            recordCount,
            channelFragment.c_str(),
            streamId,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline void stopRecording(const std::string &channel, std::int32_t streamId)
    {
        if (aeron_archive_stop_recording_channel_and_stream(
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline bool tryStopRecording(const std::string &channel, std::int32_t streamId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_channel_and_stream(
            &stopped,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    inline std::int64_t extendRecording(
        std::int64_t recordingId,
        const std::string &channel,
        std::int32_t streamId,
        SourceLocation sourceLocation,
        bool autoStop)
    {
        int64_t subscription_id;

        if (aeron_archive_extend_recording(
            &subscription_id,
            m_aeron_archive_t,
            recordingId,
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

    inline std::int64_t startReplay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        ReplayParams &replayParams)
    {
        int64_t replay_session_id;

        if (aeron_archive_start_replay(
            &replay_session_id,
            m_aeron_archive_t,
            recordingId,
            replayChannel.c_str(),
            replayStreamId,
            replayParams.params()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return replay_session_id;
    }

    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        ReplayParams &replayParams)
    {
        aeron_subscription_t *subscription;

        if (aeron_archive_replay(
            &subscription,
            m_aeron_archive_t,
            recordingId,
            replayChannel.c_str(),
            replayStreamId,
            replayParams.params()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<Subscription>(m_archiveCtxW.aeron()->aeron(), subscription, nullptr);
    }

    inline std::int64_t truncateRecording(std::int64_t recordingId, std::int64_t position)
    {
        int64_t count;

        if (aeron_archive_truncate_recording(
            &count,
            m_aeron_archive_t,
            recordingId,
            position) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::shared_ptr<Publication> addRecordedPublication(const std::string &channel, std::int32_t streamId)
    {
        aeron_publication_t *publication;

        if (aeron_archive_add_recorded_publication(
            &publication,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<Publication>(m_archiveCtxW.aeron()->aeron(), publication);
    }

    inline std::shared_ptr<ExclusivePublication> addRecordedExclusivePublication(const std::string &channel, std::int32_t streamId)
    {
        aeron_exclusive_publication_t *exclusivePublication;

        if (aeron_archive_add_recorded_exclusive_publication(
            &exclusivePublication,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<ExclusivePublication>(m_archiveCtxW.aeron()->aeron(), exclusivePublication);
    }

    inline void stopReplay(std::int64_t replaySessionId)
    {
        if (aeron_archive_stop_replay(m_aeron_archive_t, replaySessionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline void stopReplication(std::int64_t replicationId)
    {
        if (aeron_archive_stop_replication(m_aeron_archive_t, replicationId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline bool tryStopReplication(std::int64_t replicationId)
    {
        bool stopped;

        if (aeron_archive_try_stop_replication(
            &stopped,
            m_aeron_archive_t,
            replicationId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    inline std::int32_t listRecordingSubscriptions(
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        const recording_subscription_descriptor_consumer_t &consumer)
    {
        int32_t count;

        if (aeron_archive_list_recording_subscriptions(
            &count,
            m_aeron_archive_t,
            pseudoIndex,
            subscriptionCount,
            channelFragment.c_str(),
            streamId,
            applyStreamId,
            recording_subscription_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int64_t purgeRecording(std::int64_t recordingId)
    {
        int64_t deletedSegmentsCount;

        if (aeron_archive_purge_recording(
            &deletedSegmentsCount,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return deletedSegmentsCount;
    }

    inline std::int64_t replicate(
        std::int64_t srcRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        ReplicationParams &replicationParams)
    {
        int64_t replicationId;

       if (aeron_archive_replicate(
           &replicationId,
           m_aeron_archive_t,
           srcRecordingId,
           srcControlStreamId,
           srcControlChannel.c_str(),
           replicationParams.params()) < 0)
       {
           ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
       }

       return replicationId;
    }

    inline std::int32_t pollForRecordingSignals()
    {
        int32_t count;

        if (aeron_archive_poll_for_recording_signals(&count, m_aeron_archive_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline void detachSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        if (aeron_archive_detach_segments(
            m_aeron_archive_t,
            recordingId,
            newStartPosition) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    inline std::int64_t deleteDetachedSegments(std::int64_t recordingId)
    {
        int64_t count;

        if (aeron_archive_delete_detached_segments(
            &count,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int64_t attachSegments(std::int64_t recordingId)
    {
        int64_t count;

        if (aeron_archive_attach_segments(
            &count,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int64_t purgeSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        int64_t count;

        if (aeron_archive_purge_segments(
            &count,
            m_aeron_archive_t,
            recordingId,
            newStartPosition) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    inline std::int64_t migrateSegments(std::int64_t srcRecordingId, std::int64_t dstRecordingId)
    {
        int64_t count;

        if (aeron_archive_migrate_segments(
            &count,
            m_aeron_archive_t,
            srcRecordingId,
            dstRecordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    static std::int64_t segmentFileBasePosition(
        std::int64_t startPosition,
        std::int64_t position,
        std::int32_t termBufferLength,
        std::int32_t segmentFileLength)
    {
        return aeron_archive_segment_file_base_position(
            startPosition,
            position,
            termBufferLength,
            segmentFileLength);
    }


private:
    explicit AeronArchive(
        aeron_archive_t *aeron_archive,
        const std::shared_ptr<Aeron> &originalAeron,
        const recording_signal_consumer_t &recordingSignalConsumer) :
        m_aeron_archive_t(aeron_archive),
        m_archiveCtxW(aeron_archive_get_and_own_archive_context(m_aeron_archive_t))
    {
        // The following line divorces the aeron_t from the underlying aeron_archive
        aeron_archive_context_set_owns_aeron_client(m_archiveCtxW.m_aeron_archive_ctx_t, false);

        // Can't get the aeron_t via 'm_archiveCtxW.aeron()->aeron()' because m_archiveCtxW doesn't have an aeron set yet.
        // So use the C functions to acquire the underlying aeron_t.
        auto *aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(aeron_archive));

        m_archiveCtxW.setAeron(nullptr == originalAeron ? std::make_shared<Aeron>(aeron) : originalAeron);
        m_archiveCtxW.recordingSignalConsumer(recordingSignalConsumer);

        m_controlResponseSubscription = std::make_unique<Subscription>(
            aeron,
            aeron_archive_get_and_own_control_response_subscription(m_aeron_archive_t),
            nullptr);
    }

    aeron_archive_t *m_aeron_archive_t = nullptr;
    Context m_archiveCtxW;
    std::unique_ptr<Subscription> m_controlResponseSubscription = nullptr;

    static void recording_descriptor_consumer_func(
        aeron_archive_recording_descriptor_t *recording_descriptor,
        void *clientd)
    {
        auto consumer = *reinterpret_cast<recording_descriptor_consumer_t *>(clientd);

        RecordingDescriptor descriptor(
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

        consumer(descriptor);
    }

    static void recording_subscription_descriptor_consumer_func(
        aeron_archive_recording_subscription_descriptor_t *recording_subscription_descriptor,
        void *clientd)
    {
        auto consumer = *reinterpret_cast<recording_subscription_descriptor_consumer_t *>(clientd);

        RecordingSubscriptionDescriptor descriptor(
            recording_subscription_descriptor->control_session_id,
            recording_subscription_descriptor->correlation_id,
            recording_subscription_descriptor->subscription_id,
            recording_subscription_descriptor->stream_id,
            recording_subscription_descriptor->stripped_channel);

        consumer(descriptor);
    }
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_H
