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
#ifndef AERON_ARCHIVE_REPLAY_MERGE_H
#define AERON_ARCHIVE_REPLAY_MERGE_H

#include "AeronArchive.h"

namespace aeron {
namespace archive {
namespace client {

constexpr const std::int64_t REPLAY_MERGE_LIVE_ADD_THRESHOLD = LogBufferDescriptor::TERM_MIN_LENGTH / 4;
constexpr const std::int64_t REPLAY_MERGE_REPLAY_REMOVE_THRESHOLD = 0;

/**
 * Replay a recorded stream from a starting position and merge with live stream to consume a full history of a stream.
 * <p>
 * Once constructed the either of #poll(FragmentHandler, int) or #doWork() interleaved with consumption
 * of the #image() should be called in a duty cycle loop until #isMerged() is true,
 * after which the ReplayMerge can go out of scope and continued usage can be made of the Image or its
 * parent Subscription.
 */
class ReplayMerge
{
public:
    enum State : std::int8_t
    {
        AWAIT_INITIAL_RECORDING_POSITION,
        AWAIT_REPLAY,
        AWAIT_CATCH_UP,
        AWAIT_CURRENT_RECORDING_POSITION,
        AWAIT_STOP_REPLAY,
        MERGED,
        CLOSED
    };

    /**
     * Create a ReplayMerge to manage the merging of a replayed stream and switching to live stream as
     * appropriate.
     *
     * @param subscription to use for the replay and live stream. Must be a multi-destination subscription.
     * @param archive to use for the replay.
     * @param replayChannel to use for the replay.
     * @param replayDestination to send the replay to and the destination added by the Subscription.
     * @param liveDestination for the live stream and the destination added by the Subscription.
     * @param recordingId for the replay.
     * @param startPosition for the replay.
     */
    ReplayMerge(
        std::shared_ptr<Subscription> subscription,
        std::shared_ptr<AeronArchive> archive,
        const std::string& replayChannel,
        const std::string& replayDestination,
        const std::string& liveDestination,
        std::int64_t recordingId,
        std::int64_t startPosition);

    ~ReplayMerge();

    /**
     * Process the operation of the merge. Do not call the processing of fragments on the subscription.
     *
     * @return indication of work done processing the merge.
     */
    inline int doWork()
    {
        int workCount = 0;

        switch (m_state)
        {
            case State::AWAIT_INITIAL_RECORDING_POSITION:
                workCount += awaitInitialRecordingPosition();
                break;

            case State::AWAIT_REPLAY:
                workCount += awaitReplay();
                break;

            case State::AWAIT_CATCH_UP:
                workCount += awaitCatchUp();
                break;

            case State::AWAIT_CURRENT_RECORDING_POSITION:
                workCount += awaitUpdatedRecordingPosition();
                break;

            case State::AWAIT_STOP_REPLAY:
                workCount += awaitStopReplay();
                break;

            default:
                break;
        }

        return workCount;
    }

    /**
     * Poll the Image used for the merging replay and live stream. The ReplayMerge#doWork method
     * will be called before the poll so that processing of the merge can be done.
     *
     * @param fragmentHandler to call for fragments
     * @param fragmentLimit for poll call
     * @return number of fragments processed.
     */
    template<typename F>
    inline int poll(F&& fragmentHandler, int fragmentLimit)
    {
        doWork();
        return nullptr == m_image ? 0 : m_image->poll(fragmentHandler, fragmentLimit);
    }

    /**
     * State of this ReplayMerge.
     *
     * @return state of this ReplayMerge.
     */
    inline State state()
    {
        return m_state;
    }

    /**
     * Is the live stream merged and the replay stopped?
     *
     * @return true if live stream is merged and the replay stopped or false if not.
     */
    inline bool isMerged()
    {
        return m_state == State::MERGED;
    }

    /**
     * The Image used for the replay and live stream.
     *
     * @return the Image used for the replay and live stream.
     */
    inline std::shared_ptr<Image> image()
    {
        return m_image;
    }

    /**
     * Is the live destination added to the subscription?
     *
     * @return true if live destination added or false if not.
     */
    inline bool isLiveAdded()
    {
        return m_isLiveAdded;
    }

private:
    const std::shared_ptr<Subscription> m_subscription;
    const std::shared_ptr<AeronArchive> m_archive;
    const std::string m_replayChannel;
    const std::string m_replayDestination;
    const std::string m_liveDestination;
    const std::int64_t m_recordingId;
    const std::int64_t m_startPosition;
    const std::int64_t m_liveAddThreshold;
    const std::int64_t m_replayRemoveThreshold;

    State m_state = AWAIT_INITIAL_RECORDING_POSITION;
    std::shared_ptr<Image> m_image = nullptr;
    std::int64_t m_activeCorrelationId = aeron::NULL_VALUE;
    std::int64_t m_initialMaxPosition = aeron::NULL_VALUE;
    std::int64_t m_nextTargetPosition = aeron::NULL_VALUE;
    std::int64_t m_replaySessionId = aeron::NULL_VALUE;
    bool m_isLiveAdded = false;
    bool m_isReplayActive = false;

    inline void state(State state)
    {
        //std::cout << m_state << "->" << state << std::endl;
        m_state = state;
    }

    inline bool shouldAddLiveDestination(std::int64_t position)
    {
        return !m_isLiveAdded && (m_nextTargetPosition - position) <= m_liveAddThreshold;
    }

    inline bool shouldStopAndRemoveReplay(std::int64_t position)
    {
        return m_nextTargetPosition > m_initialMaxPosition &&
            m_isLiveAdded && (m_nextTargetPosition - position) <= m_replayRemoveThreshold;
    }

    int awaitInitialRecordingPosition();
    int awaitReplay();
    int awaitCatchUp();
    int awaitUpdatedRecordingPosition();
    int awaitStopReplay();

    static bool pollForResponse(AeronArchive& archive, std::int64_t correlationId);
};

}}}
#endif //AERON_ARCHIVE_REPLAY_MERGE_H
