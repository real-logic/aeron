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

#ifndef AERON_FEEDBACKDELAYGENERATOR_H
#define AERON_FEEDBACKDELAYGENERATOR_H

namespace aeron { namespace driver {

class FeedbackDelayGenerator
{
public:

    FeedbackDelayGenerator(bool feedbackImmediately) : m_feedbackImmediately(feedbackImmediately)
    {}

    virtual ~FeedbackDelayGenerator() = default;

    /**
     * Generate a new delay value
     *
     * @return delay value in nanoseconds
     */
    virtual std::int64_t generateDelay() = 0;

    /**
     * Should feedback be immediately sent?
     *
     * @return whether feedback should be immediate or not
     */
    bool shouldFeedbackImmediately()
    {
        return false;
    }

private:
    bool m_feedbackImmediately;
};

class StaticFeedbackDelayGenerator : public FeedbackDelayGenerator
{
public:
    StaticFeedbackDelayGenerator(std::int64_t delay, bool feedbackImmediately)
        : FeedbackDelayGenerator(feedbackImmediately), m_delay(delay)
    {
    }

    virtual std::int64_t generateDelay() override
    {
        return m_delay;
    }

private:
    std::int64_t m_delay;
};

}}

#endif //AERON_FEEDBACKDELAYGENERATOR_H
