/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common;

public interface Agent
{
    /**
     * An agent should implement this method to do its work.
     *
     * The boolean return value is used for implementing a backoff strategy that can be employed when no work is
     * currently available for the agent to process.
     *
     * @return true if work has been done otherwise false to indicate no work was currently available.
     */
    int doWork() throws Exception;

    /**
     * To be overridden by Agents that which to do resource cleanup on close.
     */
    default void onClose()
    {
        // default to do nothing unless you want to handle the notification.
    }

    /**
     * Get the name of this agent's role.
     *
     * @return the name of this agent's role.
     */
    String roleName();

}
