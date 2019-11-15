/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.cluster.service;

import org.agrona.ErrorHandler;

/**
 * {@link ErrorHandler} that can insert into a chain of responsibility so it handles an error and then delegates
 * on to the next in the chain. This allows for taking action pre or post invocation of the next delegate.
 * <p>
 * Implementations are responsible for calling the next in the chain.
 */
public interface DelegatingErrorHandler extends ErrorHandler
{
    // TODO: replace with Agrona version.

    /**
     * Set the next {@link ErrorHandler} to be called in a chain.
     *
     * @param errorHandler the next {@link ErrorHandler} to be called in a chain.
     */
    void next(ErrorHandler errorHandler);
}
