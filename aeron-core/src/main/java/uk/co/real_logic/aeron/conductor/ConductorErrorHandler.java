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
package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.InvalidDestinationHandler;
import uk.co.real_logic.aeron.util.command.SubscriberMessageFlyweight;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.ErrorFlyweight;

/**
 * Maps the error messages that come back from the conductor protocol into different error handling interfaces.
 */
public class ConductorErrorHandler
{
    private final ErrorFlyweight errorHeader = new ErrorFlyweight();
    private final SubscriberMessageFlyweight subscriberMessage  = new SubscriberMessageFlyweight();
    private final InvalidDestinationHandler invalidDestination;

    public ConductorErrorHandler(final InvalidDestinationHandler invalidDestination)
    {
        this.invalidDestination = invalidDestination;
    }

    public void onErrorResponse(final AtomicBuffer buffer, final int index, final int length)
    {
        errorHeader.wrap(buffer, index);

        switch (errorHeader.errorCode())
        {
            case INVALID_DESTINATION:
                subscriberMessage.wrap(buffer, errorHeader.offendingHeaderOffset());
                invalidDestination.onInvalidDestination(subscriberMessage.destination());
                break;
        }
    }
}
