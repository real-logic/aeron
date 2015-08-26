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
package uk.co.real_logic.aeron.command;

/**
 * List of event types used in the control protocol between the
 * media driver and the core.
 */
public class ControlProtocolEvents
{
    // Clients to Media Driver

    /** Add Publication */
    public static final int ADD_PUBLICATION = 0x01;
    /** Remove Publication */
    public static final int REMOVE_PUBLICATION = 0x02;
    /** Add Subscriber */
    public static final int ADD_SUBSCRIPTION = 0x04;
    /** Remove Subscriber */
    public static final int REMOVE_SUBSCRIPTION = 0x05;
    /** Keepalive from Client */
    public static final int CLIENT_KEEPALIVE = 0x06;

    // Media Driver to Clients

    /** Error Response */
    public static final int ON_ERROR = 0x0F01;
    /** New subscription Buffer Notification */
    public static final int ON_AVAILABLE_IMAGE = 0x0F02;
    /** New publication Buffer Notification */
    public static final int ON_PUBLICATION_READY = 0x0F03;
    /** Operation Succeeded */
    public static final int ON_OPERATION_SUCCESS = 0x0F04;
    /** Inform client of timeout and removal of inactive image */
    public static final int ON_UNAVAILABLE_IMAGE = 0x0F05;
}
