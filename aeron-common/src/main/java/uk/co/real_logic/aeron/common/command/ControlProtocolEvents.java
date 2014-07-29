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
package uk.co.real_logic.aeron.common.command;

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
    /** Request Term */
    public static final int CLEAN_TERM_BUFFER = 0x06;
    /** Heartbeat for Publication */
    public static final int KEEPALIVE_PUBLICATION = 0x07;
    /** Heartbeat for Subscriber */
    public static final int KEEPALIVE_SUBSCRIPTION = 0x08;

    // Media Driver to Clients

    /** Error Response */
    public static final int ON_ERROR = 0x0F01;
    /** New subscription Buffer Notification */
    public static final int ON_NEW_CONNECTED_SUBSCRIPTION = 0x0F02;
    /** New publication Buffer Notification */
    public static final int ON_NEW_PUBLICATION = 0x0F03;
    /** Operation Succeeded */
    public static final int ON_OPERATION_SUCCESS = 0x0F04;
    /** Heartbeat from driver to clients */
    public static final int ON_DRIVER_HEARTBEAT = 0x0F05;
}
