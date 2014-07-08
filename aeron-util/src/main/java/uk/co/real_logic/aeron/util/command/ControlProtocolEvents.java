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
package uk.co.real_logic.aeron.util.command;

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

    // Media Driver to Clients

    /** Error Response */
    public static final int ERROR_RESPONSE = 0x07;
    /** Error Notification */
    public static final int ON_ERROR = 0x08;
    /** New subscription Buffer Notification */
    public static final int ON_NEW_CONNECTED_SUBSCRIPTION = 0x09;
    /** New publication Buffer Notification */
    public static final int ON_NEW_PUBLICATION = 0x0A;
    /** Operation Succeeded */
    public static final int OPERATION_SUCCEEDED = 0x0B;

    // Within Media Driver between threads

    /** Receiver thread tells media driver it wants to create a connection */
    public static final int CREATE_CONNECTED_SUBSCRIPTION = 0xF0;

    /** Receiver thread tells media driver it wants to remove a connection */
    public static final int REMOVE_CONNECTED_SUBSCRIPTION = 0xF1;
}
