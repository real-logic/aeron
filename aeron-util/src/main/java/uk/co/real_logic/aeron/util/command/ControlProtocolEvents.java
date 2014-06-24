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
    // Library to Media Driver

    /** Add Channel */
    public static final int ADD_PUBLICATION = 0x01;
    /** Remove Channel */
    public static final int REMOVE_PUBLICATION = 0x02;
    /** Remove Term */
    public static final int REMOVE_TERM = 0x03;
    /** Add Subscriber */
    public static final int ADD_SUBSCRIPTION = 0x04;
    /** Remove Subscriber */
    public static final int REMOVE_SUBSCRIPTION = 0x05;
    /** Request Term */
    public static final int REQUEST_CLEANED_TERM = 0x06;

    // Media Driver to Library

    /** Error Response */
    public static final int ERROR_RESPONSE = 0x07;
    /** Error Notification */
    public static final int ERROR_NOTIFICATION = 0x08;
    /** New Receive Buffer Notification */
    public static final int NEW_SUBSCRIPTION_BUFFER_NOTIFICATION = 0x09;
    /** New Send Buffer Notification */
    public static final int NEW_PUBLICATION_BUFFER_NOTIFICATION = 0x0A;

    // Within Media Driver between threads

    /** Receiver thread tells media driver it wants to create a term buffer */
    public static final int CREATE_TERM_BUFFER = 0xF0;

    /** Receiver thread tells media driver it wants to remove a term buffer */
    public static final int REMOVE_TERM_BUFFER = 0xF1;

}
