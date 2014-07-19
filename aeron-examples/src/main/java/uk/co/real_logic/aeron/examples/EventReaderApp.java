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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.common.event.EventReader;

/**
 * Simple application for reading of event log
 */
public class EventReaderApp
{
    public static void main(final String args[])
    {
        final EventReader eventReader = new EventReader(new EventReader.Context().deleteOnExit(true));

        while (true)
        {
            eventReader.read(System.out::println);

            try
            {
                Thread.sleep(1);
            }
            catch (final Exception ex)
            {
                ex.printStackTrace();
            }
        }
    }
}
