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
package uk.co.real_logic.aeron.driver;

/**
 * Utility functions for using Strings.
 */
public class Strings
{
    /**
     * Is a string null or empty?
     *
     * @param value to be tested.
     * @return true if the value is null or an empty string.
     */
    public static boolean isEmpty(final String value)
    {
        return null == value || value.isEmpty();
    }

    /**
     * Parse an int from a String. If the String is null then return the defaultValue.
     *
     * @param value        to be parsed
     * @param defaultValue to be used if the String value is null.
     * @return the int value of the string or the default on null.
     */
    public static int parseIntOrDefault(final String value, final int defaultValue)
    {
        if (null == value)
        {
            return defaultValue;
        }

        return Integer.parseInt(value);
    }
}
