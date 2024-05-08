/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.counter;

/**
 * A handy class for storing data that gets serialized into json
 */
public class CounterInfo
{
    public final String name;

    /**
     */
    public CounterInfo()
    {
        this.name = null;
    }

    /**
     * @param name the name of the counter
     */
    public CounterInfo(final String name)
    {
        this.name = name;
    }

    public int id;
    public String counterDescription;
    public String expectedCName;
}
