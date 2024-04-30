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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A handy class for storing data that gets serialized into json
 */
public class CounterInfo
{
    @JsonProperty
    public final String name;

    /**
     * @param name the name of the counter
     */
    public CounterInfo(@JsonProperty("name") final String name)
    {
        this.name = name;
    }

    @JsonProperty
    public int id;
    @JsonProperty
    public String counterDescription;
    @JsonProperty
    public String expectedCName;
}
