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
package io.aeron.config;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("checkstyle:MissingJavadocType")
public class ConfigInfo
{
    @JsonProperty
    public final String id;
    @JsonProperty
    public final ExpectedConfig expectations;

    public ConfigInfo(@JsonProperty("id") final String id)
    {
        this.id = id;
        expectations = new ExpectedConfig();
    }

    public boolean foundPropertyName = false;
    public boolean foundDefault = false;
    @JsonProperty
    public String propertyNameDescription;
    @JsonProperty
    public String propertyNameFieldName;
    @JsonProperty
    public String propertyNameClassName;
    @JsonProperty
    public String propertyName;
    @JsonProperty
    public String defaultDescription;
    @JsonProperty
    public String defaultFieldName;
    @JsonProperty
    public String defaultClassName;
    @JsonProperty
    public Object defaultValue;
    @JsonProperty
    public DefaultType defaultValueType = DefaultType.UNDEFINED;
    @JsonProperty
    public Object overrideDefaultValue;
    @JsonProperty
    public DefaultType overrideDefaultValueType = DefaultType.UNDEFINED;
    @JsonProperty
    public String uriParam;
    @JsonProperty
    public String context;
}
