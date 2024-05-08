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

import javax.xml.bind.annotation.XmlTransient;
import java.util.concurrent.TimeUnit;

/**
 * A handy class for storing data that gets serialized into json
 */
public class ConfigInfo
{
    public final String id;
    public final ExpectedConfig expectations;

    /**
     */
    public ConfigInfo()
    {
        this.id = null;
        expectations = new ExpectedConfig();
    }

    /**
     * @param id the unique identifier for this block o' config information
     */
    public ConfigInfo(final String id)
    {
        this.id = id;
        expectations = new ExpectedConfig();
    }

    @XmlTransient
    public boolean foundPropertyName = false;
    @XmlTransient
    public boolean foundDefault = false;

    public String propertyNameDescription;
    public String propertyNameFieldName;
    public String propertyNameClassName;
    public String propertyName;
    public String defaultDescription;
    public String defaultFieldName;
    public String defaultClassName;
    public Object defaultValue;
    public DefaultType defaultValueType = DefaultType.UNDEFINED;
    public Object overrideDefaultValue;
    public DefaultType overrideDefaultValueType = DefaultType.UNDEFINED;
    public String uriParam;
    public boolean hasContext = true;
    public String context;
    public String contextDescription;
    public Boolean isTimeValue;
    public TimeUnit timeUnit;
    public boolean deprecated = false;
}
