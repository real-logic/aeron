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

import java.lang.annotation.*;

/**
 * Annotation to indicate this is a config option
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.SOURCE)
public @interface Config
{
    /**
     * Type is used to indicate whether the annotation is marking a property name or a default value
     */
    enum Type
    {
        UNDEFINED, PROPERTY_NAME, DEFAULT
    }

    /**
     * @return what type of field is being annotated
     */
    Type configType() default Type.UNDEFINED;

    /**
     * @return the unique id that ties together all the usages of the annotation across fields/methods
     */
    String id() default "";

    /**
     * @return the uri parameter (if any) associated with this option
     */
    String uriParam() default "";

    /**
     * @return whether or not this config option exists in the C code
     */
    boolean existsInC() default true;

    /**
     * @return the expected C #define name that will be set with the env variable name for this option
     */
    String expectedCEnvVarFieldName() default "";

    /**
     * @return the expected C env variable name for this option
     */
    String expectedCEnvVar() default "";

    /**
     * @return the expected C #define name that will be set with the default value for this option
     */
    String expectedCDefaultFieldName() default "";

    /**
     * @return the expected C default value for this option
     */
    String expectedCDefault() default "";

    /**
     * @return what's the type of default (string, int, etc...)
     */
    DefaultType defaultType() default DefaultType.UNDEFINED;

    /**
     * @return specify the default boolean, if defaultType is BOOLEAN
     */
    boolean defaultBoolean() default false;

    /**
     * @return specify the default int, if defaultType is INT
     */
    int defaultInt() default 0;

    /**
     * @return specify the default long, if defaultType is LONG
     */
    long defaultLong() default 0;

    /**
     * @return specify the default string, if defaultType is STRING
     */
    String defaultString() default "";
}
