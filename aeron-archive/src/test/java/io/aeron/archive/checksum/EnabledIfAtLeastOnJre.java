/*
 * Copyright 2019 Real Logic Ltd.
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
package io.aeron.archive.checksum;

import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.*;

/**
 * {@code @EnableIfAtLeastOnJre} is used to signal that the annotated test class or
 * test method is only <em>enabled</em> if the current version of the JRE is greater
 * or equal to the configured minimal version.
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(EnabledIfAtLeastOnJreCondition.class)
public @interface EnabledIfAtLeastOnJre
{
    /**
     * Minimal JRE version on which test can be executed.
     *
     * @return minimal JRE version
     */
    JRE value();
}
