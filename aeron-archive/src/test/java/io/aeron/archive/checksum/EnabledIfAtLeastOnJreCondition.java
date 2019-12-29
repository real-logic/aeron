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
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.enabled;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

public class EnabledIfAtLeastOnJreCondition implements ExecutionCondition
{
    private static final ConditionEvaluationResult ENABLED_BY_DEFAULT = enabled("@EnableIfAtLeastOnJre is not present");

    static final ConditionEvaluationResult ENABLED_ON_CURRENT_JRE =
        enabled("Enabled on JRE version: " + System.getProperty("java.version"));

    static final ConditionEvaluationResult DISABLED_ON_CURRENT_JRE =
        disabled("Disabled on JRE version: " + System.getProperty("java.version"));

    private static final JRE CURRENT_JRE = Stream.of(JRE.values())
        .filter(JRE::isCurrentVersion).findFirst().orElse(JRE.OTHER);

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context)
    {
        final Optional<EnabledIfAtLeastOnJre> optional = findAnnotation(context
            .getElement(), EnabledIfAtLeastOnJre.class);
        if (optional.isPresent())
        {
            final JRE version = optional.get().value();
            if (version.isCurrentVersion() ||
                CURRENT_JRE.compareTo(version) > 0 && (JRE.OTHER != CURRENT_JRE || JRE.OTHER == version))
            {
                return ENABLED_ON_CURRENT_JRE;
            }
            return DISABLED_ON_CURRENT_JRE;
        }
        return ENABLED_BY_DEFAULT;
    }

}
