/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.counter.validation;

import io.aeron.counter.CounterInfo;
import io.aeron.validation.Grep;

import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class Validator
{
    static ValidationReport validate(
        final Collection<CounterInfo> counterInfoCollection,
        final String sourceDir)
    {
        return new Validator(sourceDir).validate(counterInfoCollection).report;
    }

    private final String sourceDir;
    private final ValidationReport report;

    private Validator(final String sourceDir)
    {
        this.sourceDir = sourceDir;
        this.report = new ValidationReport();
    }

    private Validator validate(final Collection<CounterInfo> counterInfoCollection)
    {
        counterInfoCollection.forEach(this::validateCExpectations);

        identifyExtraCCounters(counterInfoCollection);

        return this;
    }

    private void identifyExtraCCounters(final Collection<CounterInfo> counterInfoCollection)
    {
        final Pattern compiledPattern = Pattern.compile("#define[ \t]+([A-Z_]+)[ \t]+\\([0-9]+\\)");
        final List<String> expectedCNames = counterInfoCollection
            .stream()
            .filter(counterInfo -> counterInfo.existsInC)
            .map(counterInfo -> counterInfo.expectedCName)
            .collect(Collectors.toList());

        final String pattern = "#define[ \t]+AERON_COUNTER_([A-Z_]+)_TYPE_ID[ \t]+\\([0-9]+\\)";
        final Grep grep = Grep.execute(pattern, sourceDir);

        grep.forEach((fileAndLineNo, line) ->
        {
            final Matcher matcher = compiledPattern.matcher(line);
            if (matcher.find())
            {
                final String name = matcher.group(1);

                if (expectedCNames.stream().noneMatch(cName -> cName.equals(name)))
                {
                    report.addValidation(false, name,
                        "Found C counter with no matching Java counter - " + fileAndLineNo);
                }
            }
            else
            {
                System.err.println("malformed line: " + line);
            }
        });
    }

    private void validateCExpectations(final CounterInfo counterInfo)
    {
        if (counterInfo.existsInC)
        {
            report.addValidation(counterInfo, this::validate);
        }
    }

    private void validate(final Validation validation, final CounterInfo counterInfo)
    {
        /* Expectations:
         * #define AERON_COUNTER_SOME_NAME (50)
         */
        final String pattern = "#define[ \t]+" + counterInfo.expectedCName + "[ \t]+\\([0-9]+\\)";
        final Grep grep = Grep.execute(pattern, sourceDir);
        if (grep.success())
        {

            final Matcher matcher =
                Pattern.compile("#define[ \t]+[A-Z_]+[ \t]+\\(([0-9]+)\\)").matcher(grep.getOutput());
            if (matcher.find())
            {
                final String id = matcher.group(1);

                try
                {
                    if (counterInfo.id == Integer.parseInt(id))
                    {
                        validation.valid("Expected ID found in " + grep.getFilenameAndLine());
                    }
                    else
                    {
                        validation.invalid("Incorrect ID found.  Expected: " + counterInfo.id + " but found: " + id);
                    }
                }
                catch (final NumberFormatException numberFormatException)
                {
                    validation.invalid("Unable to parse ID.  Expected a number but found: " + id);
                }
            }
            else
            {
                validation.invalid("WHAT??");
            }
        }
        else
        {
            validation.invalid("Expected ID NOT found.  `grep` command:\n" + grep.getCommandString());
        }
    }
}
