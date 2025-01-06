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
package io.aeron.counter;

import io.aeron.utility.ElementIO;
import io.aeron.utility.Processor;

import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AeronCounter processor.
 */
@SupportedAnnotationTypes("io.aeron.counter.AeronCounter")
public class CounterProcessor extends Processor
{
    protected String getEnabledPropertyName()
    {
        return "aeron.build.counterProcessor.enabled";
    }

    protected String getPrintNotesPropertyName()
    {
        return "aeron.build.counterProcessor.printNotes";
    }

    protected String getFailOnErrorPropertyName()
    {
        return "aeron.build.counterProcessor.failOnError";
    }

    /**
     * {@inheritDoc}
     */
    public void doProcess(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        final Map<String, CounterInfo> counterInfoMap = new HashMap<>();

        for (final TypeElement annotation : annotations)
        {
            for (final Element element : roundEnv.getElementsAnnotatedWith(annotation))
            {
                try
                {
                    if (element instanceof VariableElement)
                    {
                        processElement(counterInfoMap, (VariableElement)element);
                    }
                    else
                    {
                        // TODO
                    }
                }
                catch (final Exception e)
                {
                    error("an error occurred processing an element: " + e.getMessage(), element);
                    e.printStackTrace(System.err);
                }
            }
        }

        if (!counterInfoMap.isEmpty())
        {
            try
            {
                final FileObject resourceFile = processingEnv.getFiler()
                    .createResource(StandardLocation.NATIVE_HEADER_OUTPUT, "", "counter-info.dat");

                ElementIO.write(resourceFile, counterInfoMap.values());
            }
            catch (final Exception e)
            {
                e.printStackTrace(System.err);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "an error occurred while writing output: " + e.getMessage());
            }
        }
    }

    private void processElement(final Map<String, CounterInfo> counterInfoMap, final VariableElement element)
    {
        final AeronCounter counter = element.getAnnotation(AeronCounter.class);

        if (Objects.isNull(counter))
        {
            error("element found with no expected annotations", element);
            return;
        }

        final Matcher matcher = Pattern.compile("^([A-Z_]+)_TYPE_ID$").matcher(element.toString());
        if (!matcher.find())
        {
            error("unable to determine type and/or id", element);
            return;
        }

        final CounterInfo counterInfo = new CounterInfo(matcher.group(1));

        if (null != counterInfoMap.put(counterInfo.name, counterInfo))
        {
            error("duplicate counters found", element);
            return;
        }

        counterInfo.counterDescription = getDocComment(element);

        final Object constantValue = element.getConstantValue();
        if (constantValue instanceof Integer)
        {
            counterInfo.id = (Integer)constantValue;
        }
        else
        {
            error("Counter value must be an Integer", element);
        }

        if (!counter.existsInC())
        {
            note("Counter isn't expected to exist in C", element);
            counterInfo.existsInC = false;
        }

        if (counterInfo.existsInC)
        {
            final StringBuilder builder = new StringBuilder();

            builder.append("AERON_COUNTER_");

            if (counter.expectedCName().isEmpty())
            {
                builder.append(counterInfo.name.replaceAll("^DRIVER_", ""));
            }
            else
            {
                note("Counter's C name is overridden", element);

                builder.append(counter.expectedCName());
            }

            builder.append("_TYPE_ID");

            counterInfo.expectedCName = builder.toString();
        }
    }
}
