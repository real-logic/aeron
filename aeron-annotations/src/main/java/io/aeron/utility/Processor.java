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
package io.aeron.utility;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;

/**
 * abstract processor
 */
public abstract class Processor extends AbstractProcessor
{
    private boolean printNotes = false;

    private Diagnostic.Kind errorKind;

    /**
     * {@inheritDoc}
     */
    @Override
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(final ProcessingEnvironment processingEnv)
    {
        printNotes = System.getProperty(getPrintNotesPropertyName(), "false").equalsIgnoreCase("true");
        errorKind =
            System.getProperty(getFailOnErrorPropertyName(), "false").equalsIgnoreCase("true") ?
            Diagnostic.Kind.ERROR :
            Diagnostic.Kind.NOTE;
        super.init(processingEnv);
    }

    protected abstract String getPrintNotesPropertyName();

    protected abstract String getFailOnErrorPropertyName();

    protected String getDocComment(final Element element)
    {
        final String description = processingEnv.getElementUtils().getDocComment(element);
        if (description == null)
        {
            error("no javadoc found", element);
            return "NO DESCRIPTION FOUND";
        }

        return description.trim();
    }

    protected void error(final String errMsg)
    {
        error(errMsg, null);
    }

    protected void error(final String errMsg, final Element element)
    {
        printMessage(errorKind, errMsg, element);
    }

    protected void note(final String msg)
    {
        note(msg, null);
    }

    protected void note(final String msg, final Element element)
    {
        if (printNotes)
        {
            printMessage(Diagnostic.Kind.NOTE, msg, element);
        }
    }

    private void printMessage(final Diagnostic.Kind kind, final String msg, final Element element)
    {
        processingEnv.getMessager().printMessage(kind, msg, element);
    }
}
