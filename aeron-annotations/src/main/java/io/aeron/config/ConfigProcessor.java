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

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.*;
import java.util.*;

/**
 * ConfigOption processor
 */
@SupportedAnnotationTypes("io.aeron.config.Config")
public class ConfigProcessor extends AbstractProcessor
{
    private static final String[] PROPERTY_NAME_SUFFIXES = new String[] {"_PROP_NAME"};

    private static final String[] DEFAULT_SUFFIXES = new String[] {"_DEFAULT", "_DEFAULT_NS"};

    private final Diagnostic.Kind kind = Diagnostic.Kind.NOTE;

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
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        final Map<String, ConfigInfo> configInfoMap = new HashMap<>();

        for (final TypeElement annotation : annotations)
        {
            for (final Element element : roundEnv.getElementsAnnotatedWith(annotation))
            {
                try
                {
                    if (element instanceof VariableElement)
                    {
                        processElement(configInfoMap, (VariableElement)element);
                    }
                    else if (element instanceof ExecutableElement)
                    {
                        processExecutableElement(configInfoMap, (ExecutableElement)element);
                    }
                    else
                    {
                    }
                }
                catch (final Exception e)
                {
                    error("an error occurred processing an element: " + e.getMessage(), element);
                    e.printStackTrace(System.err);
                }
            }
        }

        if (!configInfoMap.isEmpty())
        {
            try
            {
                configInfoMap.forEach(this::deriveCExpectations);
                configInfoMap.forEach(this::sanityCheck);
            }
            catch (final Exception e)
            {
                e.printStackTrace(System.err);
            }

            try
            {
                final FileObject resourceFile = processingEnv.getFiler()
                    .createResource(StandardLocation.NATIVE_HEADER_OUTPUT, "", "config-info.json");
                try (OutputStream out = resourceFile.openOutputStream())
                {
                    new ObjectMapper().writeValue(out, configInfoMap.values());
                }
            }
            catch (final IOException e)
            {
                processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR,
                    "an error occurred while writing output: " + e.getMessage());
            }
        }

        return false;
    }

    @SuppressWarnings({ "checkstyle:MethodLength", "checkstyle:LineLength" })
    private void processElement(final Map<String, ConfigInfo> configInfoMap, final VariableElement element)
    {
        final Config config = element.getAnnotation(Config.class);

        if (Objects.isNull(config))
        {
            error("element found with no expected annotations", element);
            return;
        }

        final String id;
        final ConfigInfo configInfo;
        final Object constantValue = element.getConstantValue();
        switch (getConfigType(element, config))
        {
            case PROPERTY_NAME:
                id = getConfigId(element, PROPERTY_NAME_SUFFIXES, config.id());
                configInfo = configInfoMap.computeIfAbsent(id, ConfigInfo::new);
                if (configInfo.foundPropertyName)
                {
                    error("duplicate config option info for id: " + id + ".  Previous definition found at " +
                        configInfo.propertyNameClassName + ":" + configInfo.propertyNameFieldName, element);
                    return;
                }
                configInfo.foundPropertyName = true;

                configInfo.propertyNameFieldName = element.toString();
                configInfo.propertyNameClassName = element.getEnclosingElement().toString();
                configInfo.propertyNameDescription = processingEnv.getElementUtils().getDocComment(element).trim();

                if (constantValue instanceof String)
                {
                    configInfo.propertyName = (String)constantValue;
                }
                else
                {
                    error("Property names must be Strings", element);
                }
                break;
            case DEFAULT:
                id = getConfigId(element, DEFAULT_SUFFIXES, config.id());
                configInfo = configInfoMap.computeIfAbsent(id, ConfigInfo::new);
                if (configInfo.foundDefault)
                {
                    error("duplicate config default info for id: " + id + ".  Previous definition found at " +
                        configInfo.defaultClassName + ":" + configInfo.defaultFieldName, element);
                    return;
                }
                configInfo.foundDefault = true;

                configInfo.defaultFieldName = element.toString();
                configInfo.defaultClassName = element.getEnclosingElement().toString();
                configInfo.defaultDescription = processingEnv.getElementUtils().getDocComment(element).trim();

                if (constantValue != null)
                {
                    configInfo.defaultValue = constantValue;
                    configInfo.defaultValueType = DefaultType.fromCanonicalName(constantValue.getClass().getCanonicalName());
                }
                break;
            default:
                error("unable to determine config type", element);
                return;
        }

        if (!config.uriParam().isEmpty())
        {
            configInfo.uriParam = config.uriParam();
        }

        if (!DefaultType.isUndefined(config.defaultType()))
        {
            if (DefaultType.isUndefined(configInfo.defaultValueType))
            {
                configInfo.overrideDefaultValueType = config.defaultType();
                switch (config.defaultType())
                {
                    case INT:
                        configInfo.overrideDefaultValue = config.defaultInt();
                        break;
                    case LONG:
                        configInfo.overrideDefaultValue = config.defaultLong();
                        break;
                    case BOOLEAN:
                        configInfo.overrideDefaultValue = config.defaultBoolean();
                        break;
                    case STRING:
                        configInfo.overrideDefaultValue = config.defaultString();
                        break;
                    default:
                        error("unhandled default type", element);
                        break;
                }
            }
            else
            {
                // TODO bad
            }
        }

        final ExpectedCConfig c = configInfo.expectations.c;

        if (!config.existsInC())
        {
            c.exists = false;
        }

        if (c.exists)
        {
            // TODO fix isEmpty - check for NPE
            if (c.envVarFieldName == null && !config.expectedCEnvVarFieldName().isEmpty())
            {
                c.envVarFieldName = config.expectedCEnvVarFieldName();
            }

            if (c.envVar == null && !config.expectedCEnvVar().isEmpty())
            {
                c.envVar = config.expectedCEnvVar();
            }

            if (c.defaultFieldName == null && !config.expectedCDefaultFieldName().isEmpty())
            {
                c.defaultFieldName = config.expectedCDefaultFieldName();
            }

            if (c.defaultValue == null && !config.expectedCDefault().isEmpty())
            {
                c.defaultValue = config.expectedCDefault();
            }
        }
    }

    private void processExecutableElement(final Map<String, ConfigInfo> configInfoMap, final ExecutableElement element)
    {
        final Config config = element.getAnnotation(Config.class);

        if (Objects.isNull(config))
        {
            error("element found with no expected annotations", element);
            return;
        }

        final String id = getConfigId(element, config.id());
        final ConfigInfo configInfo = configInfoMap.computeIfAbsent(id, ConfigInfo::new);

        final String methodName = element.toString();

        final String enclosingElementName = element.getEnclosingElement().toString();

        Element e = element.getEnclosingElement();
        while (e.getKind() != ElementKind.PACKAGE)
        {
            e = e.getEnclosingElement();
        }

        final String packageName = e.toString();

        configInfo.context = enclosingElementName.substring(packageName.length() + 1) + "." + methodName;
    }

    private Config.Type getConfigType(final VariableElement element, final Config config)
    {
        // use an explicitly configured type
        if (config.configType() != Config.Type.UNDEFINED)
        {
            return config.configType();
        }

        if (element.toString().endsWith("_PROP_NAME"))
        {
            return Config.Type.PROPERTY_NAME;
        }

        if (element.toString().contains("DEFAULT"))
        {
            return Config.Type.DEFAULT;
        }

        return Config.Type.UNDEFINED;
    }

    private String getConfigId(final ExecutableElement element, final String id)
    {
        if (null != id && !id.isEmpty())
        {
            return id;
        }

        final StringBuilder builder = new StringBuilder();

        for (final char next: element.toString().toCharArray())
        {
            if (Character.isLetter(next))
            {
                if (Character.isUpperCase(next))
                {
                    builder.append("_");
                }
                builder.append(Character.toUpperCase(next));
            }
        }

        return builder.toString();
    }

    private String getConfigId(final VariableElement element, final String[] suffixes, final String id)
    {
        if (null != id && !id.isEmpty())
        {
            return id;
        }

        final String fieldName = element.toString();

        for (final String suffix: suffixes)
        {
            if (fieldName.endsWith(suffix))
            {
                return fieldName.substring(0, fieldName.length() - suffix.length());
            }
        }

        error("unable to determine id for: " + fieldName, element);

        return fieldName;
    }

    private void deriveCExpectations(final String id, final ConfigInfo configInfo)
    {
        if (!configInfo.expectations.c.exists)
        {
            return; // skip it
        }

        try
        {
            final ExpectedCConfig c = configInfo.expectations.c;

            if (Objects.isNull(c.envVar) && configInfo.foundPropertyName)
            {
                c.envVar = configInfo.propertyName.toUpperCase().replace('.', '_');
            }

            if (Objects.isNull(c.envVarFieldName))
            {
                c.envVarFieldName = c.envVar + "_ENV_VAR";
            }

            if (Objects.isNull(c.defaultFieldName))
            {
                c.defaultFieldName = c.envVar + "_DEFAULT";
            }

            if (DefaultType.isUndefined(configInfo.overrideDefaultValueType))
            {
                if (Objects.isNull(c.defaultValue))
                {
                    c.defaultValue = configInfo.defaultValue;
                }

                if (Objects.isNull(c.defaultValueType))
                {
                    c.defaultValueType = configInfo.defaultValueType;
                }
            }
            else
            {
                if (Objects.isNull(c.defaultValue))
                {
                    c.defaultValue = configInfo.overrideDefaultValue;
                }

                if (Objects.isNull(c.defaultValueType))
                {
                    c.defaultValueType = configInfo.overrideDefaultValueType;
                }
            }
        }
        catch (final Exception e)
        {
            error("an error occurred while deriving C config expectations for: " + id);
            e.printStackTrace(System.err);
        }
    }

    private void sanityCheck(final String id, final ConfigInfo configInfo)
    {
        if (!configInfo.foundPropertyName)
        {
            insane(id, "no property name found");
        }

        if (configInfo.defaultValue == null && configInfo.overrideDefaultValue == null)
        {
            insane(id, "no default value found");
        }
    }

    private void insane(final String id, final String errMsg)
    {
        error("Configuration (" + id + "): " + errMsg);
    }

    private void error(final String errMsg)
    {
        processingEnv.getMessager().printMessage(kind, errMsg);
    }

    private void error(final String errMsg, final Element element)
    {
        processingEnv.getMessager().printMessage(kind, errMsg, element);
    }
}
