/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.version;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Version processor
 */
@SupportedAnnotationTypes("io.aeron.version.VersionType")
@SupportedOptions({"io.aeron.version", "io.aeron.gitsha"})
public class VersionProcessor extends AbstractProcessor
{
    /**
     * {@inheritDoc}
     */
    public SourceVersion getSupportedSourceVersion()
    {
        return SourceVersion.latest();
    }

    /**
     * {@inheritDoc}
     */
    public boolean process(final Set<? extends TypeElement> annotations, final RoundEnvironment roundEnv)
    {
        boolean claimed = false;
        for (final TypeElement annotation : annotations)
        {
            final Set<? extends Element> elementsAnnotatedWith = roundEnv.getElementsAnnotatedWith(annotation);
            for (final Element element : elementsAnnotatedWith)
            {
                final VersionType versionType = element.getAnnotation(VersionType.class);
                if (null == versionType)
                {
                    continue;
                }

                claimed = true;

                try
                {
                    final JavaFileObject sourceFile = processingEnv.getFiler().createSourceFile(versionType.value());
                    try (PrintWriter out = new PrintWriter(sourceFile.openWriter()))
                    {
                        final int lastDot = versionType.value().lastIndexOf('.');
                        final String packageName = -1 != lastDot ? versionType.value().substring(0, lastDot) : null;
                        final String className = -1 != lastDot ?
                            versionType.value().substring(lastDot + 1) : versionType.value();

                        final String versionString = processingEnv.getOptions().get("io.aeron.version");
                        final VersionInformation info = new VersionInformation(versionString);
                        final String gitSha = processingEnv.getOptions().get("io.aeron.gitsha");

                        if (null != packageName)
                        {
                            out.printf("package %s;%n", packageName);
                            out.println();
                            out.printf("public class %s%n", className);
                            out.printf("{%n");
                            out.printf("    public static final String VERSION = \"%s\";%n", versionString);
                            out.printf("    public static final int MAJOR_VERSION = %s;%n", info.major);
                            out.printf("    public static final int MINOR_VERSION = %s;%n", info.minor);
                            out.printf("    public static final int PATCH_VERSION = %s;%n", info.patch);
                            out.printf("    public static final String SUFFIX = \"%s\";%n", info.suffix);
                            out.printf("    public static final String GIT_SHA = \"%s\";%n", gitSha);
                            out.printf("}%n");
                        }
                    }
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        return claimed;
    }

    private static class VersionInformation
    {
        private static final Pattern VERSION_PATTERN = Pattern.compile("([0-9]+).([0-9]+).([0-9]+)(?:-(.+))?");
        private final int major;
        private final int minor;
        private final int patch;
        private final String suffix;

        VersionInformation(final String versionString)
        {
            final Matcher matcher = VERSION_PATTERN.matcher(versionString);
            if (!matcher.matches())
            {
                throw new IllegalArgumentException("The version string: '" + versionString + "' is not valid");
            }

            major = Integer.parseInt(matcher.group(1));
            minor = Integer.parseInt(matcher.group(2));
            patch = Integer.parseInt(matcher.group(3));
            suffix = matcher.group(4);
        }
    }
}
