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
package io.aeron.version;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedOptions;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Version processor.
 */
@SupportedAnnotationTypes("io.aeron.version.Versioned")
@SupportedOptions({"io.aeron.version", "io.aeron.gitsha"})
public class VersionProcessor extends AbstractProcessor
{
    private static final String VERSION_IMPL =
        "\n" +
        "    @Override\n" +
        "    public String toString()\n" +
        "    {\n" +
        "        return VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int majorVersion()\n" +
        "    {\n" +
        "        return MAJOR_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int minorVersion()\n" +
        "    {\n" +
        "        return MINOR_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public int patchVersion()\n" +
        "    {\n" +
        "        return PATCH_VERSION;\n" +
        "    }\n" +
        "\n" +
        "    public String gitSha()\n" +
        "    {\n" +
        "        return GIT_SHA;\n" +
        "    }";

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
        for (final TypeElement annotation : annotations)
        {
            final Set<? extends Element> elementsAnnotatedWith = roundEnv.getElementsAnnotatedWith(annotation);
            for (final Element element : elementsAnnotatedWith)
            {
                final PackageElement pkg = processingEnv.getElementUtils().getPackageOf(element);
                final String packageName = pkg.getQualifiedName().toString();
                final String className = element.getSimpleName() + "Version";

                try
                {
                    final JavaFileObject sourceFile = processingEnv.getFiler().createSourceFile(
                        packageName + '.' + className);
                    try (PrintWriter out = new PrintWriter(sourceFile.openWriter()))
                    {
                        final String versionString = processingEnv.getOptions().get("io.aeron.version");
                        final VersionInformation info = new VersionInformation(versionString);
                        final String gitSha = processingEnv.getOptions().get("io.aeron.gitsha");

                        out.printf("package %s;%n", packageName);
                        out.println();
                        out.printf("public class %s%n implements io.aeron.version.Version%n", className);
                        out.printf("{%n");
                        out.printf("    public static final String VERSION = \"%s\";%n", versionString);
                        out.printf("    public static final int MAJOR_VERSION = %s;%n", info.major);
                        out.printf("    public static final int MINOR_VERSION = %s;%n", info.minor);
                        out.printf("    public static final int PATCH_VERSION = %s;%n", info.patch);
                        out.printf("    public static final String GIT_SHA = \"%s\";%n", gitSha);
                        out.println(VERSION_IMPL);
                        out.printf("}%n");
                    }
                }
                catch (final IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        return false;
    }

    private static class VersionInformation
    {
        private static final Pattern VERSION_PATTERN = Pattern.compile("([0-9]+).([0-9]+).([0-9]+)(?:-.+)?");
        private final int major;
        private final int minor;
        private final int patch;

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
        }
    }
}
