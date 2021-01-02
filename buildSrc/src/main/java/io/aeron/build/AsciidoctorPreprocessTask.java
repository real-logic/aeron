/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.build;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.PreprocessorReader;
import org.asciidoctor.log.Severity;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class AsciidoctorPreprocessTask extends DefaultTask
{
    private final String sampleBaseDir = getProject().getProjectDir().getAbsolutePath();

    private final String sampleSourceDir = sampleBaseDir + "/src/main/java";

    private final File source = new File(sampleBaseDir, "/src/docs/asciidoc");

    private final File target = new File(getProject().getBuildDir(), "/asciidoc/asciidoc");

    // Has a slightly silly name to avoid name clashes in the build script.
    private String versionText;

    @Input
    public String getSampleBaseDir()
    {
        return sampleBaseDir;
    }

    @Input
    public String getSampleSourceDir()
    {
        return sampleSourceDir;
    }

    @InputDirectory
    public File getSource()
    {
        return source;
    }

    @OutputDirectory
    public File getTarget()
    {
        return target;
    }

    @Input
    public String getVersionText()
    {
        return versionText;
    }

    public void setVersionText(final String versionText)
    {
        this.versionText = versionText;
    }

    @TaskAction
    public void preprocess() throws Exception
    {
        if (!target.exists() && !target.mkdirs())
        {
            throw new IOException("unable to create build directory");
        }

        final File[] asciidocFiles = AsciidocUtil.filterAsciidocFiles(source);

        System.out.println("Transforming from: " + source);
        System.out.println("Found files: " + Arrays.stream(asciidocFiles).map(File::getName).collect(joining(", ")));

        final Map<File, Integer> errors = new HashMap<>();

        for (final File asciidocFile : asciidocFiles)
        {
            final File outputFile = new File(target, asciidocFile.getName());

            final Asciidoctor asciidoctor = Asciidoctor.Factory.create();

            final int[] errorCount = { 0 };

            asciidoctor.registerLogHandler(
                (logRecord) ->
                {
                    if (logRecord.getSeverity() == Severity.ERROR || logRecord.getSeverity() == Severity.FATAL)
                    {
                        errorCount[0]++;
                    }
                });

            final HashMap<String, Object> attributes = new HashMap<>();
            attributes.put("sampleBaseDir", requireNonNull(sampleBaseDir, "Must specify sampleBaseDir"));
            attributes.put("sampleSourceDir", requireNonNull(sampleSourceDir, "Must specify sampleSourceDir"));

            final HashMap<String, Object> options = new HashMap<>();
            options.put("attributes", attributes);
            options.put("safe", org.asciidoctor.SafeMode.UNSAFE.getLevel());

            try (PrintStream output = new PrintStream(outputFile))
            {
                asciidoctor.javaExtensionRegistry().preprocessor(
                    new org.asciidoctor.extension.Preprocessor()
                    {
                        public void process(final Document document, final PreprocessorReader reader)
                        {
                            String line;
                            while (null != (line = reader.readLine()))
                            {
                                if (line.startsWith(":aeronVersion:"))
                                {
                                    output.println(":aeronVersion: " + versionText);
                                }
                                else
                                {
                                    output.println(line);
                                }
                            }
                        }
                    });

                asciidoctor.loadFile(asciidocFile, options);

                if (0 < errorCount[0])
                {
                    errors.put(asciidocFile, errorCount[0]);
                }
            }
        }

        errors.forEach((key, value) -> System.out.println("file: " + key + ", error count: " + value));

        if (0 < errors.size())
        {
            throw new Exception("failed due to errors in parsing");
        }
    }
}
