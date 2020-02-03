package io.aeron.build;

import org.asciidoctor.Asciidoctor;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.PreprocessorReader;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;

import static java.util.Objects.requireNonNull;

public class AsciidoctorPreprocessTask extends DefaultTask
{
    @Input
    public String sampleBaseDir;

    @Input
    public String sampleSourceDir;

    @InputFile
    public File source;

    @OutputFile
    public File target;

    @Input
    public String version;

    @TaskAction
    public void preprocess() throws Exception
    {
        final Asciidoctor asciidoctor = Asciidoctor.Factory.create();

        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put("sampleBaseDir", requireNonNull(sampleBaseDir, "Must specify sampleBaseDir"));
        attributes.put("sampleSourceDir", requireNonNull(sampleSourceDir, "Must specify sampleSourceDir"));

        final HashMap<String, Object> options = new HashMap<>();
        options.put("attributes", attributes);
        options.put("safe", org.asciidoctor.SafeMode.UNSAFE.getLevel());

        if (!target.getParentFile().exists() && !target.getParentFile().mkdirs())
        {
            throw new IOException("Unable to create build directory");
        }

        try (final PrintStream output = new PrintStream(target))
        {
            asciidoctor.javaExtensionRegistry().preprocessor(new org.asciidoctor.extension.Preprocessor()
            {
                public void process(Document document, PreprocessorReader reader)
                {
                    String line;
                    while (null != (line = reader.readLine()))
                    {
                        if (line.startsWith(":aeronVersion:"))
                        {
                            output.println(":aeronVersion: " + version);
                        }
                        else
                        {
                            output.println(line);
                        }
                    }
                }
            });

            asciidoctor.loadFile(source, options);
        }
    }
}
