package io.aeron.build;

import java.io.File;
import java.util.List;

import static java.util.Arrays.asList;

public class AsciidocUtil
{
    private static final List<String> EXTENSIONS = asList(".adoc", ".asciidoc");

    public static File[] filterAsciidocFiles(File directory)
    {
        final File[] asciidocFiles = directory.listFiles((dir, name) ->
        {
            for (final String extension : EXTENSIONS)
            {
                if (name.endsWith(extension))
                {
                    return true;
                }
            }
            return false;
        });

        return null != asciidocFiles ? asciidocFiles : new File[0];
    }
}
