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
package io.aeron.test.launcher;

import java.io.File;
import java.util.Arrays;

public class FileResolveUtil
{
    public static File resolveProjectRoot()
    {
        final File workingDir = new File(System.getProperty("user.dir"));
        File parent = workingDir;

        do
        {
            final String[] versionTxtFile = parent.list((dir, name) -> "version.txt".equals(name));
            if (null != versionTxtFile && 1 == versionTxtFile.length)
            {
                return parent;
            }

            parent = workingDir.getParentFile();
        }
        while (null != parent);

        throw new RuntimeException("Unable to find project root directory from: " + workingDir);
    }

    public static File resolveAeronAllJar()
    {
        final File projectRoot = resolveProjectRoot();
        final File allBuildLibs = new File(projectRoot, "aeron-all/build/libs");
        if (!allBuildLibs.exists())
        {
            throw new RuntimeException("Directory: " + allBuildLibs + " does not exist");
        }

        final File[] aeronAllJarFiles = allBuildLibs.listFiles(
            (dir, name) -> name.startsWith("aeron-all-") && name.endsWith(".jar"));

        if (null == aeronAllJarFiles || 0 == aeronAllJarFiles.length)
        {
            throw new RuntimeException("Unable to find aeron jar files in directory: " + allBuildLibs);
        }

        if (1 != aeronAllJarFiles.length)
        {
            throw new RuntimeException(
                "Multiple libs found, run './gradlew clean': " + Arrays.toString(aeronAllJarFiles));
        }

        return aeronAllJarFiles[0];
    }

    public static File resolveJavaBinary()
    {
        final File javaHome = new File(System.getProperty("java.home"));
        if (!javaHome.exists())
        {
            throw new RuntimeException("java.home: " + javaHome + " does not exist??");
        }

        final File javaBinary = new File(javaHome, "bin/java");
        if (!javaBinary.exists())
        {
            throw new RuntimeException("java binary: " + javaBinary + " does not exist??");
        }

        return javaBinary;
    }

    public static File resolveClusterScriptDir()
    {
        return new File(resolveProjectRoot(), "aeron-samples/scripts/cluster");
    }
}
