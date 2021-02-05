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
package io.aeron.test;

import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.agrona.Strings.isEmpty;

/**
 * {@code DataCollector} is a helper class to preserve data upon test failure.
 */
public final class DataCollector
{
    static final String THREAD_DUMP_FILE_NAME = "thread_dump.txt";
    static final AtomicInteger UNIQUE_ID = new AtomicInteger(0);
    private static final String SEPARATOR = "-";
    private final Path rootDir;
    private final Set<Path> locations = new LinkedHashSet<>();

    public DataCollector()
    {
        this(Paths.get("build/test-output"));
    }

    public DataCollector(final Path rootDir)
    {
        requireNonNull(rootDir);
        if (Files.exists(rootDir) && !Files.isDirectory(rootDir))
        {
            throw new IllegalArgumentException(rootDir + " is not a directory");
        }
        this.rootDir = rootDir;
    }

    /**
     * Add a file/directory to be preserved.
     *
     * @param location file or directory to preserve.
     * @see #dumpData(TestInfo)
     * @see #dumpData(String)
     */
    public void add(final Path location)
    {
        locations.add(requireNonNull(location));
    }

    /**
     * Copy data from all of the added locations to the directory {@code $rootDir/$testClass_$testMethod}, where:
     * <ul>
     *     <li>{@code $rootDir} is the root directory specified when {@link #DataCollector} was created.</li>
     *     <li>{@code $testClass} is the fully qualified class name of the test class.</li>
     *     <li>{@code $testMethod} is the test method name.</li>
     * </ul>
     *
     * @param testInfo test info from JUnit.
     * @return {@code null} if no data was copied or an actual destination directory used.
     * @see #dumpData(String)
     */
    public Path dumpData(final TestInfo testInfo)
    {
        final String testClass = testInfo.getTestClass().orElseThrow(IllegalStateException::new).getName();
        final String testMethod = testInfo.getTestMethod().orElseThrow(IllegalStateException::new).getName();
        return copyData(testClass + SEPARATOR + testMethod);
    }

    /**
     * Copy data from all of the added locations to the directory {@code $rootDir/$destinationDir}, where:
     * <ul>
     *     <li>{@code $rootDir} is the root directory specified when {@link #DataCollector} was created.</li>
     *     <li>{@code $destinationDir} is the destination directory name.</li>
     * </ul>
     * <p>
     *     <em>
     *     Note: If the destination directory already exists then a unique ID suffix will be added to the name.
     *     For example given that root directory is {@code build/test-output} and the destination directory is
     *     {@code my-dir} the actual directory could be {@code build/test-output/my-dir_5}, where {@code _5} is the
     *     suffix added.
     *     </em>
     * </p>
     *
     * @param destinationDir destination directory where the data should be copied into.
     * @return {@code null} if no data was copied or an actual destination directory used.
     */
    public Path dumpData(final String destinationDir)
    {
        if (isEmpty(destinationDir))
        {
            throw new IllegalArgumentException("destination dir is required");
        }

        return copyData(destinationDir);
    }

    public String toString()
    {
        return "DataCollector{" +
            "rootDir=" + rootDir +
            ", locations=" + locations +
            '}';
    }

    private Path copyData(final String destinationDir)
    {
        final List<Path> locations = this.locations.stream().filter(Files::exists).collect(toList());
        if (locations.isEmpty())
        {
            return null;
        }

        try
        {
            final Path destination = createUniqueDirectory(destinationDir);
            final Map<Path, Set<Path>> groups = groupByParent(locations);
            for (final Map.Entry<Path, Set<Path>> group : groups.entrySet())
            {
                final Set<Path> files = group.getValue();
                final Path parent = adjustParentToEnsureUniqueContext(destination, files, group.getKey());
                for (final Path srcFile : files)
                {
                    final Path dstFile = destination.resolve(parent.relativize(srcFile));
                    copyFiles(srcFile, dstFile);
                }
            }

            final Path threadDump = destination.resolve(THREAD_DUMP_FILE_NAME);
            Files.write(threadDump, SystemUtil.threadDump().getBytes(UTF_8));
            return destination;
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        return null;
    }

    private Path createUniqueDirectory(final String name) throws IOException
    {
        Path path = rootDir.resolve(name);
        while (Files.exists(path))
        {
            path = rootDir.resolve(name + SEPARATOR + UNIQUE_ID.incrementAndGet());
        }

        return Files.createDirectories(path);
    }

    private Map<Path, Set<Path>> groupByParent(final List<Path> locations)
    {
        final LinkedHashMap<Path, Set<Path>> map = new LinkedHashMap<>();
        for (final Path p : locations)
        {
            map.put(p, singleton(p));
        }

        removeNestedPaths(locations, map);

        return groupByParent(map);
    }

    private void removeNestedPaths(final List<Path> locations, final LinkedHashMap<Path, Set<Path>> map)
    {
        for (final Path p : locations)
        {
            Path parent = p.getParent();
            while (null != parent)
            {
                if (map.containsKey(parent))
                {
                    map.remove(p);
                    break;
                }
                parent = parent.getParent();
            }
        }
    }

    private LinkedHashMap<Path, Set<Path>> groupByParent(final LinkedHashMap<Path, Set<Path>> locations)
    {
        if (1 == locations.size())
        {
            return locations;
        }

        final LinkedHashMap<Path, Set<Path>> result = new LinkedHashMap<>();
        final Set<Path> processed = new HashSet<>();
        boolean recurse = false;
        for (final Map.Entry<Path, Set<Path>> e1 : locations.entrySet())
        {
            final Path path1 = e1.getKey();
            if (processed.add(path1))
            {
                boolean found = false;
                final Path parent = path1.getParent();
                if (null != parent && !parent.equals(path1.getRoot()))
                {
                    for (final Map.Entry<Path, Set<Path>> e2 : locations.entrySet())
                    {
                        final Path path2 = e2.getKey();
                        if (!processed.contains(path2) && path2.startsWith(parent))
                        {
                            found = true;
                            processed.add(path2);
                            final Set<Path> children = result.computeIfAbsent(parent, (key) -> new HashSet<>());
                            children.addAll(e1.getValue());
                            children.addAll(e2.getValue());
                        }
                    }
                }

                if (!found)
                {
                    result.put(path1, e1.getValue());
                }
                recurse = recurse || found;
            }
        }

        if (recurse)
        {
            return groupByParent(result);
        }

        return locations;
    }

    private Path adjustParentToEnsureUniqueContext(final Path destination, final Set<Path> files, final Path root)
    {
        Path parent = root;
        for (final Path srcFile : files)
        {
            while (true)
            {
                final Path dst = destination.resolve(parent.relativize(srcFile));
                if (!Files.exists(dst))
                {
                    break;
                }
                parent = parent.getParent();
            }
        }

        return parent;
    }

    private void copyFiles(final Path src, final Path dst) throws IOException
    {
        if (Files.isRegularFile(src))
        {
            ensurePathExists(dst);

            Files.copy(src, dst, COPY_ATTRIBUTES, REPLACE_EXISTING);
        }
        else
        {
            Files.walkFileTree(
                src,
                new SimpleFileVisitor<Path>()
                {
                    public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                        throws IOException
                    {
                        final Path dstDir = dst.resolve(src.relativize(dir));
                        ensurePathExists(dstDir);
                        Files.copy(dir, dstDir, COPY_ATTRIBUTES);

                        return FileVisitResult.CONTINUE;
                    }

                    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                        throws IOException
                    {
                        Files.copy(file, dst.resolve(src.relativize(file)), COPY_ATTRIBUTES);

                        return FileVisitResult.CONTINUE;
                    }
                });
        }
    }

    private void ensurePathExists(final Path dst) throws IOException
    {
        if (!Files.exists(dst.getParent()))
        {
            Files.createDirectories(dst.getParent());
        }
    }
}
