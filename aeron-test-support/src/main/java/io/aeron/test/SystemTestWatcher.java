/*
 * Copyright 2014-2022 Real Logic Limited.
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

import io.aeron.CommonContext;
import io.aeron.archive.ArchiveMarkFile;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.samples.SamplesUtil;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.driver.DriverOutputConsumer;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;

import java.io.File;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SystemTestWatcher implements DriverOutputConsumer, AfterTestExecutionCallback, AfterEachCallback
{
    private static final String CLUSTER_TERMINATION_EXCEPTION = ClusterTerminationException.class.getName();
    private static final String UNKNOWN_HOST_EXCEPTION = UnknownHostException.class.getName();
    public static final Predicate<String> UNKNOWN_HOST_FILTER =
        (s) -> s.contains(UNKNOWN_HOST_EXCEPTION) || s.contains("unknown host");
    public static final Predicate<String> WARNING_FILTER = (s) -> s.contains("WARN");
    public static final Predicate<String> CLUSTER_TERMINATION_FILTER =
        (s) -> s.contains(CLUSTER_TERMINATION_EXCEPTION);
    public static final Predicate<String> TEST_CLUSTER_DEFAULT_LOG_FILTER =
        WARNING_FILTER.negate().and(CLUSTER_TERMINATION_FILTER.negate());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    private final MediaDriverTestUtil mediaDriverTestUtil = new MediaDriverTestUtil();

    private Predicate<String> logFilter = TEST_CLUSTER_DEFAULT_LOG_FILTER;
    private DataCollector dataCollector = new DataCollector();
    private AutoCloseable closeable = () -> {};

    public SystemTestWatcher cluster(final TestCluster testCluster)
    {
        this.dataCollector = testCluster.dataCollector();
        closeable = testCluster;

        return this;
    }

    public DataCollector dataCollector()
    {
        return dataCollector;
    }

    public void outputFiles(final String aeronDirectoryName, final File stdoutFile, final File stderrFile)
    {
        mediaDriverTestUtil.outputFiles(aeronDirectoryName, stdoutFile, stderrFile);
    }

    public void exitCode(final String aeronDirectoryName, final int exitValue)
    {
        mediaDriverTestUtil.exitCode(aeronDirectoryName, exitValue);
    }

    public void environmentVariables(final String aeronDirectoryName, final Map<String, String> environment)
    {
        mediaDriverTestUtil.environmentVariables(aeronDirectoryName, environment);
    }

    @SuppressWarnings("UnusedReturnValue")
    public SystemTestWatcher ignoreErrorsMatching(final Predicate<String> logFilter)
    {
        this.logFilter = this.logFilter.and(logFilter.negate());
        return this;
    }

    /**
     * Useful when debugging tests to get them to fail on warnings as well as errors.
     */
    @SuppressWarnings("unused")
    public void showAllErrors()
    {
        this.logFilter = (s) -> true;
    }

    public void afterTestExecution(final ExtensionContext context)
    {
        mediaDriverTestUtil.afterTestExecution(context);
        assertEquals(0, errorCount(), "Errors observed in " + context.getDisplayName());
    }

    public void afterEach(final ExtensionContext context)
    {
        final boolean interrupted = Thread.interrupted();
        try
        {
            final Optional<Throwable> executionException = context.getExecutionException();
            if (executionException.filter(t -> !(t instanceof TestAbortedException)).isPresent())
            {
                final String test = context.getTestClass().map(Class::getName).orElse("unknown") + "-" +
                    context.getTestMethod().map(Method::getName).orElse("unknown");
                System.out.println("*** " + test + " failed, cause: " + executionException.get());
                reportAndTerminate(test);
                mediaDriverTestUtil.testFailed();
            }
            else
            {
                CloseHelper.close(closeable);
                mediaDriverTestUtil.testSuccessful();
            }
        }
        finally
        {
            deleteAllLocations();
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int errorCount()
    {
        if (null != dataCollector)
        {
            return countErrors(
                dataCollector.cncFiles(),
                dataCollector.archiveMarkFiles(),
                dataCollector.consensusModuleMarkFiles(),
                dataCollector.clusterServiceMarkFiles(),
                logFilter);
        }

        return 0;
    }

    private int countErrors(
        final List<Path> cncPaths,
        final List<Path> archiveMarkFiles,
        final List<Path> consensusModuleMarkFiles,
        final List<Path> clusterServiceMarkFiles,
        final Predicate<String> filter)
    {
        final boolean isInterrupted = Thread.interrupted();
        try
        {
            return countErrors(cncPaths, filter, CommonContext::errorLogBuffer) +
                countArchiveMarkFileErrors(archiveMarkFiles, filter) +
                countClusterMarkFileErrors(consensusModuleMarkFiles, filter) +
                countClusterMarkFileErrors(clusterServiceMarkFiles, filter);
        }
        finally
        {
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private int countErrors(
        final List<Path> paths,
        final Predicate<String> filter,
        final Function<MappedByteBuffer, AtomicBuffer> toErrorBuffer)
    {
        final MutableInteger errorCount = new MutableInteger();

        for (final Path path : paths)
        {
            final File file = path.toFile();
            final MappedByteBuffer mmap = SamplesUtil.mapExistingFileReadOnly(file);
            try
            {
                final AtomicBuffer buffer = toErrorBuffer.apply(mmap);
                ErrorLogReader.read(
                    buffer,
                    (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                    {
                        if (filter.test(encodedException))
                        {
                            errorCount.set(errorCount.get() + observationCount);
                        }
                    });
            }
            finally
            {
                IoUtil.unmap(mmap);
            }
        }

        return errorCount.get();
    }

    private int countClusterMarkFileErrors(
        final List<Path> paths,
        final Predicate<String> filter)
    {
        final MutableInteger errorCount = new MutableInteger();

        for (final Path path : paths)
        {
            try (ClusterMarkFile clusterMarkFile = openClusterMarkFile(path))
            {
                final AtomicBuffer buffer = clusterMarkFile.errorBuffer();
                ErrorLogReader.read(
                    buffer,
                    (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                    {
                        if (filter.test(encodedException))
                        {
                            errorCount.set(errorCount.get() + observationCount);
                        }
                    });
            }
        }

        return errorCount.get();
    }

    private int countArchiveMarkFileErrors(
        final List<Path> paths,
        final Predicate<String> filter)
    {
        final MutableInteger errorCount = new MutableInteger();

        for (final Path path : paths)
        {
            try (ArchiveMarkFile archive = openArchiveMarkFile(path))
            {
                final AtomicBuffer buffer = archive.errorBuffer();
                ErrorLogReader.read(
                    buffer,
                    (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
                    {
                        if (filter.test(encodedException))
                        {
                            errorCount.set(errorCount.get() + observationCount);
                        }
                    });
            }
        }

        return errorCount.get();
    }

    private static ClusterMarkFile openClusterMarkFile(final Path path)
    {
        return new ClusterMarkFile(
            path.getParent().toFile(), path.getFileName().toString(), SystemEpochClock.INSTANCE, 0, (s) -> {});
    }

    private static ArchiveMarkFile openArchiveMarkFile(final Path path)
    {
        return new ArchiveMarkFile(
            path.getParent().toFile(), path.getFileName().toString(), SystemEpochClock.INSTANCE, 0, (s) -> {});
    }

    private void printObservationCallback(
        final int observationCount,
        final long firstObservationTimestamp,
        final long lastObservationTimestamp,
        final String encodedException)
    {
        final String ignored = !logFilter.test(encodedException) ? "(ignored) " : "";
        System.out.format(
            "***%n%s%d observations from %s to %s for:%n %s%n",
            ignored,
            observationCount,
            DATE_FORMAT.format(new Date(firstObservationTimestamp)),
            DATE_FORMAT.format(new Date(lastObservationTimestamp)),
            encodedException);
    }

    private void reportAndTerminate(final String test)
    {
        Throwable error = null;

        if (null != dataCollector)
        {
            try
            {
                printCncErrors(dataCollector.cncFiles(), CommonContext::errorLogBuffer);
                printArchiveMarkFileErrors(dataCollector.archiveMarkFiles());
                printClusterMarkFileErrors(dataCollector.consensusModuleMarkFiles(), "Consensus Module Errors");
                printClusterMarkFileErrors(dataCollector.clusterServiceMarkFiles(), "Cluster Service Errors");
            }
            catch (final Exception t)
            {
                error = t;
            }

            try
            {
                dataCollector.dumpData(test);
            }
            catch (final Exception t)
            {
                if (null != error)
                {
                    error.addSuppressed(t);
                }
                else
                {
                    error = t;
                }
            }
        }

        try
        {
            CloseHelper.close(closeable);
        }
        catch (final Exception t)
        {
            if (null != error)
            {
                error.addSuppressed(t);
            }
            else
            {
                error = t;
            }
        }

        if (null != error)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    private void printCncErrors(
        final List<Path> paths,
        final Function<MappedByteBuffer, AtomicBuffer> toErrorBuffer)
    {
        for (final Path path : paths)
        {
            final File cncFile = path.toFile();
            final MappedByteBuffer mmap = SamplesUtil.mapExistingFileReadOnly(cncFile);
            try
            {
                final AtomicBuffer buffer = toErrorBuffer.apply(mmap);

                System.out.printf("%n%n%s file %s%n", "Command `n Control Errors", cncFile);
                final int distinctErrorCount = ErrorLogReader.read(buffer, this::printObservationCallback);
                System.out.format("%d distinct errors observed.%n", distinctErrorCount);
            }
            finally
            {
                IoUtil.unmap(mmap);
            }
        }
    }

    private void printArchiveMarkFileErrors(final List<Path> paths)
    {
        for (final Path path : paths)
        {
            try (ArchiveMarkFile archiveFile = openArchiveMarkFile(path))
            {
                final AtomicBuffer buffer = archiveFile.errorBuffer();

                System.out.printf("%n%n%s file %s%n", "Archive Errors", path);
                final int distinctErrorCount = ErrorLogReader.read(buffer, this::printObservationCallback);
                System.out.format("%d distinct errors observed.%n", distinctErrorCount);
            }
        }
    }

    private void printClusterMarkFileErrors(
        final List<Path> paths,
        final String fileDescription)
    {
        for (final Path path : paths)
        {
            try (ClusterMarkFile clusterMarkFile = openClusterMarkFile(path))
            {
                final AtomicBuffer buffer = clusterMarkFile.errorBuffer();

                System.out.printf("%n%n%s file %s%n", fileDescription, path);
                final int distinctErrorCount = ErrorLogReader.read(buffer, this::printObservationCallback);
                System.out.format("%d distinct errors observed.%n", distinctErrorCount);
            }
        }
    }

    private void deleteAllLocations()
    {
        for (final Path path : dataCollector.allLocations())
        {
            IoUtil.delete(path.toFile(), true);
        }
    }
}
