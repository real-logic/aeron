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

import io.aeron.CommonContext;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.samples.SamplesUtil;
import io.aeron.test.cluster.TestCluster;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.net.UnknownHostException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

public class ClusterTestWatcher implements TestWatcher
{
    public static final Predicate<String> UNKNOWN_HOST_FILTER = s -> s.contains(UnknownHostException.class.getName());
    public static final Predicate<String> WARNING_FILTER = s -> s.contains("WARN");
    public static final Predicate<String> CLUSTER_TERMINATION_FILTER =
        s -> s.contains(ClusterTerminationException.class.getName());
    public static final Predicate<String> TEST_CLUSTER_DEFAULT_LOG_FILTER =
        WARNING_FILTER.negate().and(CLUSTER_TERMINATION_FILTER.negate());

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");

    private TestCluster testCluster = null;
    private Predicate<String> logFilter = TEST_CLUSTER_DEFAULT_LOG_FILTER;

    public ClusterTestWatcher cluster(final TestCluster testCluster)
    {
        this.testCluster = testCluster;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public ClusterTestWatcher ignoreErrorsMatching(final Predicate<String> logFilter)
    {
        this.logFilter = this.logFilter.and(logFilter.negate());
        return this;
    }

    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        if (null != testCluster)
        {
            printErrors(testCluster.dataCollector().cncFiles(), testCluster.dataCollector().errorLogFiles());

            final String testClass = context.getTestClass().orElseThrow(IllegalStateException::new).getName();
            final String testMethod = context.getTestMethod().orElseThrow(IllegalStateException::new).getName();

            testCluster.dataCollector().dumpData(testClass, testMethod);
        }

        CloseHelper.close(testCluster);
    }

    public int errorCount()
    {
        if (null != testCluster)
        {
            return countErrors(
                testCluster.dataCollector().cncFiles(),
                testCluster.dataCollector().errorLogFiles(),
                logFilter);
        }
        return 0;
    }

    private void printErrors(final List<Path> cncPaths, final List<Path> clusterErrorPaths)
    {
        printErrors(cncPaths, "Command `n Control", CommonContext::errorLogBuffer);
        printErrors(clusterErrorPaths, "Cluster Errors", UnsafeBuffer::new);
    }

    private void printErrors(
        final List<Path> paths,
        final String fileDescription,
        final Function<MappedByteBuffer, AtomicBuffer> toErrorBuffer)
    {
        for (final Path path : paths)
        {
            final File cncFile = path.toFile();
            final MappedByteBuffer mmap = SamplesUtil.mapExistingFileReadOnly(cncFile);
            try
            {
                final AtomicBuffer buffer = toErrorBuffer.apply(mmap);

                System.out.printf("%n%n%s file %s%n", fileDescription, cncFile);
                final int distinctErrorCount = ErrorLogReader.read(
                    buffer, this::printObservationCallback);
                System.out.format("%d distinct errors observed.%n", distinctErrorCount);
            }
            finally
            {
                IoUtil.unmap(mmap);
            }
        }
    }

    private int countErrors(
        final List<Path> paths,
        final Predicate<String> filter,
        final Function<MappedByteBuffer, AtomicBuffer> toErrorBuffer)
    {
        final MutableInteger errorCount = new MutableInteger(0);

        for (final Path path : paths)
        {
            final File cncFile = path.toFile();
            final MappedByteBuffer cncMmap = SamplesUtil.mapExistingFileReadOnly(cncFile);
            try
            {
                final AtomicBuffer buffer = toErrorBuffer.apply(cncMmap);
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
                IoUtil.unmap(cncMmap);
            }
        }

        return errorCount.get();
    }

    private int countErrors(
        final List<Path> cncPaths,
        final List<Path> clusterErrorPaths,
        final Predicate<String> filter)
    {
        return countErrors(cncPaths, filter, CommonContext::errorLogBuffer) +
            countErrors(clusterErrorPaths, filter, UnsafeBuffer::new);
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

    public void testDisabled(final ExtensionContext context, final Optional<String> reason)
    {
        CloseHelper.close(testCluster);
    }

    public void testSuccessful(final ExtensionContext context)
    {
        CloseHelper.close(testCluster);
    }

    public void testAborted(final ExtensionContext context, final Throwable cause)
    {
        CloseHelper.close(testCluster);
    }
}
