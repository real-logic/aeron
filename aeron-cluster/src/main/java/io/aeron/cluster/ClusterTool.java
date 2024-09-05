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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterNodeControlProperties;
import io.aeron.cluster.service.ConsensusModuleProxy;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardCopyOption.*;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.Strings.isEmpty;
import static org.agrona.SystemUtil.getDurationInNanos;

/**
 * Tool for control and investigating the state of a cluster node.
 * <pre>
 * Usage: ClusterTool &#60;cluster-dir&#62; &#60;command&#62; [options]
 *                         describe: prints out all descriptors in the mark file.
 *                              pid: prints PID of cluster component.
 *                    recovery-plan: [service count] prints recovery plan of cluster component.
 *                    recording-log: prints recording log of cluster component.
 *               sort-recording-log: reorders entries in the recording log to match the order in memory.
 * seed-recording-log-from-snapshot: creates a new recording log based on the latest valid snapshot.
 *                           errors: prints Aeron and cluster component error logs.
 *                     list-members: prints leader memberId, active members and passive members lists.
 *                     backup-query: [delay] get, or set, time of next backup query.
 *       invalidate-latest-snapshot: marks the latest snapshot as a invalid so the previous is loaded.
 *                         snapshot: triggers a snapshot on the leader.
 *                 standby-snapshot: triggers a snapshot on cluster standby nodes.
 *                          suspend: suspends appending to the log.
 *                           resume: resumes reading from the log.
 *                         shutdown: initiates an orderly stop of the cluster with a snapshot.
 *                            abort: stops the cluster without a snapshot.
 *      describe-latest-cm-snapshot: prints the contents of the latest valid consensus module snapshot.
 *                        is-leader: returns zero if the cluster node is leader, non-zero if not
 * </pre>
 */
@Config(existsInC = false)
public class ClusterTool
{
    /**
     * Timeout in nanoseconds for the tool to wait while trying to perform an operation.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 0, hasContext = false)
    public static final String AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME = "aeron.cluster.tool.timeout";

    /**
     * Delay in nanoseconds to be applied to an operation such as when the new cluster backup query will occur.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 0, hasContext = false)
    public static final String AERON_CLUSTER_TOOL_DELAY_PROP_NAME = "aeron.cluster.tool.delay";

    /**
     * Property name for setting the channel used for archive replays.
     */
    @Config(hasContext = false)
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.tool.replay.channel";

    /**
     * Default channel used for archive replays.
     */
    @Config
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT = "aeron:ipc";

    /**
     * Channel used for archive replays.
     */
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL = SystemUtil.getProperty(
        AERON_CLUSTER_TOOL_REPLAY_CHANNEL_PROP_NAME, AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT);

    /**
     * Property name for setting the stream id used for archive replays.
     */
    @Config(hasContext = false)
    public static final String AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.tool.replay.stream.id";

    /**
     * Default stream id used for archive replays.
     */
    @Config
    public static final int AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT = 103;

    /**
     * Stream id used for archive replays.
     */
    public static final int AERON_CLUSTER_TOOL_REPLAY_STREAM_ID = Integer.getInteger(
        AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_PROP_NAME, AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT);

    static final long TIMEOUT_MS =
        NANOSECONDS.toMillis(getDurationInNanos(AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME, 0));

    private static final int MARK_FILE_VERSION_WITH_CLUSTER_SERVICES_DIR = 1;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("methodlength")
    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            printHelp();
            System.exit(-1);
        }

        final File clusterDir = new File(args[0]);
        if (!clusterDir.exists())
        {
            System.err.println("ERR: cluster directory not found: " + clusterDir.getAbsolutePath());
            printHelp();
            System.exit(-1);
        }

        switch (args[1])
        {
            case "describe":
                describe(System.out, clusterDir);
                break;

            case "pid":
                pid(System.out, clusterDir);
                break;

            case "recovery-plan":
                if (args.length < 3)
                {
                    printHelp();
                    System.exit(-1);
                }
                recoveryPlan(System.out, clusterDir, Integer.parseInt(args[2]));
                break;

            case "recording-log":
                recordingLog(System.out, clusterDir);
                break;

            case "sort-recording-log":
                sortRecordingLog(clusterDir);
                break;

            case "seed-recording-log-from-snapshot":
                seedRecordingLogFromSnapshot(clusterDir);
                break;

            case "errors":
                errors(System.out, clusterDir);
                break;

            case "list-members":
                listMembers(System.out, clusterDir);
                break;

            case "backup-query":
                if (args.length < 3)
                {
                    printNextBackupQuery(System.out, clusterDir);
                }
                else
                {
                    nextBackupQuery(
                        System.out,
                        clusterDir,
                        NANOSECONDS.toMillis(SystemUtil.parseDuration(AERON_CLUSTER_TOOL_DELAY_PROP_NAME, args[2])));
                }
                break;

            case "invalidate-latest-snapshot":
                invalidateLatestSnapshot(System.out, clusterDir);
                break;

            case "is-leader":
                System.exit(isLeader(System.out, clusterDir));
                break;

            case "snapshot":
                exitWithErrorOnFailure(snapshot(clusterDir, System.out));
                break;

            case "suspend":
                exitWithErrorOnFailure(suspend(clusterDir, System.out));
                break;

            case "resume":
                exitWithErrorOnFailure(resume(clusterDir, System.out));
                break;

            case "shutdown":
                exitWithErrorOnFailure(shutdown(clusterDir, System.out));
                break;

            case "abort":
                exitWithErrorOnFailure(abort(clusterDir, System.out));
                break;

            case "describe-latest-cm-snapshot":
                describeLatestConsensusModuleSnapshot(System.out, clusterDir);
                break;

            default:
                System.out.println("Unknown command: " + args[1]);
                printHelp();
                System.exit(-1);
        }
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void describe(final PrintStream out, final File clusterDir)
    {
        describeClusterMarkFile(clusterDir, out);
    }

    // Used by Standby Tooling.
    @SuppressWarnings("UnusedReturnValue")
    static boolean describeClusterMarkFile(final File clusterDir, final PrintStream out)
    {
        final File clusterServicesDir;
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final MarkFileHeaderDecoder decoder = markFile.decoder();

                printTypeAndActivityTimestamp(out, markFile);
                out.println(decoder);

                clusterServicesDir = resolveClusterServicesDir(clusterDir, decoder);
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return false;
        }

        final ClusterMarkFile[] serviceMarkFiles = openServiceMarkFiles(clusterServicesDir, out::println);
        describe(out, serviceMarkFiles);

        return true;
    }

    static File resolveClusterServicesDir(final File clusterDir, final MarkFileHeaderDecoder decoder)
    {
        final File clusterServicesDir;

        if (MARK_FILE_VERSION_WITH_CLUSTER_SERVICES_DIR <= decoder.sbeSchemaVersion())
        {
            decoder.sbeRewind();
            decoder.skipAeronDirectory();
            decoder.skipControlChannel();
            decoder.skipIngressChannel();
            decoder.skipServiceName();
            decoder.skipAuthenticator();

            final String servicesClusterDir = decoder.servicesClusterDir();
            clusterServicesDir = isEmpty(servicesClusterDir) ? clusterDir : new File(servicesClusterDir);
        }
        else
        {
            clusterServicesDir = clusterDir;
        }

        return clusterServicesDir;
    }

    /**
     * Print out the PID of the cluster process.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void pid(final PrintStream out, final File clusterDir)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                out.println(markFile.decoder().pid());
            }
        }
        else
        {
            System.exit(-1);
        }
    }

    /**
     * Print out the {@link io.aeron.cluster.RecordingLog.RecoveryPlan} for the cluster.
     *
     * @param out          to print the output to.
     * @param clusterDir   where the cluster is running.
     * @param serviceCount of services running in the containers.
     */
    public static void recoveryPlan(final PrintStream out, final File clusterDir, final int serviceCount)
    {
        try (AeronArchive archive = AeronArchive.connect();
            RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            out.println(recordingLog.createRecoveryPlan(archive, serviceCount, Aeron.NULL_VALUE));
        }
    }

    /**
     * Print out the {@link RecordingLog} for the cluster.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void recordingLog(final PrintStream out, final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            out.println(recordingLog);
        }
    }

    /**
     * Re-order entries in thee {@link RecordingLog} file on disc if they are not in a proper order.
     *
     * @param clusterDir where the cluster is running.
     * @return {@code true} if file contents was changed or {@code false} if it was already in the correct order.
     */
    public static boolean sortRecordingLog(final File clusterDir)
    {
        final List<RecordingLog.Entry> entries;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            entries = recordingLog.entries();
            if (isRecordingLogSorted(entries))
            {
                return false;
            }
        }
        catch (final RuntimeException ex)
        {
            return false;
        }

        updateRecordingLog(clusterDir, entries);

        return true;
    }

    /**
     * Create a new {@link RecordingLog} based on the latest valid snapshot whose term base and log positions are set
     * to zero. The original recording log file is backed up as {@code recording.log.bak}.
     *
     * @param clusterDir where the cluster is running.
     */
    public static void seedRecordingLogFromSnapshot(final File clusterDir)
    {
        int snapshotIndex = Aeron.NULL_VALUE;
        final List<RecordingLog.Entry> entries;
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            entries = recordingLog.entries();
            for (int i = entries.size() - 1; i >= 0; i--)
            {
                final RecordingLog.Entry entry = entries.get(i);
                if (RecordingLog.isValidSnapshot(entry) && ConsensusModule.Configuration.SERVICE_ID == entry.serviceId)
                {
                    snapshotIndex = i;
                    break;
                }
            }
        }

        final Path recordingLogBackup = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".bak");
        try
        {
            Files.copy(
                clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME),
                recordingLogBackup,
                REPLACE_EXISTING,
                COPY_ATTRIBUTES);
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }

        if (Aeron.NULL_VALUE == snapshotIndex)
        {
            updateRecordingLog(clusterDir, Collections.emptyList());
        }
        else
        {
            final List<RecordingLog.Entry> truncatedEntries = new ArrayList<>();
            int serviceId = ConsensusModule.Configuration.SERVICE_ID;
            for (int i = snapshotIndex; i >= 0; i--)
            {
                final RecordingLog.Entry entry = entries.get(i);
                if (RecordingLog.isValidSnapshot(entry) && entry.serviceId == serviceId)
                {
                    truncatedEntries.add(new RecordingLog.Entry(
                        entry.recordingId,
                        entry.leadershipTermId,
                        0,
                        0,
                        entry.timestamp,
                        entry.serviceId,
                        entry.type,
                        null,
                        entry.isValid,
                        NULL_VALUE));
                    serviceId++;
                }
                else
                {
                    break;
                }
            }
            Collections.reverse(truncatedEntries);

            updateRecordingLog(clusterDir, truncatedEntries);
        }
    }

    /**
     * Print out the errors in the error logs for the cluster components.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void errors(final PrintStream out, final File clusterDir)
    {
        File clusterServicesDir = clusterDir;
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                printTypeAndActivityTimestamp(out, markFile);
                printErrors(out, markFile);

                final String aeronDirectory = markFile.decoder().aeronDirectory();
                out.println();
                printDriverErrors(out, aeronDirectory);

                clusterServicesDir = resolveClusterServicesDir(clusterDir, markFile.decoder());
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
        }

        final ClusterMarkFile[] serviceMarkFiles = openServiceMarkFiles(clusterServicesDir, out::println);
        for (final ClusterMarkFile serviceMarkFile : serviceMarkFiles)
        {
            printTypeAndActivityTimestamp(out, serviceMarkFile);
            printErrors(out, serviceMarkFile);
            serviceMarkFile.close();
        }
    }

    /**
     * Print out a list of the current members of the cluster.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void listMembers(final PrintStream out, final File clusterDir)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final ClusterMembership clusterMembership = new ClusterMembership();
                final long timeoutMs = Math.max(TimeUnit.SECONDS.toMillis(1), TIMEOUT_MS);

                if (queryClusterMembers(markFile, timeoutMs, clusterMembership))
                {
                    out.println(
                        "currentTimeNs=" + clusterMembership.currentTimeNs +
                        ", leaderMemberId=" + clusterMembership.leaderMemberId +
                        ", memberId=" + clusterMembership.memberId +
                        ", activeMembers=" + clusterMembership.activeMembers +
                        ", passiveMembers=" + clusterMembership.passiveMembers);
                }
                else
                {
                    out.println("timeout waiting for response from node");
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
        }
    }

    /**
     * Print out the time of the next backup query.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster backup is running.
     */
    public static void printNextBackupQuery(final PrintStream out, final File clusterDir)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                if (markFile.decoder().componentType() != ClusterComponentType.BACKUP)
                {
                    out.println("not a cluster backup node");
                }
                else
                {
                    out.format("%2$tF %1$tH:%1$tM:%1$tS next: %2$tF %2$tH:%2$tM:%2$tS%n",
                        new Date(),
                        new Date(nextBackupQueryDeadlineMs(markFile)));
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
        }
    }

    /**
     * Set the time of the next backup query for the cluster backup.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     * @param delayMs    from the current time for the next backup query.
     */
    public static void nextBackupQuery(final PrintStream out, final File clusterDir, final long delayMs)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                if (markFile.decoder().componentType() != ClusterComponentType.BACKUP)
                {
                    out.println("not a cluster backup node");
                }
                else
                {
                    final EpochClock epochClock = SystemEpochClock.INSTANCE;
                    nextBackupQueryDeadlineMs(markFile, epochClock.time() + delayMs);
                    out.format("%2$tF %1$tH:%1$tM:%1$tS setting next: %2$tF %2$tH:%2$tM:%2$tS%n",
                        new Date(),
                        new Date(nextBackupQueryDeadlineMs(markFile)));
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
        }
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param out              to print the output to.
     * @param serviceMarkFiles to query.
     */
    public static void describe(final PrintStream out, final ClusterMarkFile[] serviceMarkFiles)
    {
        for (final ClusterMarkFile serviceMarkFile : serviceMarkFiles)
        {
            printTypeAndActivityTimestamp(out, serviceMarkFile);
            out.println(serviceMarkFile.decoder());
            serviceMarkFile.close();
        }
    }

    /**
     * Determine if the given node is a leader
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     * @return 0 if the node is an active leader in a closed election, 1 if not,
     * -1 if the mark file does not exist.
     */
    public static int isLeader(final PrintStream out, final File clusterDir)
    {
        if (markFileExists(clusterDir))
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                final String aeronDirectoryName = markFile.decoder().aeronDirectory();
                final MutableLong nodeRoleCounter = new MutableLong(-1);
                final MutableLong electionStateCounter = new MutableLong(-1);

                try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
                {
                    final CountersReader countersReader = aeron.countersReader();

                    countersReader.forEach(
                        (counterId, typeId, keyBuffer, label) ->
                        {
                            if (ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID == typeId)
                            {
                                nodeRoleCounter.set(countersReader.getCounterValue(counterId));
                            }
                            else if (ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID == typeId)
                            {
                                electionStateCounter.set(countersReader.getCounterValue(counterId));
                            }
                        });
                }

                if (nodeRoleCounter.get() == Cluster.Role.LEADER.code() &&
                    electionStateCounter.get() == ElectionState.CLOSED.code())
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
        }
        else
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return -1;
        }
    }

    /**
     * Does a {@link ClusterMarkFile} exist in the cluster directory.
     *
     * @param clusterDir to check for if a mark file exists.
     * @return true if the cluster mark file exists.
     */
    public static boolean markFileExists(final File clusterDir)
    {
        final File markFileDir = resolveClusterMarkFileDir(clusterDir);
        final File markFile = new File(markFileDir, ClusterMarkFile.FILENAME);

        return markFile.exists();
    }

    /**
     * List the current members of a cluster.
     *
     * @param clusterMembership to populate.
     * @param clusterDir        where the cluster is running.
     * @param timeoutMs         to wait on the query completing.
     * @return true if successful.
     */
    public static boolean listMembers(
        final ClusterMembership clusterMembership, final File clusterDir, final long timeoutMs)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return queryClusterMembers(markFile, timeoutMs, clusterMembership);
            }
        }

        return false;
    }

    /**
     * Query the membership of a cluster.
     *
     * @param markFile          for the cluster component.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    public static boolean queryClusterMembers(
        final ClusterMarkFile markFile, final long timeoutMs, final ClusterMembership clusterMembership)
    {
        return queryClusterMembers(markFile.loadControlProperties(), timeoutMs, clusterMembership);
    }

    /**
     * Query the membership of a cluster.
     *
     * @param controlProperties from a {@link ClusterMarkFile}.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    public static boolean queryClusterMembers(
        final ClusterNodeControlProperties controlProperties,
        final long timeoutMs,
        final ClusterMembership clusterMembership)
    {
        final MutableLong id = new MutableLong(NULL_VALUE);
        final ClusterControlAdapter.Listener listener = new ClusterControlAdapter.Listener()
        {
            public void onClusterMembersResponse(
                final long correlationId,
                final int leaderMemberId,
                final String activeMembers,
                final String passiveMembers)
            {
                if (correlationId == id.get())
                {
                    clusterMembership.leaderMemberId = leaderMemberId;
                    clusterMembership.activeMembersStr = activeMembers;
                    clusterMembership.passiveMembersStr = passiveMembers;
                    id.set(NULL_VALUE);
                }
            }

            public void onClusterMembersExtendedResponse(
                final long correlationId,
                final long currentTimeNs,
                final int leaderMemberId,
                final int memberId,
                final List<ClusterMember> activeMembers,
                final List<ClusterMember> passiveMembers)
            {
                if (correlationId == id.get())
                {
                    clusterMembership.currentTimeNs = currentTimeNs;
                    clusterMembership.leaderMemberId = leaderMemberId;
                    clusterMembership.memberId = memberId;
                    clusterMembership.activeMembers = activeMembers;
                    clusterMembership.passiveMembers = passiveMembers;
                    clusterMembership.activeMembersStr = ClusterMember.encodeAsString(activeMembers);
                    clusterMembership.passiveMembersStr = ClusterMember.encodeAsString(passiveMembers);
                    id.set(NULL_VALUE);
                }
            }
        };

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(controlProperties.aeronDirectoryName));
            ConcurrentPublication publication = aeron.addPublication(
                controlProperties.controlChannel, controlProperties.consensusModuleStreamId);
            ConsensusModuleProxy consensusModuleProxy = new ConsensusModuleProxy(publication);
            ClusterControlAdapter clusterControlAdapter = new ClusterControlAdapter(aeron.addSubscription(
                controlProperties.controlChannel, controlProperties.serviceStreamId), listener))
        {
            final long deadlineMs = System.currentTimeMillis() + timeoutMs;
            final long correlationId = aeron.nextCorrelationId();
            id.set(correlationId);

            while (!publication.isConnected())
            {
                if (System.currentTimeMillis() > deadlineMs)
                {
                    break;
                }
                Thread.yield();
            }

            while (!consensusModuleProxy.clusterMembersQuery(correlationId))
            {
                if (System.currentTimeMillis() > deadlineMs)
                {
                    break;
                }
                Thread.yield();
            }

            while (NULL_VALUE != id.get())
            {
                if (0 == clusterControlAdapter.poll())
                {
                    if (System.currentTimeMillis() > deadlineMs)
                    {
                        break;
                    }
                    Thread.yield();
                }
            }
        }

        return true;
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    public static long nextBackupQueryDeadlineMs(final File clusterDir)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return nextBackupQueryDeadlineMs(markFile);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    public static long nextBackupQueryDeadlineMs(final ClusterMarkFile markFile)
    {
        final String aeronDirectoryName = markFile.decoder().aeronDirectory();
        final MutableLong nextQueryMs = new MutableLong(NULL_VALUE);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
        {
            aeron.countersReader().forEach(
                (counterId, typeId, keyBuffer, label) ->
                {
                    if (ClusterBackup.QUERY_DEADLINE_TYPE_ID == typeId)
                    {
                        nextQueryMs.set(aeron.countersReader().getCounterValue(counterId));
                    }
                });
        }

        return nextQueryMs.get();
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @param timeMs     to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    public static boolean nextBackupQueryDeadlineMs(final File clusterDir, final long timeMs)
    {
        if (markFileExists(clusterDir) || TIMEOUT_MS > 0)
        {
            try (ClusterMarkFile markFile = openMarkFile(clusterDir))
            {
                return nextBackupQueryDeadlineMs(markFile, timeMs);
            }
        }

        return false;
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @param timeMs   to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    public static boolean nextBackupQueryDeadlineMs(final ClusterMarkFile markFile, final long timeMs)
    {
        final String aeronDirectoryName = markFile.decoder().aeronDirectory();
        final MutableBoolean result = new MutableBoolean(false);

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)))
        {
            final CountersReader countersReader = aeron.countersReader();

            countersReader.forEach(
                (counterId, typeId, keyBuffer, label) ->
                {
                    if (ClusterBackup.QUERY_DEADLINE_TYPE_ID == typeId)
                    {
                        final AtomicCounter atomicCounter = new AtomicCounter(
                            countersReader.valuesBuffer(), counterId, null);

                        atomicCounter.setOrdered(timeMs);
                        result.value = true;
                    }
                });
        }

        return result.value;
    }

    /**
     * Invalidate the latest snapshot so recovery will use an earlier one or log if no earlier one exists.
     *
     * @param out        to print the operation result.
     * @param clusterDir where the cluster component is running.
     * @return true if the latest snapshot was invalidated.
     */
    public static boolean invalidateLatestSnapshot(final PrintStream out, final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            final boolean result = recordingLog.invalidateLatestSnapshot();
            out.println(" invalidate latest snapshot: " + result);
            return result;
        }
    }

    /**
     * Print out a summary of the state captured in the latest consensus module snapshot.
     *
     * @param out        to print the operation result.
     * @param clusterDir where the cluster is running.
     */
    public static void describeLatestConsensusModuleSnapshot(final PrintStream out, final File clusterDir)
    {
        final RecordingLog.Entry entry = findLatestValidSnapshot(clusterDir);
        if (null == entry)
        {
            out.println("Snapshot not found");
            return;
        }

        final ClusterNodeControlProperties properties = loadControlProperties(clusterDir);

        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(properties.aeronDirectoryName));
            AeronArchive archive = AeronArchive.connect(archiveCtx.aeron(aeron)))
        {
            final String channel = AERON_CLUSTER_TOOL_REPLAY_CHANNEL;
            final int streamId = AERON_CLUSTER_TOOL_REPLAY_STREAM_ID;
            final int sessionId = (int)archive.startReplay(
                entry.recordingId, 0, AeronArchive.NULL_LENGTH, channel, streamId);

            final String replayChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replayChannel, streamId))
            {
                Image image;
                while ((image = subscription.imageBySessionId(sessionId)) == null)
                {
                    archive.checkForErrorResponse();
                    Thread.yield();
                }

                final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(
                    image, new ConsensusModuleSnapshotPrinter(out));

                while (true)
                {
                    final int fragments = adapter.poll();
                    if (adapter.isDone())
                    {
                        break;
                    }

                    if (0 == fragments)
                    {
                        if (image.isClosed())
                        {
                            throw new ClusterException("snapshot ended unexpectedly: " + image);
                        }

                        archive.checkForErrorResponse();
                        Thread.yield();
                    }
                }

                out.println("Consensus Module Snapshot End:" +
                    " memberId=" + properties.memberId +
                    " recordingId=" + entry.recordingId +
                    " length=" + image.position());
            }
        }
    }

    /**
     * Instruct the cluster to take a snapshot.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean snapshot(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SNAPSHOT,
            true,
            TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Instruct the cluster to suspend operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean suspend(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SUSPEND,
            false,
            TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Instruct the cluster to resume operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean resume(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.SUSPENDED,
            ClusterControl.ToggleState.RESUME,
            true,
            TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Instruct the cluster to shut down.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean shutdown(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.SHUTDOWN,
            false,
            TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Instruct the cluster to abort.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean abort(final File clusterDir, final PrintStream out)
    {
        return toggleClusterState(
            out,
            clusterDir,
            ConsensusModule.State.ACTIVE,
            ClusterControl.ToggleState.ABORT,
            false,
            TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Finds the latest valid snapshot from the log file.
     *
     * @param clusterDir where the cluster node is running.
     * @return entry or {@code null} if not found.
     */
    static RecordingLog.Entry findLatestValidSnapshot(final File clusterDir)
    {
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false))
        {
            return recordingLog.getLatestSnapshot(ConsensusModule.Configuration.SERVICE_ID);
        }
    }

    /**
     * Load {@link ClusterNodeControlProperties} from the mark file.
     *
     * @param clusterDir where the cluster node is running.
     * @return control properties.
     */
    static ClusterNodeControlProperties loadControlProperties(final File clusterDir)
    {
        final ClusterNodeControlProperties properties;
        try (ClusterMarkFile markFile = openMarkFile(clusterDir))
        {
            properties = markFile.loadControlProperties();
        }
        return properties;
    }

    @SuppressWarnings("MethodLength")
    static boolean toggleClusterState(
        final PrintStream out,
        final File clusterDir,
        final ConsensusModule.State expectedState,
        final ClusterControl.ToggleState toggleState,
        final boolean waitForToggleToComplete,
        final long defaultTimeoutMs)
    {
        return toggleState(
            out,
            clusterDir,
            true,
            expectedState,
            toggleState,
            ToggleApplication.CLUSTER_CONTROL,
            waitForToggleToComplete,
            defaultTimeoutMs);
    }

    @SuppressWarnings("MethodLength")
    static <T extends Enum<T>> boolean toggleState(
        final PrintStream out,
        final File clusterDir,
        final boolean isLeaderRequired,
        final ConsensusModule.State expectedState,
        final T targetState,
        final ToggleApplication<T> toggleApplication,
        final boolean waitForToggleToComplete,
        final long defaultTimeoutMs)
    {
        if (!markFileExists(clusterDir) && TIMEOUT_MS <= 0)
        {
            out.println(ClusterMarkFile.FILENAME + " does not exist.");
            return false;
        }

        final int clusterId;
        final ClusterNodeControlProperties clusterNodeControlProperties;
        try (ClusterMarkFile markFile = openMarkFile(clusterDir))
        {
            clusterId = markFile.clusterId();
            clusterNodeControlProperties = markFile.loadControlProperties();
        }

        final ClusterMembership clusterMembership = new ClusterMembership();
        final long queryTimeoutMs = Math.max(TimeUnit.SECONDS.toMillis(1), TIMEOUT_MS);

        if (!queryClusterMembers(clusterNodeControlProperties, queryTimeoutMs, clusterMembership))
        {
            out.println("Timed out querying cluster.");
            return false;
        }

        final String prefix = "Member [" + clusterMembership.memberId + "]: ";

        if (isLeaderRequired && clusterMembership.leaderMemberId != clusterMembership.memberId)
        {
            out.println(prefix + "Current node is not the leader (leaderMemberId = " +
                clusterMembership.leaderMemberId + "), unable to " + targetState);
            return false;
        }

        final File cncFile = new File(clusterNodeControlProperties.aeronDirectoryName, CncFileDescriptor.CNC_FILE);
        if (!cncFile.exists())
        {
            out.println(prefix + "Unable to locate media driver. C`n`C file [" + cncFile.getAbsolutePath() +
                "] does not exist.");
            return false;
        }

        final CountersReader countersReader = ClusterControl.mapCounters(cncFile);
        try
        {
            final ConsensusModule.State moduleState = ConsensusModule.State.find(countersReader, clusterId);
            if (null == moduleState)
            {
                out.println(prefix + "Unable to resolve state of consensus module.");
                return false;
            }

            if (expectedState != moduleState)
            {
                out.println(prefix + "Unable to " + targetState + " as the state of the consensus module is " +
                    moduleState + ", but needs to be " + expectedState);
                return false;
            }

            final AtomicCounter controlToggle = toggleApplication.find(countersReader, clusterId);
            if (null == controlToggle)
            {
                out.println(prefix + "Failed to find control toggle");
                return false;
            }

            if (!toggleApplication.apply(controlToggle, targetState))
            {
                out.println(prefix + "Failed to apply " + targetState + ", current toggle value = " +
                    ClusterControl.ToggleState.get(controlToggle));
                return false;
            }

            if (waitForToggleToComplete)
            {
                final long toggleTimeoutMs = Math.max(defaultTimeoutMs, TIMEOUT_MS);
                final long deadlineMs = System.currentTimeMillis() + toggleTimeoutMs;
                T currentState = null;

                do
                {
                    Thread.yield();
                    if (System.currentTimeMillis() > deadlineMs)
                    {
                        break;
                    }

                    currentState = toggleApplication.get(controlToggle);
                }
                while (!toggleApplication.isNeutral(currentState));

                if (!toggleApplication.isNeutral(currentState))
                {
                    out.println(prefix + "Timed out after " + toggleTimeoutMs + "ms waiting for " +
                        targetState + " to complete.");
                }
            }

            out.println(prefix + targetState + " applied successfully");

            return true;

        }
        finally
        {
            IoUtil.unmap(countersReader.valuesBuffer().byteBuffer());
        }
    }

    static ClusterMarkFile openMarkFile(final File clusterDir)
    {
        final File markFileDir = resolveClusterMarkFileDir(clusterDir);
        return new ClusterMarkFile(
            markFileDir, ClusterMarkFile.FILENAME, System::currentTimeMillis, TIMEOUT_MS, null);
    }

    private static ClusterMarkFile[] openServiceMarkFiles(final File clusterDir, final Consumer<String> logger)
    {
        File[] clusterMarkFileNames =
            clusterDir.listFiles((dir, name) ->
            name.startsWith(ClusterMarkFile.SERVICE_FILENAME_PREFIX) &&
            (name.endsWith(ClusterMarkFile.FILE_EXTENSION) ||
            name.endsWith(ClusterMarkFile.LINK_FILE_EXTENSION)));

        if (null == clusterMarkFileNames)
        {
            clusterMarkFileNames = new File[0];
        }

        final ArrayList<File> resolvedMarkFileNames = new ArrayList<>();
        resolveServiceMarkFileNames(clusterMarkFileNames, resolvedMarkFileNames);

        final ClusterMarkFile[] clusterMarkFiles = new ClusterMarkFile[clusterMarkFileNames.length];

        for (int i = 0, n = resolvedMarkFileNames.size(); i < n; i++)
        {
            final File resolvedMarkFile = resolvedMarkFileNames.get(i);
            clusterMarkFiles[i] = new ClusterMarkFile(
                resolvedMarkFile.getParentFile(),
                resolvedMarkFile.getName(),
                System::currentTimeMillis,
                TIMEOUT_MS,
                logger);
        }

        return clusterMarkFiles;
    }

    private static void resolveServiceMarkFileNames(final File[] clusterMarkFiles, final ArrayList<File> resolvedFiles)
    {
        final HashSet<String> resolvedServices = new HashSet<>();

        for (final File clusterMarkFile : clusterMarkFiles)
        {
            final String filename = clusterMarkFile.getName();
            if (filename.endsWith(ClusterMarkFile.LINK_FILE_EXTENSION))
            {
                final String name = filename.substring(
                    0, filename.length() - ClusterMarkFile.LINK_FILE_EXTENSION.length());

                final File markFileDir = resolveDirectoryFromLinkFile(clusterMarkFile);
                resolvedFiles.add(new File(markFileDir, name + ClusterMarkFile.FILE_EXTENSION));
                resolvedServices.add(name);
            }
        }

        for (final File clusterMarkFile : clusterMarkFiles)
        {
            final String filename = clusterMarkFile.getName();
            if (filename.endsWith(ClusterMarkFile.FILE_EXTENSION))
            {
                final String name = filename.substring(
                    0, filename.length() - ClusterMarkFile.FILE_EXTENSION.length());

                if (!resolvedServices.contains(name))
                {
                    resolvedFiles.add(clusterMarkFile);
                    resolvedServices.add(name);
                }
            }
        }
    }

    static void printTypeAndActivityTimestamp(final PrintStream out, final ClusterMarkFile markFile)
    {
        printTypeAndActivityTimestamp(
            out,
            markFile.decoder().componentType().toString(),
            markFile.decoder().startTimestamp(),
            markFile.activityTimestampVolatile());
    }

    static void printTypeAndActivityTimestamp(
        final PrintStream out,
        final String clusterComponentType,
        final long startTimestampMs,
        final long activityTimestampMs)
    {
        out.print("Type: " + clusterComponentType + " ");
        out.format(
            "%1$tH:%1$tM:%1$tS (start: %2$tF %2$tH:%2$tM:%2$tS, activity: %3$tF %3$tH:%3$tM:%3$tS)%n",
            new Date(),
            new Date(startTimestampMs),
            new Date(activityTimestampMs));
    }

    private static void printErrors(final PrintStream out, final ClusterMarkFile markFile)
    {
        printErrors(out, markFile::errorBuffer, "Cluster");
    }

    static void printErrors(final PrintStream out, final Supplier<AtomicBuffer> errorBuffer, final String name)
    {
        out.println(name + " component error log:");
        CommonContext.printErrorLog(errorBuffer.get(), out);
    }

    static void printDriverErrors(final PrintStream out, final String aeronDirectory)
    {
        out.println("Aeron driver error log (directory: " + aeronDirectory + "):");
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        MappedByteBuffer cncByteBuffer = null;
        try
        {
            cncByteBuffer = IoUtil.mapExistingFile(cncFile, FileChannel.MapMode.READ_ONLY, "cnc");
            final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
            final int cncVersion = cncMetaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

            CncFileDescriptor.checkVersion(cncVersion);
            CommonContext.printErrorLog(CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), out);
        }
        finally
        {
            if (null != cncByteBuffer)
            {
                IoUtil.unmap(cncByteBuffer);
            }
        }
    }

    private static boolean isRecordingLogSorted(final List<RecordingLog.Entry> entries)
    {
        for (int i = entries.size() - 1; i >= 0; i--)
        {
            if (entries.get(i).entryIndex != i)
            {
                return false;
            }
        }

        return true;
    }

    private static void updateRecordingLog(final File clusterDir, final List<RecordingLog.Entry> entries)
    {
        final Path recordingLog = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME);
        try
        {
            if (entries.isEmpty())
            {
                Files.delete(recordingLog);
            }
            else
            {
                final Path newRecordingLog = clusterDir.toPath().resolve(RecordingLog.RECORDING_LOG_FILE_NAME + ".tmp");
                Files.deleteIfExists(newRecordingLog);
                final ByteBuffer byteBuffer = ByteBuffer
                    .allocateDirect(RecordingLog.MAX_ENTRY_LENGTH).order(LITTLE_ENDIAN);
                final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
                try (FileChannel fileChannel = FileChannel.open(newRecordingLog, CREATE_NEW, WRITE))
                {
                    long position = 0;
                    for (final RecordingLog.Entry e : entries)
                    {
                        RecordingLog.writeEntryToBuffer(e, buffer);
                        byteBuffer.limit(e.length()).position(0);

                        if (e.length() != fileChannel.write(byteBuffer, position))
                        {
                            throw new ClusterException("failed to write recording");
                        }
                        position += e.length();
                    }
                }
                finally
                {
                    BufferUtil.free(byteBuffer);
                }

                Files.delete(recordingLog);
                Files.move(newRecordingLog, recordingLog);
            }
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    static File resolveClusterMarkFileDir(final File dir)
    {
        final File linkFile = new File(dir, ClusterMarkFile.LINK_FILENAME);
        return linkFile.exists() ? resolveDirectoryFromLinkFile(linkFile) : dir;
    }

    static File resolveDirectoryFromLinkFile(final File linkFile)
    {
        final File markFileDir;

        try
        {
            final byte[] bytes = Files.readAllBytes(linkFile.toPath());
            final String markFileDirPath = new String(bytes, US_ASCII).trim();
            markFileDir = new File(markFileDirPath);
        }
        catch (final IOException ex)
        {
            throw new RuntimeException("failed to read link file=" + linkFile, ex);
        }

        return markFileDir;
    }

    static void exitWithErrorOnFailure(final boolean success)
    {
        if (!success)
        {
            System.exit(-1);
        }
    }

    static void printHelp()
    {
        System.out.format(
            "Usage: <cluster-dir> <command> [options]%n" +
            "                         describe: prints out all descriptors in the mark file.%n" +
            "                              pid: prints PID of cluster component.%n" +
            "                    recovery-plan: [service count] prints recovery plan of cluster component.%n" +
            "                    recording-log: prints recording log of cluster component.%n" +
            "               sort-recording-log: reorders entries in the recording log to match the order in memory.%n" +
            " seed-recording-log-from-snapshot: creates a new recording log based on the latest valid snapshot.%n" +
            "                           errors: prints Aeron and cluster component error logs.%n" +
            "                     list-members: prints leader memberId, active members and passive members lists.%n" +
            "                     backup-query: [delay] get, or set, time of next backup query.%n" +
            "       invalidate-latest-snapshot: marks the latest snapshot as a invalid so the previous is loaded.%n" +
            "                         snapshot: triggers a snapshot on the leader.%n" +
            "                          suspend: suspends appending to the log.%n" +
            "                           resume: resumes appending to the log.%n" +
            "                         shutdown: initiates an orderly stop of the cluster with a snapshot.%n" +
            "                            abort: stops the cluster without a snapshot.%n" +
            "      describe-latest-cm-snapshot: prints the contents of the latest valid consensus module snapshot.%n" +
            "                        is-leader: returns zero if the cluster node is leader, non-zero if not.%n");
        System.out.flush();
    }
}
