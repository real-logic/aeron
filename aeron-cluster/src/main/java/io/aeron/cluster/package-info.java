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

/**
 * Aeron Cluster provides support for fault-tolerant services as replicated state machines based on the
 * <a href="https://raft.github.io/raft.pdf" target="_blank">Raft</a> consensus algorithm.
 * <p>
 * The purpose of Aeron Cluster is to aggregate and sequence streams from cluster clients into a single log. A number of
 * nodes will replicate and archive the log to achieve fault tolerance.
 * {@link io.aeron.cluster.service.ClusteredService}s deterministically process the log and respond to cluster clients.
 * <p>
 * Aeron Cluster works on the concept of a strong leader using an adaptation of the
 * <a href="https://raft.github.io/raft.pdf" target="_blank">Raft</a> algorithm. The leader sequences the log and is
 * responsible for replicating it to other cluster members known as followers.
 * <p>
 * A number of components make up Aeron Cluster. Central is the {@link io.aeron.cluster.ConsensusModule} which sequences
 * the log and coordinates consensus for the recording of the sequenced log to persistent storage, and the services
 * consuming the log across cluster members. Aeron {@link io.aeron.archive.Archive} records the log to durable storage.
 * Services consume the log once a majority of the cluster members have safely recorded the log to durable storage.
 * <p>
 * To enable fast recovery, the services and consensus module can take a snapshot of their state as of a given log
 * position thus enabling recovery by loading the most recent snapshot and replaying logs from that point forward.
 * Snapshots are recorded as streams in the {@link io.aeron.archive.Archive} for local and remote replay so that a
 * distributed file system is not required.
 * <h2>Usage</h2>
 * The cluster can run in various configurations:
 * <ul>
 *     <li>
 *         <b>Single Node:</b> For development, debugging, or when a sequenced and archived log on a single node is
 *         sufficient.
 *     </li>
 *     <li>
 *         <b>Appointed Leader:</b> A leader of the cluster can be appointed via configuration without requiring an
 *         election. In the event of a leader failure then a follower will have to be manually appointed the new leader.
 *         This is not the recommended way to use Cluster. Automatic elections are more reliable.
 *     </li>
 *     <li>
 *         <b>Automatic Elections:</b> Automatic elections (default) can be enabled to have a leader elected at random
 *         from the members with the most up to date log.
 *     </li>
 * </ul>
 * <p>
 * The majority of cluster members determine consensus. Clusters should typically be 3 or 5 in population size.
 * However, 2 node clusters are supported whereby both members must agree the log and in the event of failure the
 * remaining member must be manually reconfigured as a single node cluster to progress.
 * <h2>Protocol</h2>
 * Messages are specified using <a href="https://github.com/real-logic/simple-binary-encoding" target="_blank">SBE</a>
 * in this schema
 * <a href="https://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml"
 *    target="_blank">aeron-cluster-codecs.xml</a>
 */
package io.aeron.cluster;