/*
 * Copyright 2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <b>Note:</b> Aeron Cluster is currently an experimental feature.
 * <p>
 * The purpose of Aeron Cluster is to aggregate and sequence streams from cluster clients into a single log which is
 * replicated and archived on a number of nodes to achieve resilience. Cluster services process this log and respond
 * to cluster clients.
 * <p>
 * Aeron Cluster works on the concept of a strong leader using an adaptation of the
 * <a href="https://raft.github.io/raft.pdf" target="_blank">Raft</a> protocol. The leader sequences the log and is
 * responsible for replicating it to other cluster members known as followers.
 * <p>
 * Aeron Cluster is composes of a number of components. Central is the Consensus Module which sequences the log and
 * coordinates consensus on the archiving of the sequenced log, and the services consuming the log across cluster
 * members. Recording of the log to persistent storage is performed by the Aeron Archive module. Services are allowed
 * to consume the log once a majority of the cluster members have safely recorded the log to persistent storage.
 * <p>
 * To enable fast recovery the services and consensus module can take a snapshot of their state as of a given log
 * position thus enabling recovery by loading the most recent snapshot and replaying logs from that point forward.
 * Snapshots are recorded as streams in the Archive for local and remote replay so that a distributed file system is
 * not required.
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
 *     </li>
 *     <li>
 *         <b>Automatic Elections:</b> Automatic elections can be enabled to have a leader elected at random from the
 *         members with with the most up to date log.
 *     </li>
 * </ul>
 * <p>
 * Based on the membership size, consensus is determined by the majority of the cluster members. It is recommended that
 * clusters be 3 or 5 in population size. However 2 node clusters are supported whereby both members must agree the log
 * and in the event of failure the remaining member must be manually reconfigured as a single node cluster.
 * <h2>Protocol</h2>
 * Messages are specified using SBE in
 * <a href="https://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/resources/aeron-cluster-codecs.xml"
 *    target="_blank">aeron-cluster-codecs.xml</a>
 */
package io.aeron.cluster;