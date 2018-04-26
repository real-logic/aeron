Aeron Cluster
===

**Note:** Aeron Cluster is currently an experimental feature.

The purpose of Aeron Cluster is to aggregate and sequence streams from cluster clients into a single log which is
replicated and archived on a number of nodes to achieve resilience. Cluster services process this log and respond
to cluster clients.

Aeron Cluster works on the concept of a strong leader using an adaptation of the [Raft](https://raft.github.io/raft.pdf)
protocol. The leader sequences the log and is responsible for replicating it to other cluster members known as
followers.

Aeron Cluster is composes of a number of components. Central is the Consensus Module which sequences the log and
coordinates consensus on the archiving of the sequenced log, and the services consuming the log across cluster members.
Recording of the log to persistent storage is performed by the Aeron Archive module. Services are allowed to consume
the log once a majority of the cluster members have safely recorded the log to persistent storage.

To enable fast recovery the services and consensus module can take a snapshot of their state as of a given log position
thus enabling recovery by loading the most recent snapshot and replaying logs from that point forward. Snapshots are
recorded as streams in the Archive for local and remote replay so that a distributed file system is not required.

Usage
=====

The cluster can run in various configurations:

 - **Single Node:** For development, debugging, or when a sequenced and archived log on a single node is sufficient.
 - **Appointed Leader:** A leader of the cluster can be appointed via configuration without requiring an election.
    In the event of a leader failure then a follower will have to be manually appointed the new leader.
 - **Automatic Elections:** Automatic elections can be enabled to have a leader elected at random from the members with
    with the most up to date log.
       
Based on the membership size, consensus is determined by the majority of the cluster members. It is recommended that
clusters be 3 or 5 in population size. However 2 node clusters are supported whereby both members must agree the log 
and in the event of failure the remaining member must be manually reconfigured as a single node cluster.

Protocol
=====

Messages are specified using SBE in [aeron-cluster-codecs.xml](https://github.com/real-logic/aeron/blob/master/aeron-cluster/src/main/resources/aeron-cluster-codecs.xml).
