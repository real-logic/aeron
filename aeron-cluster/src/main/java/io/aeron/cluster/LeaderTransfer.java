package io.aeron.cluster;

import static io.aeron.cluster.LeaderTransferState.*;

public class LeaderTransfer {
    private final LogPublisher logPublisher;
    private final ConsensusPublisher consensusPublisher;
    private final ClusterMember[] activeMembers;
    private final ClusterMember newLeader;
    private long stepDownDeadlineNs;
    private final long leadershipTermId;
    private final ConsensusModuleAgent consensusModuleAgent;
    private LeaderTransferState state = INIT;

    LeaderTransfer(
            final LogPublisher logPublisher,
            final ConsensusPublisher consensusPublisher,
            final long leadershipTermId,
            final ClusterMember[] activeMembers,
            final ClusterMember newLeader,
            final ConsensusModule.Context ctx,
            final ConsensusModuleAgent consensusModuleAgent) {
        this.logPublisher = logPublisher;
        this.consensusPublisher = consensusPublisher;
        this.leadershipTermId = leadershipTermId;
        this.activeMembers = activeMembers;
        this.newLeader = newLeader;
        this.consensusModuleAgent = consensusModuleAgent;
        this.stepDownDeadlineNs = ctx.clusterClock().timeNanos() + ctx.electionTimeoutNs();
    }

    public int doWork(final long nowNs) {
        int workCount = 0;
        if (nowNs >= stepDownDeadlineNs) {
            state = TRANSFER_TIMEOUT;
        }
        switch (state) {
            case INIT:
                workCount += init();
                break;
            case WAITING_NOTIFY:
                workCount += waiting();
                break;
            case TRANSFER_TIMEOUT:
                workCount += complete();
                break;

        }
        return workCount;
    }

    private int init() {
        int workCount = 0;
        state = WAITING_NOTIFY;
        workCount++;
        return workCount;
    }

    private int waiting() {
        int workCount = 0;
        long logPosition = logPublisher.position();
        if (logPosition == newLeader.logPosition()) {
            if (consensusPublisher.enterElection(newLeader.publication(), leadershipTermId, newLeader.id(), false)) {
                for (ClusterMember member : activeMembers) {
                    if (member.id() != newLeader.id() && !member.isLeader()) {
                        consensusPublisher.enterElection(member.publication(), leadershipTermId, member.id(), true);
                    }
                }
                state = NOTIFIED;
            }
            workCount++;
        }
        return workCount;
    }

    private int complete() {
        int workCount = 0;
        consensusModuleAgent.leaderTransferComplete();
        return workCount;
    }
}