package io.aeron.cluster;

public enum LeaderTransferState {
    INIT,
    WAITING_NOTIFY,
    NOTIFIED,
    TRANSFER_TIMEOUT
}
