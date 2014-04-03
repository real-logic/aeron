package uk.co.real_logic.aeron.mediadriver;

public interface SenderFlowControlStrategy
{

    /**
     * Updates the rightEdgeOfWindow based upon new status message information
     *
     * @return the calculated rightEdgeOfWindow
     */
    int onStatusMessage(final long termId,
                        final int highestContiguousSequenceNumber,
                        final int receiverWindow);

}
