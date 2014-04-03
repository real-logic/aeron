package uk.co.real_logic.aeron.mediadriver;

public class DefaultSenderFlowControlStrategy implements SenderFlowControlStrategy
{
    private int rightEdgeOfWindow;

    public DefaultSenderFlowControlStrategy()
    {
        rightEdgeOfWindow = 0;
    }

    @Override
    public int onStatusMessage(final long termId, final int highestContiguousSequenceNumber, final int receiverWindow)
    {
        // TODO: review this logic
        rightEdgeOfWindow = Math.max(rightEdgeOfWindow, highestContiguousSequenceNumber + receiverWindow);
        return rightEdgeOfWindow;
    }

}
