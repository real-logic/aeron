package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.util.ClosableThread;
import uk.co.real_logic.aeron.util.HeaderFlyweight;
import uk.co.real_logic.aeron.util.command.MediaDriverFacade;

import java.util.List;

/**
 * Handles
 */
public final class AdminThread extends ClosableThread implements MediaDriverFacade
{
    // TODO: add correct types once comms buffers are committed
    private final Object recvBuffer;
    private final Object sendBuffer;

    public AdminThread()
    {
        recvBuffer = null;
        sendBuffer = null;
    }

    public void work() {
        // read from recvBuffer and delegate to event handlers
    }

    /* commands to MediaDriver */

    @Override
    public void sendAddChannel(String destination, long sessionId, long channelId) {

    }

    @Override
    public void sendRemoveSession(String destination, long sessionId) {

    }

    @Override
    public void sendRemoveChannel(String destination, long sessionId, long channelId) {

    }

    @Override
    public void sendRemoveTerm(String destination, long sessionId, long channelId, long termId) {

    }

    @Override
    public void sendAddReceiver(String destination, List<Long> channelIdList) {

    }

    @Override
    public void sendRemoveReceiver(String destination) {

    }

    /* callbacks from MediaDriver */

    @Override
    public void onFlowControlResponse(HeaderFlyweight header) {

    }

    @Override
    public void onErrorResponse(int code, byte[] message) {
        
    }

    @Override
    public void onError(int code, byte[] message) {

    }

    @Override
    public void onLocationResponse(List<byte[]> filenames) {

    }

    @Override
    public void onNewSession(long sessionId, List<byte[]> filenames) {

    }

}
