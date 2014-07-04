package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.util.ErrorCode;
import uk.co.real_logic.aeron.util.command.LogBuffersMessageFlyweight;

import java.io.IOException;

/**
 * .
 */
public interface MediaDriverListener
{

    void onNewPublication(
            String destination,
            long sessionId,
            long channelId,
            long termId,
            int positionIndicatorId,
            LogBuffersMessageFlyweight logBuffersMessage) throws IOException;


    void onNewConnectedSubscription(
            String destination,
            long sessionId,
            long channelId,
            long termId,
            LogBuffersMessageFlyweight logBuffersMessage) throws IOException;


    void onError(ErrorCode errorCode, String message);
}
