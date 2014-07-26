package uk.co.real_logic.aeron.conductor;

import uk.co.real_logic.aeron.common.ErrorCode;
import uk.co.real_logic.aeron.common.command.LogBuffersMessageFlyweight;

import java.io.IOException;

/**
 * Callback interface for receiving messages from the driver.
 */
public interface DriverListener
{
    void onNewPublication(String channel,
                          int sessionId,
                          int streamId,
                          int termId,
                          int positionIndicatorId,
                          LogBuffersMessageFlyweight logBuffersMessage) throws IOException;

    void onNewConnection(String channel,
                         int sessionId,
                         int streamId,
                         int termId,
                         LogBuffersMessageFlyweight logBuffersMessage) throws IOException;

    void onError(ErrorCode errorCode, String message);

    void operationSucceeded();
}
