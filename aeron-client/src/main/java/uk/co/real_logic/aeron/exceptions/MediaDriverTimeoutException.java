package uk.co.real_logic.aeron.exceptions;

/**
 * A timeout has occurred while waiting on the media driver responding to an operation.
 */
public class MediaDriverTimeoutException extends RuntimeException
{
    public MediaDriverTimeoutException(final String message)
    {
        super(message);
    }
}
