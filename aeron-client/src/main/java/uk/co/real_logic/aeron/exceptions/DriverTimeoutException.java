package uk.co.real_logic.aeron.exceptions;

/**
 * A timeout has occurred while waiting on the media driver responding to an operation.
 */
public class DriverTimeoutException extends RuntimeException
{
    public DriverTimeoutException(final String message)
    {
        super(message);
    }
}
