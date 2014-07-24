package uk.co.real_logic.aeron.exceptions;

import uk.co.real_logic.aeron.common.ErrorCode;

/**
 * Caused when a error occurs during addition or release of {@link uk.co.real_logic.aeron.Publication}s
 * or {@link uk.co.real_logic.aeron.Subscription}s
 */
public class RegistrationException extends RuntimeException
{
    private final ErrorCode code;

    public RegistrationException(final ErrorCode code, final String msg)
    {
        super(msg);
        this.code = code;
    }

    /**
     * Get the {@link ErrorCode} for the specific exception.
     *
     * @return the {@link ErrorCode} for the specific exception.
     */
    public ErrorCode errorCode()
    {
        return code;
    }
}
