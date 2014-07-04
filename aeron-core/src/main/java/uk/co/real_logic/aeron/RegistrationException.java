package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.util.ErrorCode;

public class RegistrationException extends RuntimeException
{
    private final ErrorCode code;

    public RegistrationException(final ErrorCode code, final String msg)
    {
        super(msg);
        this.code = code;
    }

    public ErrorCode errorCode()
    {
        return code;
    }

}
