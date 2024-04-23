package io.aeron.config.validation;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

class Validation
{
    private boolean valid = false;

    private String message;

    private ByteArrayOutputStream ba_out;

    private PrintStream ps_out;

    boolean isValid()
    {
        return valid;
    }

    void close()
    {
        if (this.ps_out != null)
        {
            this.ps_out.close();
        }
    }

    void valid(final String message)
    {
        this.valid = true;
        this.message = message;
    }

    void invalid(final String message)
    {
        this.valid = false;
        this.message = message;
    }

    PrintStream out()
    {
        if (this.ps_out == null)
        {
            this.ba_out = new ByteArrayOutputStream();
            this.ps_out = new PrintStream(ba_out);
        }

        return ps_out;
    }

    void printOn(final PrintStream out)
    {
        out.println(" " + (this.valid ? "+" : "-") + " " + this.message);
        if (this.ps_out != null)
        {
            out.println(this.ba_out);
        }
    }
}
