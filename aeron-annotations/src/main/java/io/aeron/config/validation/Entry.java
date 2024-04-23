package io.aeron.config.validation;

import java.io.PrintStream;
import io.aeron.config.ConfigInfo;

class Entry
{
    private final ConfigInfo configInfo;
    final Validation envVarValidation;
    final Validation defaultValidation;

    Entry(final ConfigInfo configInfo)
    {
        this.configInfo = configInfo;
        this.envVarValidation = new Validation();
        this.defaultValidation = new Validation();
    }

    void printOn(final PrintStream out)
    {
        if (configInfo.expectations.c.exists)
        {
            out.println(configInfo.id);
            envVarValidation.printOn(out);
            defaultValidation.printOn(out);
        }
        else
        {
            out.println(configInfo.id + " -- SKIPPED");
        }
    }

    void printFailuresOn(final PrintStream out)
    {
        if (configInfo.expectations.c.exists && (!envVarValidation.isValid() || !defaultValidation.isValid()))
        {
            printOn(out);
        }
    }
}
