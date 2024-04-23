package io.aeron.config.validation;

import io.aeron.config.ConfigInfo;
import io.aeron.config.ExpectedCConfig;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

final class ValidationReport
{
    private final List<Entry> entries;

    ValidationReport()
    {
        entries = new ArrayList<>();
    }

    void addEntry(
        final ConfigInfo configInfo,
        final BiConsumer<Validation, ExpectedCConfig> validateCEnvVar,
        final BiConsumer<Validation, ExpectedCConfig> validateCDefault)
    {
        final Entry entry = new Entry(configInfo);
        final ExpectedCConfig c = configInfo.expectations.c;
        if (c.exists)
        {
            validate(validateCEnvVar, entry.envVarValidation, c);
            validate(validateCDefault, entry.defaultValidation, c);
        }
        entries.add(entry);
    }

    private void validate(
        final BiConsumer<Validation, ExpectedCConfig> func,
        final Validation validation,
        final ExpectedCConfig c)
    {
        try
        {
            func.accept(validation, c);
        }
        catch (final Exception e)
        {
            validation.invalid(e.getMessage());
            e.printStackTrace(validation.out());
        }
        finally
        {
            validation.close();
        }
    }

    void printOn(final PrintStream out)
    {
        entries.forEach(entry -> entry.printOn(out));
    }

    void printFailuresOn(final PrintStream out)
    {
        entries.forEach(entry -> entry.printFailuresOn(out));
    }
}
