package io.aeron.counter.validation;

import io.aeron.counter.CounterInfo;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

final class ValidationReport
{
    private final List<Validation> validations;

    ValidationReport()
    {
        validations = new ArrayList<>();
    }

    void addValidation(
        final CounterInfo counterInfo,
        final BiConsumer<Validation, CounterInfo> validateFunc)
    {
        final Validation validation = new Validation(counterInfo.name);
        validate(validateFunc, validation, counterInfo);
        validations.add(validation);
    }

    void addValidation(
        final boolean valid,
        final String name,
        final String message)
    {
        final Validation validation = new Validation(name);
        if (valid)
        {
            validation.valid(message);
        }
        else
        {
            validation.invalid(message);
        }
        validations.add(validation);
    }

    private void validate(
        final BiConsumer<Validation, CounterInfo> func,
        final Validation validation,
        final CounterInfo c)
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
        validations.forEach(validation -> validation.printOn(out));
    }

    void printFailuresOn(final PrintStream out)
    {
        validations.stream().filter(validation -> !validation.isValid()).forEach(validation -> validation.printOn(out));
    }
}
