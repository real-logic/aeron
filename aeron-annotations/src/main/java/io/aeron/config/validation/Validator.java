package io.aeron.config.validation;

import io.aeron.config.ConfigInfo;
import io.aeron.config.DefaultType;
import io.aeron.config.ExpectedCConfig;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Collection;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class Validator
{
    static ValidationReport validate(
        final Collection<ConfigInfo> configInfoCollection,
        final String sourceDir)
    {
        final Validator validator = new Validator(sourceDir);
        validator.validate(configInfoCollection);
        return validator.report;
    }

    private final String sourceDir;
    private final ScriptEngine scriptEngine;
    private final ValidationReport report;

    private Validator(final String sourceDir)
    {
        this.sourceDir = sourceDir;
        this.scriptEngine = new ScriptEngineManager().getEngineByName("JavaScript");
        this.report = new ValidationReport();
    }

    private void validate(final Collection<ConfigInfo> configInfoCollection)
    {
        configInfoCollection.forEach(this::validateCExpectations);
    }

    private void validateCExpectations(final ConfigInfo configInfo)
    {
        report.addEntry(configInfo, this::validateCEnvVar, this::validateCDefault);
    }

    private void validateCEnvVar(final Validation validation, final ExpectedCConfig c)
    {
        if (Objects.isNull(c.envVarFieldName))
        {
            return;
        }

        /* Expectations:
         * #define AERON_OPTION_ENV_VAR "AERON_OPTION"
         */
        final String pattern = "#define[ \t]+" + c.envVarFieldName + "[ \t]+\"" + c.envVar + "\"";
        final Grep grep = Grep.execute(pattern, sourceDir);
        if (grep.success())
        {
            validation.valid("Expected Env Var found in " + grep.getFilenameAndLine());
        }
        else
        {
            validation.invalid("Expected Env Var NOT found.  `grep` command:\n" + grep.getCommandString());
        }
    }

    private void validateCDefault(final Validation validation, final ExpectedCConfig c)
    {
        if (Objects.isNull(c.defaultFieldName))
        {
            return;
        }

        /* Expectations:
         * #define AERON_OPTION_DEFAULT ("some_string")
         * #define AERON_OPTION_DEFAULT (1234)
         * #define AERON_OPTION_DEFAULT (10 * 1024)
         * #define AERON_OPTION_DEFAULT (1024 * INT64_C(1000))
         * #define AERON_OPTION_DEFAULT false
         * #define AERON_OPTION_DEFAULT (true)
         */
        final String pattern = "#define[ \t]+" + c.defaultFieldName;

        final Grep grep = Grep.execute(pattern, sourceDir);
        if (!grep.success())
        {
            validation.invalid("Expected Default NOT found.  `grep` command:\n" + grep.getCommandString());
            return;
        }

        final Matcher matcher = Pattern.compile(pattern + "(.*)$").matcher(grep.getOutput());
        if (!matcher.find())
        {
            throw new RuntimeException("asdf");
        }

        final String originalFoundDefaultString = matcher.group(1).trim();

        if (c.defaultValueType == DefaultType.STRING)
        {
            final String foundDefaultString = originalFoundDefaultString
                .replaceFirst("^\\(", "")
                .replaceFirst("\\)$", "")
                .replaceFirst("^\"", "")
                .replaceFirst("\"$", "");

            if (foundDefaultString.equals(c.defaultValue))
            {
                validation.valid("Expected Default (\"" + foundDefaultString + "\") found in " +
                    grep.getFilenameAndLine());
            }
            else
            {
                validation.invalid("Expected Default string doesn't match.  Expected '" + c.defaultValue +
                    "' but found '" + foundDefaultString + "' in " + grep.getFilenameAndLine());
            }
        }
        else if (c.defaultValueType == DefaultType.BOOLEAN)
        {
            final String foundDefaultString = originalFoundDefaultString
                .replaceFirst("^\\(", "")
                .replaceFirst("\\)$", "");

            if (foundDefaultString.equals(c.defaultValue.toString()))
            {
                validation.valid("Expected Default '" + foundDefaultString + "' found in " +
                    grep.getFilenameAndLine());
            }
            else
            {
                validation.invalid("boolean doesn't match");
            }
        }
        else if (c.defaultValueType.isNumeric())
        {
            final String foundDefaultString = originalFoundDefaultString
                .replaceAll("INT64_C", "")
                .replaceAll("UINT32_C", "");

            try
            {
                final String evaluatedFoundDefaultString = scriptEngine.eval(
                    "AERON_TERM_BUFFER_LENGTH_DEFAULT = (16 * 1024 * 1024);\n" + // this feels like a (very) bad idea
                    "(" + foundDefaultString + ").toFixed(0)" // avoid scientific notation
                ).toString();

                if (evaluatedFoundDefaultString.equals(c.defaultValue.toString()))
                {
                    validation.valid("Expected Default '" + foundDefaultString + "'" +
                        (foundDefaultString.equals(evaluatedFoundDefaultString) ?
                        "" : " (" + evaluatedFoundDefaultString + ")") +
                        " found in " + grep.getFilenameAndLine());
                }
                else
                {
                    validation.invalid("found " + foundDefaultString +
                        " (" + evaluatedFoundDefaultString + ") but expected " + c.defaultValue);
                }
            }
            catch (final ScriptException e)
            {
                validation.invalid("Expected Default - unable to evaluate expression '" +
                    originalFoundDefaultString + "' in " + grep.getFilenameAndLine());
                e.printStackTrace(validation.out());
            }
        }
        else
        {
            validation.invalid("bad default type");
        }
    }
}
