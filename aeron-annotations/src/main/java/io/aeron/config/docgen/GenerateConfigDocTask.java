/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.config.docgen;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aeron.config.ConfigInfo;
import io.aeron.config.DefaultType;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A gradle task for generating config documentation
 */
public class GenerateConfigDocTask
{
    private static String toString(final Object a)
    {
        return a == null ? "" : a.toString();
    }

    private static FileWriter writer;

    /**
     * @param args
     * Arg 0 should be the location of a config-info.json file with a list of ConfigInfo objects
     * Arg 1 should be the location of an output file where a .md file is to be written
     *
     * @throws Exception
     * it sure does
     */
    public static void main(final String[] args) throws Exception
    {
        try (FileWriter writer = new FileWriter(args[1]))
        {
            GenerateConfigDocTask.writer = writer;

            final List<ConfigInfo> config = fetchConfig(args[0])
                .stream()
                .sorted(Comparator.comparing(a -> a.id))
                .collect(Collectors.toList());

            for (final ConfigInfo configInfo: config)
            {
                writeHeader(toHeaderString(configInfo.id));
                write("Description", configInfo.propertyNameDescription);
                write("Type",
                    (DefaultType.isUndefined(configInfo.overrideDefaultValueType) ?
                    configInfo.defaultValueType :
                    configInfo.overrideDefaultValueType).getSimpleName());
                writeCode("System Property", configInfo.propertyName);
                if (configInfo.context != null && !configInfo.context.isEmpty())
                {
                    writeCode("Context", configInfo.context);
                }
                if (configInfo.contextDescription != null && !configInfo.contextDescription.isEmpty())
                {
                    write("Context Description", configInfo.contextDescription);
                }
                if (configInfo.uriParam != null && !configInfo.uriParam.isEmpty())
                {
                    writeCode("URI Param", configInfo.uriParam);
                }

                if (configInfo.defaultDescription != null)
                {
                    write("Default Description", configInfo.defaultDescription);
                }
                final String defaultValue = configInfo.overrideDefaultValue == null ?
                    toString(configInfo.defaultValue) :
                    configInfo.overrideDefaultValue.toString();
                write("Default", getDefaultString(
                    defaultValue,
                    configInfo.isTimeValue == Boolean.TRUE,
                    configInfo.timeUnit));
                if (configInfo.isTimeValue == Boolean.TRUE)
                {
                    write("Time Unit", configInfo.timeUnit.toString());
                }

                if (configInfo.expectations.c.exists)
                {
                    writeCode("C Env Var", configInfo.expectations.c.envVar);
                    write("C Default", getDefaultString(
                        toString(configInfo.expectations.c.defaultValue),
                        configInfo.isTimeValue == Boolean.TRUE,
                        configInfo.timeUnit));
                }
                writeLine();
            }
        }
        catch (final IOException e)
        {
            e.printStackTrace(System.err);
        }
        finally
        {
            GenerateConfigDocTask.writer = null;
        }
    }

    private static List<ConfigInfo> fetchConfig(final String configInfoFilename) throws Exception
    {
        return new ObjectMapper().readValue(
            Paths.get(configInfoFilename).toFile(),
            new TypeReference<List<ConfigInfo>>()
            {
            });
    }


    private static void writeHeader(final String t) throws IOException
    {
        writeRow("", t);
        writeLine();
        writeRow("---", "---");
        writeLine();
    }

    private static void writeCode(final String a, final String b) throws IOException
    {
        write(a, "`" + b + "`");
    }

    private static void write(final String a, final String b) throws IOException
    {
        writeRow("**" + a + "**", b.replaceAll("\n", " "));
        writeLine();
    }

    private static void writeLine() throws IOException
    {
        writer.write("\n");
    }

    private static void writeRow(final String a, final String b) throws IOException
    {
        writer.write("| " + a + " | " + b + " |");
    }

    private static String toHeaderString(final String t)
    {
        final StringBuilder builder = new StringBuilder();

        char previous = '_';
        for (final char next: t.toCharArray())
        {
            if (next == '_')
            {
                builder.append(' ');
            }
            else if (previous == '_')
            {
                builder.append(Character.toUpperCase(next));
            }
            else
            {
                builder.append(Character.toLowerCase(next));
            }
            previous = next;
        }
        return builder.toString();
    }

    private static String getDefaultString(
        final String defaultValue,
        final boolean isTimeValue,
        final TimeUnit timeUnit)
    {
        if (defaultValue != null && !defaultValue.isEmpty() && defaultValue.chars().allMatch(Character::isDigit))
        {
            final long defaultLong = Long.parseLong(defaultValue);
            final StringBuilder builder = new StringBuilder();

            builder.append(defaultValue);

            if (defaultValue.length() > 3)
            {
                builder.append(" (");
                builder.append(DecimalFormat.getNumberInstance().format(defaultLong));
                builder.append(")");

                int kCount = 0;
                long remainingValue = defaultLong;
                while (remainingValue % 1024 == 0)
                {
                    kCount++;
                    remainingValue = remainingValue / 1024;
                }

                if (kCount > 0 && remainingValue < 1024)
                {
                    builder.append(" (");
                    builder.append(remainingValue);
                    IntStream.range(0, kCount).forEach(i -> builder.append(" * 1024"));
                    builder.append(")");
                }
            }

            if (isTimeValue)
            {
                int tCount = 0;

                long remaining = timeUnit.toNanos(defaultLong);
                while (remaining % 1000 == 0 && tCount < 3)
                {
                    tCount++;
                    remaining = remaining / 1000;
                }
                builder.append(" (");
                builder.append(remaining);

                switch (tCount)
                {
                    case 0:
                        builder.append(" nano");
                        break;
                    case 1:
                        builder.append(" micro");
                        break;
                    case 2:
                        builder.append(" milli");
                        break;
                    case 3:
                        builder.append(" ");
                        break;
                }
                builder.append("second");
                if (remaining != 1)
                {
                    builder.append("s");
                }
                builder.append(")");
            }

            return builder.toString();
        }
        return defaultValue;
    }
}
